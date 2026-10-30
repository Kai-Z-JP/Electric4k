package electric4k

import io.ktor.client.*
import io.ktor.client.plugins.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.utils.io.*
import kotlinx.io.IOException
import kotlinx.coroutines.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.*
import kotlinx.serialization.json.*
import kotlinx.datetime.Clock
import kotlin.random.Random

internal fun createCacheBuster(): String = "${Clock.System.now().toEpochMilliseconds()}-${Random.nextLong().toULong()}"
fun canonicalShapeKey(url: String): String {
    val builder = URLBuilder(url)
    val parameters = builder.parameters.entries().filter { it.key !in ELECTRIC_PROTOCOL_QUERY_PARAMS }.sortedBy { it.key }.flatMap { (key, values) -> values.map { key to it } }
    builder.parameters.clear(); builder.fragment = ""
    return builder.buildString() + if (parameters.isEmpty()) "" else "?" + parameters.formUrlEncode()
}

/** Suspendable transport. An injected HttpClient remains owned by the caller. */
class ShapeClient(
    val endpoint: String,
    httpClient: HttpClient? = null,
    val retryPolicy: RetryPolicy = RetryPolicy(),
    private val headers: suspend () -> Map<String, String> = { emptyMap() },
    private val refreshAuth: suspend () -> Boolean = { false },
    private val queryParams: suspend () -> Map<String, String> = { emptyMap() },
    val columnMapper: ColumnMapper? = null,
) {
    private val ownsClient = httpClient == null
    private val http = httpClient ?: HttpClient {
        expectSuccess = false
        install(HttpTimeout) { requestTimeoutMillis = 120_000; socketTimeoutMillis = 90_000 }
    }
    init {
        val url = Url(endpoint)
        require(url.protocol.name in setOf("http", "https") && url.host.isNotBlank())
        require(url.fragment.isEmpty() && url.user == null && url.password == null)
    }
    fun close() { if (ownsClient) http.close() }

    private suspend fun builder(request: ShapeRequest, protocol: Map<String, String>, extraHeaders: Map<String, String> = emptyMap(), extraParams: Map<String, String> = emptyMap()): HttpRequestBuilder = HttpRequestBuilder().apply {
        expectSuccess = false
        url(endpoint)
        val supplied = this@ShapeClient.queryParams() + extraParams
        supplied.keys.forEach { key -> require(key !in ELECTRIC_PROTOCOL_QUERY_PARAMS && !key.startsWith("subset__")) { "External parameters cannot override $key" } }
        url.parameters.apply {
            supplied.forEach { (key, value) -> set(key, value) }
            request.table?.let { if ("table" !in supplied) set("table", it) }
            request.where?.let { if ("where" !in supplied) set("where", encodeWhereClause(it, columnMapper)) }
            if (request.columns.isNotEmpty() && "columns" !in supplied) set("columns", request.columns.joinToString(",") { quoteIdentifier(columnMapper?.encode(it) ?: it) })
            request.params.forEach { (key, value) -> set("params[$key]", value) }
            if ("replica" !in supplied) set("replica", request.replica)
            set("log", request.log.wireValue)
            protocol.forEach { (key, value) -> set(key, value) }
        }
        (this@ShapeClient.headers() + extraHeaders).forEach { (key, value) -> header(key, value) }
        headers.remove(HttpHeaders.Accept)
        val sse = protocol["live_sse"] == "true"
        header(HttpHeaders.Accept, if (sse) "text/event-stream" else "application/json")
        if (sse) timeout { requestTimeoutMillis = Long.MAX_VALUE }
        sortParameters(this)
    }
    private fun sortParameters(request: HttpRequestBuilder) {
        val entries = request.url.parameters.entries().sortedBy { it.key }.map { it.key to it.value.toList() }
        request.url.parameters.clear()
        entries.forEach { (key, values) -> request.url.parameters.appendAll(key, values) }
    }
    private suspend fun checkStatus(response: HttpResponse) {
        if (response.status.value == 409) throw ShapeInvalidatedException(response.headers["electric-handle"], response.request.url.toString())
        if (response.status.value !in 200..299) throw ShapeHttpException(response.status.value, retryAfterMillis(response.headers[HttpHeaders.RetryAfter]), response.headers.entries().associate { it.key to it.value.joinToString(",") }, response.bodyAsText())
    }
    private fun validateHeaders(response: HttpResponse, live: Boolean) {
        val required = listOf("electric-offset", "electric-handle", if (live) "electric-cursor" else "electric-schema")
        val missing = required.filter { response.headers[it] == null }
        if (missing.isNotEmpty()) throw ShapeProtocolException("Missing response headers: ${missing.joinToString()}")
    }
    private fun cursor(response: HttpResponse): ShapeCursor = ShapeCursor(
        response.headers["electric-offset"] ?: throw ShapeProtocolException("Missing electric-offset"),
        response.headers["electric-handle"] ?: throw ShapeProtocolException("Missing electric-handle"), response.headers["electric-cursor"],
    )
    private fun schema(response: HttpResponse): ShapeSchema = response.headers["electric-schema"]?.let { wireJson.decodeFromString<Map<String, ColumnInfo>>(it) } ?: emptyMap()
    private suspend fun <T> retry(action: suspend () -> T): T {
        var attempt = 0
        var refreshed = false
        while (true) {
            currentCoroutineContext().ensureActive()
            try { return action() }
            catch (e: CancellationException) { throw e }
            catch (e: Exception) {
                retryPolicy.onFailedAttempt?.invoke()
                if (e is ShapeHttpException && e.status in setOf(401, 403) && !refreshed) {
                    refreshed = true
                    if (refreshAuth()) continue
                }
                val retryable = e is IOException || e is HttpRequestTimeoutException || (e is ShapeHttpException && (e.status == 429 || e.status in 500..599))
                if (!retryable || ++attempt >= retryPolicy.maxAttempts) throw e
                delay(retryPolicy.delayMillis(attempt - 1, (e as? ShapeHttpException)?.retryAfterMillis))
            }
        }
    }
    suspend fun fetch(table: String, cursor: ShapeCursor? = null, live: Boolean = false): ShapeBatch = fetch(ShapeRequest(table), cursor, live)
    suspend fun fetch(request: ShapeRequest, cursor: ShapeCursor? = null, live: Boolean = false, initialHandle: String? = null): ShapeBatch {
        require(!live || cursor != null)
        val parameters = buildMap {
            put("offset", cursor?.offset ?: if (request.log == LogMode.ChangesOnly) "now" else "-1")
            (cursor?.handle ?: initialHandle)?.let { put("handle", it) }
            if (live) { put("live", "true"); put("cursor", cursor?.cacheBuster ?: "") }
        }
        return requestBatch(request, parameters)
    }
    internal suspend fun requestBatch(request: ShapeRequest, protocol: Map<String, String>, extraHeaders: Map<String, String> = emptyMap(), extraParams: Map<String, String> = emptyMap()): ShapeBatch = retry {
        http.prepareRequest(builder(request, protocol, extraHeaders, extraParams)).execute { response ->
            checkStatus(response)
            validateHeaders(response, protocol["live"] == "true")
            val body = try { response.bodyAsText() }
            catch (e: CancellationException) { throw e }
            catch (e: Exception) { throw ShapeHttpException(response.status.value, responseBody = e.message ?: "Failed to read response body") }
            val messages = if (response.status.value in setOf(201, 204, 205)) JsonArray(emptyList()) else wireJson.parseToJsonElement(body).jsonArray
            messages.checkInvalidation(response.headers["electric-handle"], response.request.url.toString())
            ShapeBatch(messages, cursor(response), messages.isUpToDate() || response.status.value == 204, schema(response), response.status.value, response.request.url.toString())
        }
    }

    /** Fetches a subset without mutating a stream. ShapeStream.requestSnapshot also injects and deduplicates it. */
    suspend fun fetchSnapshot(request: ShapeRequest, subset: SubsetParams, cursor: ShapeCursor? = null, defaultMethod: String = "GET"): SubsetSnapshot = requestSnapshot(request, subset, buildMap {
        put("offset", cursor?.offset ?: if (request.log == LogMode.ChangesOnly) "now" else "-1")
        cursor?.let { put("handle", it.handle) }
    }, defaultMethod)

    internal suspend fun requestSnapshot(request: ShapeRequest, subset: SubsetParams, protocol: Map<String, String>, defaultMethod: String, extraHeaders: Map<String, String> = emptyMap(), extraParams: Map<String, String> = emptyMap(), cachedSchema: ShapeSchema? = null, onInvalidated: suspend (String?, ShapeInvalidatedException) -> Unit = { _, _ -> }): SubsetSnapshot {
        var parameters = protocol - setOf("live", "cursor", "live_sse", "experimental_live_sse")
        var rotations = 0
        while (true) {
            try {
                return retry {
                    val body = subset.body(columnMapper)
                    val method = subset.method ?: defaultMethod
                    require(method in setOf("GET", "POST"))
                    val requestBuilder = builder(request, parameters, extraHeaders, extraParams)
                    if (method == "POST") {
                        requestBuilder.method = HttpMethod.Post
                        requestBuilder.contentType(ContentType.Application.Json)
                        requestBuilder.setBody(body.toString())
                    } else body.forEach { (key, value) ->
                        requestBuilder.url.parameters["subset__$key"] = if (value is JsonPrimitive) value.content else value.toString()
                    }
                    sortParameters(requestBuilder)
                    val response = http.request(requestBuilder)
                    checkStatus(response)
                    if (cachedSchema == null && response.headers["electric-schema"] == null) throw ShapeProtocolException("Missing electric-schema for snapshot")
                    val result = wireJson.parseToJsonElement(response.bodyAsText()).jsonObject
                    SubsetSnapshot(wireJson.decodeFromJsonElement(SnapshotMetadata.serializer(), result.getValue("metadata")), result.getValue("data").jsonArray, response.headers["electric-offset"], response.headers["electric-handle"], cachedSchema ?: schema(response))
                }
            } catch (e: ShapeInvalidatedException) {
                if (++rotations > 5) throw ShapeHttpException(502, responseBody = "Snapshot stuck in 409 retry loop")
                onInvalidated(parameters["handle"], e)
                parameters = parameters + (e.replacementHandle?.let { mapOf("handle" to it) } ?: emptyMap()) + mapOf("cache-buster" to createCacheBuster())
            }
        }
    }

    /** One SSE connection; emits only complete up-to-date batches. EOF discards any incomplete tail. */
    fun events(request: ShapeRequest, cursor: ShapeCursor, schema: ShapeSchema = emptyMap()): Flow<ShapeBatch> = streamEvents(request, mapOf("offset" to cursor.offset, "handle" to cursor.handle, "cursor" to (cursor.cacheBuster ?: ""), "live" to "true"), schema)

    internal fun streamEvents(request: ShapeRequest, protocol: Map<String, String>, schema: ShapeSchema = emptyMap(), extraHeaders: Map<String, String> = emptyMap(), extraParams: Map<String, String> = emptyMap()): Flow<ShapeBatch> = flow {
        var refreshed = false
        var delivered = false
        while (true) {
            try {
                val requestBuilder = builder(request, protocol + mapOf("live_sse" to "true", "experimental_live_sse" to "true"), extraHeaders, extraParams)
                http.prepareRequest(requestBuilder).execute { response ->
                    checkStatus(response)
                    validateHeaders(response, true)
                    if (response.status.value == 204) {
                        emit(ShapeBatch(JsonArray(emptyList()), cursor(response), true, schema, 204, response.request.url.toString()))
                        return@execute
                    }
                    if (response.contentType()?.withoutParameters() != ContentType.Text.EventStream) throw ShapeProtocolException("Expected text/event-stream")
                    val responseCursor = cursor(response)
                    var resumeOffset = responseCursor.offset
                    val responseSchema = schema.ifEmpty { schema(response) }
                    val messages = mutableListOf<JsonElement>()
                    var data = StringBuilder()
                    var firstLine = true
                    val channel = response.bodyAsChannel()
                    while (true) {
                        currentCoroutineContext().ensureActive()
                        var line = channel.readUTF8Line(max = 4 * 1024 * 1024) ?: break
                        if (firstLine) { line = line.removePrefix("\uFEFF"); firstLine = false }
                        if (line.isEmpty()) {
                            if (data.isNotEmpty()) {
                                val message = wireJson.parseToJsonElement(data.toString().removeSuffix("\n"))
                                data = StringBuilder()
                                if (message.control() == "must-refetch") throw ShapeInvalidatedException(response.headers["electric-handle"], response.request.url.toString())
                                messages.add(message)
                                if (message.control() == "up-to-date") {
                                    val lsn = message.jsonObject["headers"]!!.jsonObject["global_last_seen_lsn"]?.jsonPrimitive?.content
                                    val offset = if (lsn == null) null else if ('/' in lsn) {
                                        val parts = lsn.split('/')
                                        require(parts.size == 2)
                                        ((parts[0].toULong(16) shl 32) or parts[1].toULong(16)).toString()
                                    } else { lsn.toULong(); lsn }
                                    if (offset != null) resumeOffset = "${offset}_0"
                                    delivered = true
                                    emit(ShapeBatch(JsonArray(messages.toList()), responseCursor.copy(offset = resumeOffset), true, responseSchema, 200, response.request.url.toString()))
                                    messages.clear()
                                }
                            }
                        } else if (line.startsWith("data:")) {
                            data.append(line.removePrefix("data:").removePrefix(" ")).append('\n')
                            if (data.length > 4 * 1024 * 1024) throw ShapeProtocolException("SSE event exceeds 4 MiB")
                        }
                    }
                }
                break
            } catch (e: ShapeHttpException) {
                if (!delivered && !refreshed && e.status in setOf(401, 403)) {
                    refreshed = true
                    if (refreshAuth()) continue
                }
                throw e
            }
        }
    }
}
