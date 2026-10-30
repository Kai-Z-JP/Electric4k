@file:OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
package electric4k

import io.ktor.client.*
import io.ktor.client.engine.mock.*
import io.ktor.http.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.test.*
import kotlinx.serialization.json.*
import kotlin.test.*

private const val UPSTREAM_UP = """{"headers":{"control":"up-to-date","global_last_seen_lsn":"20"}}"""
private const val SUBSET_BODY = """{"metadata":{"xmin":"10","xmax":"20","xip_list":["15"],"snapshot_mark":1,"database_lsn":"100"},"data":[{"key":"a","value":{"id":"1","title":"snapshot"},"headers":{"operation":"insert"}}]}"""
class ShapeStreamTest {
    @Test fun oneShotPublishesParsedMessagesThenCompletes() = runTest {
        var requests = 0
        val http = HttpClient(MockEngine { requests++; respond("""[{"key":"a","headers":{"operation":"insert"},"value":{"id":"1","done":"t"}},$UPSTREAM_UP]""", headers = responseHeaders()) })
        val stream = ShapeStream(ShapeClient("https://example.test/shape", http), ShapeRequest("todos"), backgroundScope, ShapeStreamOptions(subscribe = false, detectSystemWake = false, liveRequestTimeoutMillis = null), cache = ProtocolCache())
        try {
            val batches = stream.messages().toList()
            assertEquals(1, requests); assertEquals(1, batches.size)
            assertEquals(1L, batches[0][0].jsonObject["value"]!!.jsonObject["id"]!!.jsonPrimitive.long)
            assertTrue(batches[0][0].jsonObject["value"]!!.jsonObject["done"]!!.jsonPrimitive.boolean)
            assertTrue(stream.isUpToDate); assertNotNull(stream.lastSyncedAt())
        } finally { stream.close(); http.close() }
    }
    @Test fun changesOnlyStartsNowAndColdSnapshotAdvancesPosition() = runTest {
        val urls = mutableListOf<Url>()
        val http = HttpClient(MockEngine { request ->
            urls.add(request.url)
            if (request.method == HttpMethod.Post) respond(SUBSET_BODY, headers = responseHeaders("15_0"))
            else respond("[$UPSTREAM_UP]", headers = responseHeaders("20_0"))
        })
        val stream = ShapeStream(ShapeClient("https://example.test/shape", http), ShapeRequest("todos", log = LogMode.ChangesOnly), backgroundScope, ShapeStreamOptions(subscribe = false, detectSystemWake = false, liveRequestTimeoutMillis = null), cache = ProtocolCache())
        val batches = mutableListOf<JsonArray>()
        try {
            stream.subscribe({ batches.add(it) })
            val snapshot = stream.requestSnapshot(SubsetParams(where = "id = $1", params = mapOf("1" to JsonPrimitive(1)), method = "POST"))
            assertEquals(1, snapshot.data.size)
            runCurrent()
            stream.status.first { it.isUpToDate }
            assertEquals("now", urls.first().parameters["offset"])
            assertEquals("changes_only", urls.first().parameters["log"])
            assertEquals("15_0", urls.last().parameters["offset"])
            assertEquals(listOf("snapshot-end", "subset-end"), batches.first().takeLast(2).map { it.control() })
        } finally { stream.close(); http.close() }
    }
    @Test fun subsetGetAndPostUseSameCompiledParameters() = runTest {
        var getBody: JsonObject? = null
        var postBody: JsonObject? = null
        val http = HttpClient(MockEngine { request ->
            if (request.method == HttpMethod.Get) getBody = buildJsonObject {
                put("where", request.url.parameters["subset__where"])
                put("params", wireJson.parseToJsonElement(request.url.parameters["subset__params"]!!))
                put("order_by", request.url.parameters["subset__order_by"])
            } else postBody = wireJson.parseToJsonElement(request.body.toByteArray().decodeToString()).jsonObject
            respond(SUBSET_BODY, headers = responseHeaders())
        })
        try {
            val client = ShapeClient("https://example.test/shape", http, columnMapper = snakeCamelMapper())
            val subset = SubsetParams(whereExpr = SerializedExpression.Function("eq", listOf(SerializedExpression.Ref("userId"), SerializedExpression.Value(1))), params = mapOf("1" to JsonPrimitive("a&b")), orderByExpr = listOf(SerializedOrderByClause("createdAt", "desc", "last")))
            client.fetchSnapshot(ShapeRequest("todos"), subset)
            client.fetchSnapshot(ShapeRequest("todos"), subset.copy(method = "POST"))
            assertEquals("\"user_id\" = $1", getBody!!["where"]!!.jsonPrimitive.content)
            assertEquals(getBody, JsonObject(postBody!!.filterKeys { it in setOf("where", "params", "order_by") }))
        } finally { http.close() }
    }
    @Test fun everySnapshot409HasNewCacheBusterAndRetriesAreBounded() = runTest {
        val urls = mutableListOf<Url>()
        val http = HttpClient(MockEngine { request -> urls.add(request.url); respond("", HttpStatusCode.Conflict, headersOf("electric-handle", "same")) })
        try {
            assertEquals(502, assertFailsWith<ShapeHttpException> { ShapeClient("https://example.test/shape", http).fetchSnapshot(ShapeRequest("todos"), SubsetParams(limit = 1)) }.status)
            assertEquals(6, urls.size)
            val busters = urls.drop(1).map { it.parameters["cache-buster"] }
            assertTrue(busters.all { it != null }); assertEquals(5, busters.toSet().size)
        } finally { http.close() }
    }
    @Test fun pauseReasonsDoNotReleaseEachOtherAndResumeCatchesUp() = runTest {
        val entered = CompletableDeferred<Unit>()
        val urls = mutableListOf<Url>()
        val http = HttpClient(MockEngine { request ->
            urls.add(request.url)
            if (urls.size == 1) respond("[$UPSTREAM_UP]", headers = responseHeaders())
            else { entered.complete(Unit); awaitCancellation() }
        })
        val stream = ShapeStream(ShapeClient("https://example.test/shape", http), ShapeRequest("todos"), backgroundScope, ShapeStreamOptions(detectSystemWake = false, liveRequestTimeoutMillis = null), cache = ProtocolCache())
        try {
            stream.subscribe({})
            entered.await()
            stream.pause("visibility"); stream.pause("snapshot")
            stream.resume("snapshot"); assertTrue(stream.isPaused())
            val before = urls.size
            runCurrent(); assertEquals(before, urls.size)
            stream.resume("visibility")
            while (urls.size == before) yield()
            assertNull(urls.last().parameters["live"])
        } finally { stream.close(); http.close() }
    }
    @Test fun forceRefreshAbortsLiveAndWaitsForCatchUp() = runTest {
        val entered = CompletableDeferred<Unit>()
        var count = 0
        val http = HttpClient(MockEngine { request ->
            when (++count) {
                1 -> respond("[$UPSTREAM_UP]", headers = responseHeaders())
                2 -> { assertEquals("true", request.url.parameters["live"]); entered.complete(Unit); awaitCancellation() }
                3 -> { assertNull(request.url.parameters["live"]); respond("[$UPSTREAM_UP]", headers = responseHeaders("20_0")) }
                else -> awaitCancellation()
            }
        })
        val stream = ShapeStream(ShapeClient("https://example.test/shape", http), ShapeRequest("todos"), backgroundScope, ShapeStreamOptions(detectSystemWake = false, liveRequestTimeoutMillis = null), cache = ProtocolCache())
        try { stream.subscribe({}); entered.await(); stream.forceDisconnectAndRefresh(); assertEquals("20_0", stream.lastOffset) }
        finally { stream.close(); http.close() }
    }
    @Test fun snapshotTrackerRespectsVisibilityAndXidWraparound() {
        val tracker = SnapshotTracker()
        val metadata = SnapshotMetadata("4294967290", "4294967300", listOf("4294967295"), 1, "100")
        tracker.add(metadata, setOf("a"))
        fun change(xid: Long) = buildJsonObject { put("key", "a"); putJsonObject("headers") { putJsonArray("txids") { add(xid) } } }
        assertTrue(tracker.reject(change(4294967294)))
        assertFalse(tracker.reject(change(4294967295)))
        assertTrue(tracker.reject(change(1)))
        tracker.lastSeen(100u)
        assertFalse(tracker.reject(change(1)))
    }
    @Test fun notNullAndMetadataArePassedToCustomParsers() {
        val types = PostgresTypes(parsers = mapOf("numeric" to { value, info -> JsonPrimitive("$value:${info.scale}") }))
        assertEquals(JsonPrimitive("1.2:2"), types.convert(buildJsonObject { put("a", "1.2") }, mapOf("a" to ColumnInfo("numeric", scale = 2)))["a"])
        assertFailsWith<ShapeProtocolException> { types.convert(buildJsonObject { put("a", JsonNull) }, mapOf("a" to ColumnInfo("text", notNull = true))) }
    }
    @Test fun queryUrlAndOptionalTableAreSupported() = runTest {
        val http = HttpClient(MockEngine { request ->
            assertEquals("proxy", request.url.parameters["source_id"])
            assertNull(request.url.parameters["table"])
            assertEquals("\"user_id\",\"has,comma\"", request.url.parameters["columns"])
            respond("[$UPSTREAM_UP]", headers = responseHeaders())
        })
        try { ShapeClient("https://example.test/shape?source_id=proxy", http, columnMapper = snakeCamelMapper()).fetch(ShapeRequest(columns = listOf("userId", "has,comma"))) }
        finally { http.close() }
    }
    @Test fun shortSseConnectionsFallBackToLongPolling() = runTest {
        val transports = mutableListOf<Boolean>()
        val http = HttpClient(MockEngine { request ->
            val sse = request.url.parameters["live_sse"] == "true"
            transports.add(sse)
            if (sse) respond("data: $UPSTREAM_UP\n\n", headers = responseHeaders(type = "text/event-stream"))
            else respond("[$UPSTREAM_UP]", headers = responseHeaders())
        })
        val stream = ShapeStream(ShapeClient("https://example.test/shape", http), ShapeRequest("todos"), backgroundScope,
            ShapeStreamOptions(liveSse = true, detectSystemWake = false, liveRequestTimeoutMillis = null, clock = { 0 }), ProtocolCache())
        try {
            stream.messages().take(5).collect()
            assertEquals(listOf(false, true, true, true, false), transports.take(5))
        } finally { stream.close(); http.close() }
    }
    @Test fun onErrorCanReplaceHeadersWithoutNotifyingSubscribersOfRecoveredError() = runTest {
        var failures = 0
        var callbacks = 0
        val http = HttpClient(MockEngine { request ->
            if (request.headers["Authorization"] != "updated") respond("", HttpStatusCode.Unauthorized)
            else respond("[$UPSTREAM_UP]", headers = responseHeaders())
        })
        val stream = ShapeStream(ShapeClient("https://example.test/shape", http, RetryPolicy(3, 0, 0)), ShapeRequest("todos"), backgroundScope,
            ShapeStreamOptions(subscribe = false, detectSystemWake = false, liveRequestTimeoutMillis = null, onError = { failures++; StreamRetryOptions(headers = mapOf("Authorization" to "updated")) }), ProtocolCache())
        try {
            stream.subscribe({}, { callbacks++ })
            stream.status.first { it.isUpToDate }
            assertEquals(1, failures); assertEquals(0, callbacks)
        } finally { stream.close(); http.close() }
    }
    @Test fun snapshotInjectionInsideSubscriberDoesNotDeadlock() = runTest {
        val done = CompletableDeferred<Unit>()
        var requested = false
        val http = HttpClient(MockEngine { request ->
            if (request.url.parameters["subset__limit"] != null) respond(SUBSET_BODY, headers = responseHeaders())
            else if (request.url.parameters["live"] == "true") awaitCancellation()
            else respond("[$UPSTREAM_UP]", headers = responseHeaders())
        })
        val stream = ShapeStream(ShapeClient("https://example.test/shape", http), ShapeRequest("todos", log = LogMode.ChangesOnly), backgroundScope,
            ShapeStreamOptions(detectSystemWake = false, liveRequestTimeoutMillis = null), ProtocolCache())
        try {
            stream.subscribe({ batch ->
                if (!requested && batch.any { it.control() == "up-to-date" }) {
                    requested = true
                    val result = stream.requestSnapshot(SubsetParams(limit = 1))
                    assertEquals(1, result.data.size)
                    done.complete(Unit)
                }
            }, { done.completeExceptionally(it) })
            done.await()
        } finally { stream.close(); http.close() }
    }
    @Test fun cachedExpiredResponseIsNeverPublished() = runTest {
        val cache = ProtocolCache()
        cache.markExpired(canonicalShapeKey("https://example.test/shape?table=todos&replica=default"), "expired")
        val http = HttpClient(MockEngine { request ->
            val fresh = request.url.parameters["cache-buster"] != null
            val value = if (fresh) "fresh" else "stale"
            respond("[{\"key\":\"a\",\"value\":{\"title\":\"$value\"},\"headers\":{\"operation\":\"insert\"}},$UPSTREAM_UP]", headers = responseHeaders(handle = if (fresh) "new" else "expired"))
        })
        val stream = ShapeStream(ShapeClient("https://example.test/shape", http), ShapeRequest("todos"), backgroundScope,
            ShapeStreamOptions(subscribe = false, detectSystemWake = false, liveRequestTimeoutMillis = null), cache)
        try {
            val batches = stream.messages().toList()
            assertEquals(1, batches.size)
            assertEquals("fresh", batches.single()[0].jsonObject["value"]!!.jsonObject["title"]!!.jsonPrimitive.content)
        } finally { stream.close(); http.close() }
    }
    @Test fun replaySuppressesOnlyControlAndKeepsChanges() = runTest {
        val cache = ProtocolCache()
        cache.synced(canonicalShapeKey("https://example.test/shape?table=todos&replica=default"), "cache-1")
        val http = HttpClient(MockEngine { respond("[{\"key\":\"a\",\"value\":{\"id\":\"1\"},\"headers\":{\"operation\":\"insert\"}},$UPSTREAM_UP]", headers = responseHeaders()) })
        val stream = ShapeStream(ShapeClient("https://example.test/shape", http), ShapeRequest("todos"), backgroundScope,
            ShapeStreamOptions(subscribe = false, detectSystemWake = false, liveRequestTimeoutMillis = null), cache)
        try {
            val messages = stream.messages().toList().single()
            assertEquals(1, messages.size); assertTrue(isChangeMessage(messages.single())); assertTrue(stream.isUpToDate)
        } finally { stream.close(); http.close() }
    }
    @Test fun fullShapeAwaitRowsWaitsForMaterialization() = runTest {
        val http = HttpClient(MockEngine { respond("[{\"key\":\"a\",\"value\":{\"id\":\"1\"},\"headers\":{\"operation\":\"insert\"}},$UPSTREAM_UP]", headers = responseHeaders()) })
        val stream = ShapeStream(ShapeClient("https://example.test/shape", http), ShapeRequest("todos"), backgroundScope,
            ShapeStreamOptions(subscribe = false, detectSystemWake = false, liveRequestTimeoutMillis = null), ProtocolCache())
        val shape = Shape(stream)
        try { assertEquals(1L, shape.awaitRows().single().getValue("id").jsonPrimitive.long) }
        finally { shape.close(); stream.close(); http.close() }
    }

    @Test fun failingSubscriberDoesNotStopOtherSubscribersOrSynchronization() = runTest {
        val reported = CompletableDeferred<Throwable>()
        val owner = CoroutineScope(SupervisorJob() + StandardTestDispatcher(testScheduler) + CoroutineExceptionHandler { _, error -> reported.complete(error) })
        val http = HttpClient(MockEngine { respond("[$UPSTREAM_UP]", headers = responseHeaders()) })
        val stream = ShapeStream(ShapeClient("https://example.test/shape", http), ShapeRequest("todos"), owner,
            ShapeStreamOptions(subscribe = false, detectSystemWake = false, liveRequestTimeoutMillis = null), ProtocolCache())
        val delivered = CompletableDeferred<Unit>()
        try {
            stream.subscribe({ error("subscriber failure") })
            stream.subscribe({ delivered.complete(Unit) })
            delivered.await()
            assertEquals("subscriber failure", reported.await().message)
            assertNull(stream.error)
            assertTrue(stream.isUpToDate)
        } finally { stream.close(); owner.cancel(); http.close() }
    }
    @Test fun subsetRotationUpdatesHandleEvenWhenSubsequentFetchFails() = runTest {
        val cache = ProtocolCache()
        var attempts = 0
        val http = HttpClient(MockEngine {
            if (++attempts == 1) respond("", HttpStatusCode.Conflict, headersOf("electric-handle", "replacement"))
            else respond("denied", HttpStatusCode.Forbidden)
        })
        val stream = ShapeStream(ShapeClient("https://example.test/shape", http), ShapeRequest("todos", log = LogMode.ChangesOnly), backgroundScope,
            ShapeStreamOptions(handle = "original", offset = "10_0", detectSystemWake = false, liveRequestTimeoutMillis = null), cache)
        try {
            assertFailsWith<ShapeHttpException> { stream.fetchSnapshot(SubsetParams(limit = 1)) }
            assertEquals("replacement", stream.shapeHandle)
            assertEquals("10_0", stream.lastOffset)
            assertEquals("original", cache.expired(canonicalShapeKey("https://example.test/shape?table=todos&replica=default")))
        } finally { stream.close(); http.close() }
    }

}
