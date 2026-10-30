package electric4k

import io.ktor.client.plugins.HttpRequestTimeoutException
import kotlinx.io.IOException
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.serialization.json.*

enum class SyncMode { LongPolling, Sse }
enum class SyncStatus { Syncing, UpToDate, Reset }
data class ShapeSnapshot<T>(val rows: Map<String, T>, val cursor: ShapeCursor?, val status: SyncStatus)

/** A cold, cancellable stream. Persisted keys must be scoped to the authenticated user/tenant. */
class ShapeSync<T>(
    private val client: ShapeClient,
    private val request: ShapeRequest,
    private val decoder: RowDecoder<T>,
    private val store: ShapeStore = MemoryShapeStore(),
    private val storageKey: String,
    private val mode: SyncMode = SyncMode.LongPolling,
    private val reconnectDelayMillis: Long = 250,
) {
    private val active = Mutex()
    init { require(storageKey.isNotBlank()); require(reconnectDelayMillis >= 0) }
    private val identity = buildJsonObject {
        put("endpoint", client.endpoint); put("table", request.table); put("where", request.where)
        put("columns", JsonArray(request.columns.map(::JsonPrimitive)))
        put("params", JsonObject(request.params.entries.sortedBy { it.key }.associate { it.toPair() }.mapValues { JsonPrimitive(it.value) }))
        put("replica", request.replica)
    }.toString()

    /** Cancel collection to stop requests. Only one collector/writer may use this instance at a time. */
    fun snapshots(): Flow<ShapeSnapshot<T>> = flow {
        check(active.tryLock()) { "ShapeSync already has an active collector" }
        try {
            var state = store.load(storageKey) ?: StoredShape(identity)
            require(state.version == 1) { "Unsupported stored shape version" }
            require(state.identity == identity) { "Storage key belongs to another shape" }
            fun snapshot(s: StoredShape, status: SyncStatus): ShapeSnapshot<T> = ShapeSnapshot(
                s.rows.mapValues { decoder.decode(it.value, s.schema) }, s.cursor, status,
            )
            if (state.upToDate) emit(snapshot(state, SyncStatus.UpToDate))
            else emit(ShapeSnapshot(emptyMap(), null, SyncStatus.Syncing))
            var resets = 0
            var failures = 0
            var applying = false
            val expiredHandles = mutableSetOf<String>()
            suspend fun apply(batch: ShapeBatch) {
                applying = true
                val rows = state.rows.toMutableMap()
                for (message in batch.messages) {
                    val objectMessage = message.jsonObject
                    val headers = objectMessage["headers"]?.jsonObject ?: throw ShapeProtocolException("Message is missing headers")
                    val operation = headers["operation"]?.jsonPrimitive?.content ?: continue
                    val key = objectMessage["key"]?.jsonPrimitive?.content ?: throw ShapeProtocolException("Change is missing key")
                    when (operation) {
                        "insert" -> rows[key] = objectMessage["value"]?.jsonObject ?: throw ShapeProtocolException("Insert is missing value")
                        "update" -> rows[key] = JsonObject((rows[key] ?: emptyMap()) + (objectMessage["value"]?.jsonObject ?: throw ShapeProtocolException("Update is missing value")))
                        "delete" -> rows.remove(key)
                        else -> throw ShapeProtocolException("Unknown change operation")
                    }
                }
                val next = state.copy(rows = rows.toMap(), cursor = batch.cursor, schema = batch.schema.ifEmpty { state.schema }, upToDate = batch.upToDate, initialHandle = null)
                // Decode before saving; failed conversion must not advance the durable cursor.
                val result = if (next.upToDate) snapshot(next, SyncStatus.UpToDate) else null
                store.save(storageKey, next)
                state = next
                resets = 0; failures = 0
                if (result != null) emit(result)
                applying = false
            }
            while (true) {
                currentCoroutineContext().ensureActive()
                val streaming = mode == SyncMode.Sse && state.upToDate && state.cursor != null
                try {
                    if (streaming) {
                        client.events(request, state.cursor!!, state.schema).collect { apply(it) }
                        delay(reconnectDelayMillis)
                    } else {
                        val previous = state.cursor
                        val batch = client.fetch(request, previous, state.upToDate, state.initialHandle)
                        if (batch.cursor.handle in expiredHandles) throw ShapeInvalidatedException()
                        apply(batch)
                        if (state.cursor == previous || batch.upToDate) delay(reconnectDelayMillis)
                    }
                } catch (e: CancellationException) { throw e }
                catch (e: ShapeInvalidatedException) {
                    if (applying) throw e
                    if (++resets > 5) throw ShapeProtocolException("Repeated shape invalidation; check server/cache configuration")
                    state.cursor?.handle?.let { expiredHandles.add(it) }
                    val handle = e.replacementHandle?.takeUnless { it == state.cursor?.handle } ?: state.cursor?.handle?.let { "$it-next" } ?: state.initialHandle
                    state = StoredShape(identity, initialHandle = handle)
                    store.save(storageKey, state)
                    emit(ShapeSnapshot(emptyMap(), null, SyncStatus.Reset))
                    delay(reconnectDelayMillis)
                } catch (e: Exception) {
                    if (applying) throw e
                    // Polling already retries its requests. SSE resumes only from the last saved complete batch.
                    val retryable = streaming && (e is IOException || e is HttpRequestTimeoutException ||
                        e is ShapeHttpException && (e.status == 429 || e.status in 500..599))
                    if (!retryable || ++failures >= client.retryPolicy.maxAttempts) throw e
                    delay(client.retryPolicy.delayMillis(failures - 1, (e as? ShapeHttpException)?.retryAfterMillis))
                }
            }
        } finally { active.unlock() }
    }
}

fun ShapeClient.sync(request: ShapeRequest, storageKey: String, store: ShapeStore = MemoryShapeStore(), mode: SyncMode = SyncMode.LongPolling): ShapeSync<JsonObject> =
    ShapeSync(this, request, RowDecoder { row, _ -> row }, store, storageKey, mode)
