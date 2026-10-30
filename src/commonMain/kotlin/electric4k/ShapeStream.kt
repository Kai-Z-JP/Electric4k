package electric4k

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.datetime.Clock
import kotlinx.serialization.json.*
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.math.pow
import kotlin.random.Random
import kotlinx.io.IOException
import io.ktor.client.plugins.HttpRequestTimeoutException

/** Kotlin equivalents of ShapeStreamOptions; coroutine cancellation replaces AbortSignal. */
data class ShapeStreamOptions(
    val subscribe: Boolean = true,
    val liveSse: Boolean = false,
    val offset: String? = null,
    val handle: String? = null,
    val subsetMethod: String = "GET",
    val liveRequestTimeoutMillis: Long? = 45_000,
    val runtimeVisibility: RuntimeVisibilityAdapter? = null,
    val onError: (suspend (Throwable) -> StreamRetryOptions?)? = null,
    val transformer: ((JsonObject) -> JsonObject)? = null,
    val parser: PostgresTypes = PostgresTypes(),
    val clock: () -> Long = { Clock.System.now().toEpochMilliseconds() },
    val detectSystemWake: Boolean = true,
) {
    init {
        require(liveRequestTimeoutMillis == null || liveRequestTimeoutMillis > 0)
        require(subsetMethod in setOf("GET", "POST"))
        require(offset == null || offset in setOf("-1", "now") || handle != null) { "A resume offset requires a handle" }
    }
}
data class StreamRetryOptions(val headers: Map<String, String> = emptyMap(), val params: Map<String, String> = emptyMap())
data class StreamStatus(val started: Boolean = false, val connected: Boolean = false, val isUpToDate: Boolean = false, val offset: String = "-1", val handle: String? = null, val lastSyncedAt: Long? = null, val error: Throwable? = null, val paused: Boolean = false)

private class DeliveryContext(val stream: ShapeStream) : AbstractCoroutineContextElement(Key) { companion object Key : CoroutineContext.Key<DeliveryContext> }

/** Shared message stream. Subscriptions receive every message; Shape materializes consistent rows. */
class ShapeStream(
    val client: ShapeClient,
    val request: ShapeRequest = ShapeRequest(),
    scope: CoroutineScope = CoroutineScope(Dispatchers.Default),
    val options: ShapeStreamOptions = ShapeStreamOptions(),
    private val cache: ProtocolCache = SharedProtocolCache.value,
) {
    private val worker = CoroutineScope(scope.coroutineContext + SupervisorJob(scope.coroutineContext[Job]))
    private val mutex = Mutex()
    private val deliveryMutex = Mutex()
    private val subsetMutex = Mutex()
    private val subscribers = MutableStateFlow<List<Subscriber>>(emptyList())
    private data class Subscriber(val id: Any, val callback: suspend (JsonArray) -> Unit, val error: (Throwable) -> Unit)
    private val runner = MutableStateFlow<Job?>(null)
    private val network = MutableStateFlow<Job?>(null)
    private val closed = MutableStateFlow(false)
    private val paused = MutableStateFlow(false)
    private val tick = MutableStateFlow(0L)
    private val _status = MutableStateFlow(StreamStatus(offset = options.offset ?: if (request.log == LogMode.ChangesOnly) "now" else "-1"))
    val status: StateFlow<StreamStatus> = _status.asStateFlow()
    internal val deliveryPending = MutableStateFlow(false)
    private var state = StreamState.initial(StreamFields(handle = options.handle, offset = _status.value.offset))
    private val holders = mutableSetOf<String>()
    private val tracker = SnapshotTracker()
    private val parser = MessageParser(options.parser, client.columnMapper, options.transformer)
    private var generation = 0L
    private var snapshotCounter = 0L
    private var refreshing = false
    private var resuming = false
    private var pendingBuster: String? = null
    private var shapeKey: String? = null
    private var recoveringKey: String? = null
    private var staleGuard: String? = null
    private var extraHeaders = emptyMap<String, String>()
    private var extraParams = emptyMap<String, String>()
    private var unsubscribeVisibility: (() -> Unit)? = null
    private var wakeJob: Job? = null
    val mode: LogMode get() = request.log
    val isUpToDate: Boolean get() = status.value.isUpToDate
    val lastOffset: String get() = status.value.offset
    val shapeHandle: String? get() = status.value.handle
    val error: Throwable? get() = status.value.error
    fun isLoading() = !isUpToDate
    fun isConnected() = status.value.connected
    fun hasStarted() = status.value.started
    fun isPaused() = paused.value
    fun lastSyncedAt(): Long? = status.value.lastSyncedAt
    fun lastSynced(): Double = lastSyncedAt()?.let { (options.clock() - it).toDouble() } ?: Double.POSITIVE_INFINITY

    fun subscribe(callback: suspend (JsonArray) -> Unit, onError: (Throwable) -> Unit = {}): () -> Unit {
        check(!closed.value) { "Stream is closed" }
        val id = Any()
        subscribers.update { it + Subscriber(id, callback, onError) }
        start()
        return { subscribers.update { list -> list.filterNot { it.id === id } } }
    }
    fun unsubscribeAll() { subscribers.value = emptyList(); unsubscribeVisibility?.invoke(); unsubscribeVisibility = null; wakeJob?.cancel() }
    fun close() { closed.value = true; network.value?.cancel(); runner.value?.cancel(); unsubscribeAll(); worker.cancel() }
    fun messages(): Flow<JsonArray> = callbackFlow {
        val unsubscribe = subscribe({ send(it) }, { close(it) })
        val completion = runner.value?.invokeOnCompletion { cause -> close(cause) }
        awaitClose { unsubscribe(); completion?.dispose() }
    }
    private fun updateStatus(connected: Boolean = status.value.connected, failure: Throwable? = state.error) {
        _status.value = StreamStatus(true, connected, state.isUpToDate, state.fields.offset, state.fields.handle, state.fields.lastSyncedAt, failure, paused.value)
    }
    private fun start() {
        if (runner.value != null || closed.value) return
        val job = worker.launch(start = CoroutineStart.LAZY) {
            val visibility = options.runtimeVisibility ?: platformRuntimeVisibility()
            if (visibility?.isVisible() == false) pause("visibility")
            unsubscribeVisibility = visibility?.subscribe { visible -> worker.launch { if (visible) resume("visibility") else pause("visibility") } }
            if (options.detectSystemWake) wakeJob = worker.launch {
                var previous = options.clock()
                while (isActive) {
                    delay(5000)
                    val now = options.clock()
                    if (now - previous > 10_000 && !isPaused()) refreshWithoutWaiting()
                    previous = now
                }
            }
            try { loop() } finally { network.value?.cancel(); wakeJob?.cancel(); unsubscribeVisibility?.invoke(); unsubscribeVisibility = null }
        }
        if (runner.compareAndSet(null, job)) { _status.update { it.copy(started = true) }; job.start() } else job.cancel()
    }
    suspend fun pause(reason: String = "manual") {
        mutex.withLock {
            if (holders.add(reason) && holders.size == 1) {
                generation++; state = state.pause(); paused.value = true
                network.value?.cancel(); updateStatus(connected = false)
            }
        }
    }
    suspend fun resume(reason: String = "manual") {
        mutex.withLock {
            if (holders.remove(reason) && holders.isEmpty()) {
                state = state.resume(); resuming = true; paused.value = false; updateStatus()
            }
        }
    }
    private suspend fun refreshWithoutWaiting() {
        mutex.withLock { generation++; refreshing = true; network.value?.cancel() }
    }
    suspend fun forceDisconnectAndRefresh() {
        check(!isPaused()) { "Cannot refresh while paused" }
        start()
        val before = tick.value
        refreshWithoutWaiting()
        combine(tick, status, closed) { count, status, closed -> count > before || status.error != null || closed }.first { it }
        error?.let { throw it }
    }
    private suspend fun publish(messages: JsonArray) {
        if (messages.isEmpty()) return
        suspend fun deliver() = supervisorScope {
            subscribers.value.map { subscriber ->
                async {
                    try { subscriber.callback(messages) }
                    catch (e: Throwable) {
                        currentCoroutineContext().ensureActive()
                        // Like upstream's queued uncaught callback error: report to the
                        // owner scope, while allowing other subscribers and sync to continue.
                        worker.launch { throw e }
                    }
                }
            }.awaitAll()
            Unit
        }
        if (currentCoroutineContext()[DeliveryContext]?.stream === this) deliver()
        else deliveryMutex.withLock { withContext(DeliveryContext(this)) { deliver() } }
    }
    private suspend fun accept(batch: ShapeBatch, expectedGeneration: Long, sse: Boolean): Boolean {
        val output = mutex.withLock {
            if (expectedGeneration != generation || paused.value) return false
            val key = batch.requestUrl?.let(::canonicalShapeKey) ?: shapeKey
            shapeKey = key
            val expired = key?.let { cache.expired(it) }
            val transition = state.response(ResponseMetadata(batch.httpStatus, batch.cursor.handle, batch.cursor.offset, batch.cursor.cacheBuster, batch.schema, expired, options.clock(), cacheBuster = ::createCacheBuster))
            state = transition.state
            if (transition.action == "stale-retry") {
                if (transition.exceededMaxRetries) {
                    if (key == recoveringKey) throw ShapeHttpException(502, responseBody = "Repeated expired shape from cache")
                    recoveringKey = key; staleGuard = expired
                    key?.let { cache.forgetExpired(it) }
                    state = state.reset(); pendingBuster = createCacheBuster()
                }
                updateStatus(connected = false)
                return false
            }
            if (batch.cursor.handle == staleGuard) throw ShapeHttpException(502, responseBody = "Expired handle returned after cache recovery")
            if (state.canReplay()) key?.let { cache.replayCursor(it) }?.let { state = state.enterReplay(it) }
            val messageTransition = state.messages(batch.messages.isNotEmpty(), batch.messages.isUpToDate(), sse, if (sse) batch.cursor.offset else null, options.clock(), state.fields.liveCacheBuster)
            state = messageTransition.state
            if (batch.upToDate) {
                key?.let { cache.synced(it, state.fields.liveCacheBuster) }
                recoveringKey = null; staleGuard = null; refreshing = false
            }
            val filtered = JsonArray(batch.messages.filter { message ->
                val h = message.jsonObject["headers"]?.jsonObject
                if (isChangeMessage(message)) {
                    h?.get("lsn")?.jsonPrimitive?.content?.toULongOrNull()?.let(tracker::lastSeen)
                    !tracker.reject(message.jsonObject)
                } else {
                    h?.get("global_last_seen_lsn")?.jsonPrimitive?.content?.toULongOrNull()?.let(tracker::lastSeen)
                    !(messageTransition.suppressUpToDate && message.control() == "up-to-date")
                }
            })
            val parsed = parser.parse(filtered, state.fields.schema.orEmpty())
            deliveryPending.value = true
            updateStatus(connected = true)
            parsed
        }
        try { publish(output) } finally { deliveryPending.value = false }
        return true
    }
    private suspend fun loop() {
        var errorRetries = 0
        var sseFailures = 0
        val recentRequests = mutableListOf<Pair<Long, String?>>()
        var fastLoops = 0
        while (worker.isActive && !closed.value) {
            paused.first { !it || closed.value }
            if (closed.value) return
            val plan = mutex.withLock {
                if (!options.subscribe && state.isUpToDate) return
                val protocol = state.parameters(canLongPoll = !refreshing && !resuming).toMutableMap()
                shapeKey?.let { cache.expired(it) }?.let { protocol["expired_handle"] = it }
                pendingBuster?.let { protocol["cache-buster"] = it }; pendingBuster = null
                val sse = state.shouldUseSse(options.liveSse, refreshing, resuming)
                resuming = false
                Triple(protocol.toMap(), generation, sse)
            }
            try {
                if (!state.isUpToDate) {
                    val now = options.clock()
                    val offset = plan.first["offset"]
                    recentRequests.removeAll { now - it.first >= 500 }
                    recentRequests.add(now to offset)
                    if (recentRequests.count { it.second == offset } >= 5) {
                        if (++fastLoops >= 5) throw ShapeHttpException(502, responseBody = "Non-live stream made no offset progress")
                        if (fastLoops == 1) {
                            mutex.withLock { shapeKey?.let { cache.delete(it) }; state = state.reset(); pendingBuster = createCacheBuster(); updateStatus(connected = false) }
                            recentRequests.clear()
                            continue
                        }
                        delay(Random.nextLong(0, minOf(5000L, 100L shl fastLoops)))
                        recentRequests.clear()
                    }
                } else { recentRequests.clear(); fastLoops = 0 }
                if (plan.third) {
                    val began = options.clock()
                    val channel = Channel<ShapeBatch>(Channel.RENDEZVOUS)
                    val task = worker.launch(start = CoroutineStart.LAZY) {
                        try { client.streamEvents(request, plan.first, state.fields.schema.orEmpty(), extraHeaders, extraParams).collect { channel.send(it) }; channel.close() }
                        catch (e: Throwable) { channel.close(e) }
                    }
                    task.invokeOnCompletion { channel.close(it) }
                    mutex.withLock { network.value = task; if (generation != plan.second || paused.value) task.cancel() else task.start() }
                    try { for (batch in channel) { if (!accept(batch, plan.second, true)) break; errorRetries = 0; sseFailures = 0 } }
                    finally { task.cancel(); channel.cancel() }
                    val short = mutex.withLock {
                        val result = state.sseClosed(options.clock() - began, plan.second != generation)
                        state = result.state; result.wasShort
                    }
                    if (short) delay(Random.nextLong(1, minOf(5000L, 100L shl state.consecutiveShortSseConnections.coerceAtMost(5))))
                } else if (plan.first["live"] != "true") {
                    // Bounded snapshot prefetch: one queued chunk plus one in flight. State is
                    // advanced only as the consumer accepts each chunk, never by the producer.
                    val chunks = Channel<ShapeBatch>(1)
                    val task = worker.launch(start = CoroutineStart.LAZY) {
                        try {
                            var protocol = plan.first
                            while (isActive) {
                                val timeout = options.liveRequestTimeoutMillis?.takeIf { refreshing }
                                val batch = if (timeout == null) client.requestBatch(request, protocol, extraHeaders, extraParams)
                                    else withTimeout(timeout) { client.requestBatch(request, protocol, extraHeaders, extraParams) }
                                chunks.send(batch)
                                if (batch.upToDate || batch.httpStatus != 200 || batch.cursor.handle == protocol["expired_handle"] || batch.cursor.offset == protocol["offset"]) break
                                protocol = protocol + mapOf("handle" to batch.cursor.handle, "offset" to batch.cursor.offset)
                            }
                            chunks.close()
                        } catch (e: Throwable) { chunks.close(e) }
                    }
                    task.invokeOnCompletion { chunks.close(it) }
                    mutex.withLock { network.value = task; if (generation != plan.second || paused.value) task.cancel() else task.start() }
                    try { for (batch in chunks) { if (!accept(batch, plan.second, false)) break; errorRetries = 0 } }
                    finally { task.cancel(); chunks.cancel() }
                } else {
                    val task = worker.async(start = CoroutineStart.LAZY) { client.requestBatch(request, plan.first, extraHeaders, extraParams) }
                    mutex.withLock { network.value = task; if (generation != plan.second || paused.value) task.cancel() else task.start() }
                    val batch = try {
                        val timeout = options.liveRequestTimeoutMillis
                        if (timeout != null && (plan.first["live"] == "true" || refreshing)) withTimeout(timeout) { task.await() } else task.await()
                    } finally { task.cancel() }
                    accept(batch, plan.second, false)
                    errorRetries = 0
                }
                tick.update { it + 1 }
                mutex.withLock { updateStatus() }
            } catch (e: CancellationException) {
                currentCoroutineContext().ensureActive()
                if (e is TimeoutCancellationException) refreshWithoutWaiting()
                // Cancellation from pause/refresh/watchdog restarts the request without data loss.
            } catch (e: ShapeInvalidatedException) {
                mutex.withLock {
                    e.requestUrl?.let { shapeKey = canonicalShapeKey(it) }
                    shapeKey?.let { key -> state.fields.handle?.let { cache.markExpired(key, it) } }
                    state = state.reset(e.replacementHandle); tracker.clear(); pendingBuster = createCacheBuster(); generation++
                    if (paused.value) state = state.pause()
                    updateStatus(connected = false)
                }
                publish(JsonArray(listOf(buildJsonObject { putJsonObject("headers") { put("control", "must-refetch") } })))
            } catch (e: Throwable) {
                if (plan.third) client.retryPolicy.onFailedAttempt?.invoke()
                if (plan.third && (e is IOException || e is HttpRequestTimeoutException || e is ShapeProtocolException || e is ShapeHttpException && (e.status == 429 || e.status in 500..599))) {
                    if (++sseFailures < client.retryPolicy.maxAttempts) {
                        mutex.withLock {
                            state = state.sseClosed(0, false).state
                            updateStatus(connected = false)
                        }
                        delay(client.retryPolicy.delayMillis(sseFailures - 1, (e as? ShapeHttpException)?.retryAfterMillis))
                        continue
                    }
                }
                mutex.withLock { state = state.toError(e); updateStatus(connected = false) }
                val retry = options.onError?.invoke(e)
                if (retry == null || ++errorRetries > 50) {
                    subscribers.value.forEach { runCatching { it.error(e) } }
                    return
                }
                delay(client.retryPolicy.delayMillis(errorRetries - 1, null))
                mutex.withLock { extraHeaders += retry.headers; extraParams += retry.params; state = state.retry(); updateStatus(connected = false, failure = null) }
            } finally { network.value = null }
        }
    }
    internal fun launchTask(block: suspend CoroutineScope.() -> Unit): Job = worker.launch(block = block)

    suspend fun fetchSnapshot(params: SubsetParams): SubsetSnapshot {
        val protocol = mutex.withLock { state.parameters(snapshot = true) }
        val result = client.requestSnapshot(request, params, protocol, options.subsetMethod, extraHeaders, extraParams, mutex.withLock { state.fields.schema }) { usedHandle, failure ->
            mutex.withLock {
                failure.requestUrl?.let { url -> usedHandle?.let { cache.markExpired(canonicalShapeKey(url), it) } }
                failure.replacementHandle?.let { state = state.withHandle(it) }
                updateStatus()
            }
        }
        val schema = mutex.withLock { state.fields.schema ?: result.schema }
        return result.copy(data = parser.parse(result.data, schema))
    }
    suspend fun requestSnapshot(params: SubsetParams): SubsetSnapshot {
        require(mode == LogMode.ChangesOnly) { "Snapshot injection requires changes_only mode" }
        start()
        val reason = mutex.withLock { "snapshot-${++snapshotCounter}" }
        pause(reason)
        try {
            val result = subsetMutex.withLock {
                val result = fetchSnapshot(params)
                mutex.withLock {
                    tracker.add(result.metadata, result.data.map { it.jsonObject.getValue("key").jsonPrimitive.content }.toSet())
                    state = state.messages(true, false, false, null, options.clock(), state.fields.liveCacheBuster).state
                    updateStatus()
                }
                result
            }
            val metadata = wireJson.encodeToJsonElement(SnapshotMetadata.serializer(), result.metadata).jsonObject
            val end = buildJsonObject { put("headers", JsonObject(metadata + ("control" to JsonPrimitive("snapshot-end")))) }
            val subsetEnd = buildJsonObject { put("headers", JsonObject(wireJson.encodeToJsonElement(SubsetParams.serializer(), params).jsonObject + ("control" to JsonPrimitive("subset-end")))) }
            publish(JsonArray(result.data + listOf(end, subsetEnd)))
            mutex.withLock {
                if (result.responseHandle != null || result.responseOffset != null) state = state.response(ResponseMetadata(200, result.responseHandle, result.responseOffset, schema = result.schema, now = options.clock(), cacheBuster = ::createCacheBuster)).state
                updateStatus()
            }
            return result
        } finally { resume(reason) }
    }
}
