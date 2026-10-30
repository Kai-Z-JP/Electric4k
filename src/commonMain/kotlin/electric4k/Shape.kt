package electric4k

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.serialization.json.*

/** Pure upstream Shape notification semantics, also exercised by differential fixtures. */
internal class ShapeMaterializer(private val mode: LogMode) {
    private val data = linkedMapOf<String, JsonObject>()
    private val insertedKeys = mutableSetOf<String>()
    var isUpToDate = false
        private set
    val value: Map<String, JsonObject> get() = data.toMap()
    fun process(messages: JsonArray): Boolean {
        var notify = false
        for (message in messages) {
            val objectMessage = message.jsonObject
            val headers = objectMessage["headers"]?.jsonObject ?: continue
            val operation = headers["operation"]?.jsonPrimitive?.content
            if (operation != null) {
                val wasUpToDate = isUpToDate
                isUpToDate = false
                val key = objectMessage.getValue("key").jsonPrimitive.content
                when (operation) {
                    "insert" -> { data[key] = objectMessage.getValue("value").jsonObject; insertedKeys.add(key); if (wasUpToDate) notify = true }
                    "update" -> if (mode == LogMode.Full || key in insertedKeys) {
                        data[key] = JsonObject(data[key].orEmpty() + objectMessage.getValue("value").jsonObject)
                        if (wasUpToDate) notify = true
                    }
                    "delete" -> if (mode == LogMode.Full || key in insertedKeys) { data.remove(key); insertedKeys.remove(key); if (wasUpToDate) notify = true }
                }
            }
            when (message.control()) {
                "up-to-date" -> { if (!isUpToDate) notify = true; isUpToDate = true }
                "must-refetch" -> { data.clear(); insertedKeys.clear(); isUpToDate = false }
            }
        }
        return notify
    }
}
data class ShapeChange<T>(val value: Map<String, T>, val rows: List<T>)
class Shape<T>(val stream: ShapeStream, private val decode: (JsonObject) -> T) {
    private val materializer = ShapeMaterializer(stream.mode)
    private val callbacks = MutableStateFlow<Map<Any, (ShapeChange<T>) -> Unit>>(emptyMap())
    private val _value = MutableStateFlow<Map<String, T>>(emptyMap())
    private val revision = MutableStateFlow(0L)
    private val _error = MutableStateFlow<Throwable?>(null)
    private val requestedSnapshots = MutableStateFlow<Set<SubsetParams>>(emptySet())
    private var reexecute = false
    private var replayJob: Job? = null
    private val unsubscribe = stream.subscribe({ messages ->
        if (messages.any { it.control() == "must-refetch" }) { _error.value = null; reexecute = true }
        val notify = materializer.process(messages)
        _value.value = materializer.value.mapValues { decode(it.value) }
        if (notify) notifySubscribers()
        if (reexecute && messages.any { it.control() == "up-to-date" }) {
            reexecute = false
            replayJob = stream.launchTask {
                try { requestedSnapshots.value.forEach { stream.requestSnapshot(it) } }
                catch (e: CancellationException) { throw e }
                catch (e: Throwable) { _error.value = e; notifySubscribers() }
            }
        }
    }, { failure -> _error.value = failure; notifySubscribers() })
    val currentValue: Map<String, T> get() = _value.value
    val currentRows: List<T> get() = currentValue.values.toList()
    val isUpToDate: Boolean get() = materializer.isUpToDate
    val lastOffset: String get() = stream.lastOffset
    val handle: String? get() = stream.shapeHandle
    val error: Throwable? get() = _error.value
    val mode: LogMode get() = stream.mode
    val numSubscribers: Int get() = callbacks.value.size
    fun isLoading() = stream.isLoading()
    fun isConnected() = stream.isConnected()
    fun lastSyncedAt() = stream.lastSyncedAt()
    fun lastSynced() = stream.lastSynced()
    private fun notifySubscribers() {
        revision.update { it + 1 }
        val change = ShapeChange(currentValue, currentRows)
        callbacks.value.values.forEach { it(change) }
    }
    fun subscribe(callback: (ShapeChange<T>) -> Unit): () -> Unit {
        val id = Any(); callbacks.update { it + (id to callback) }
        return { callbacks.update { it - id } }
    }
    fun unsubscribeAll() { callbacks.value = emptyMap() }
    suspend fun awaitValue(): Map<String, T> {
        if (!isUpToDate && (!stream.isUpToDate || stream.deliveryPending.value)) combine(revision, stream.status, _error, stream.deliveryPending) { _, status, error, pending -> isUpToDate || status.isUpToDate && !pending || error != null || status.error != null }.first { it }
        (error ?: stream.error)?.let { throw it }
        return currentValue
    }
    suspend fun awaitRows(): List<T> = awaitValue().values.toList()
    suspend fun requestSnapshot(params: SubsetParams) {
        requestedSnapshots.update { it + params }
        awaitValue()
        stream.requestSnapshot(params)
    }
    fun close() { unsubscribe(); unsubscribeAll(); replayJob?.cancel() }
}
fun Shape(stream: ShapeStream): Shape<JsonObject> = Shape(stream) { it }
