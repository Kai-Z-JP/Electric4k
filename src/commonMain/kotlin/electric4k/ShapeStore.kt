package electric4k

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject

@Serializable
data class StoredShape(
    val identity: String,
    val rows: Map<String, JsonObject> = emptyMap(),
    val cursor: ShapeCursor? = null,
    val schema: ShapeSchema = emptyMap(),
    val upToDate: Boolean = false,
    val initialHandle: String? = null,
    val version: Int = 1,
)
/** Implementations must atomically replace rows AND cursor; one active writer per key is required. */
interface ShapeStore {
    suspend fun load(key: String): StoredShape?
    suspend fun save(key: String, shape: StoredShape)
    suspend fun clear(key: String)
}
class MemoryShapeStore : ShapeStore {
    private val mutex = Mutex()
    private val values = mutableMapOf<String, String>()
    override suspend fun load(key: String): StoredShape? = mutex.withLock { values[key]?.let { wireJson.decodeFromString<StoredShape>(it) } }
    override suspend fun save(key: String, shape: StoredShape) { mutex.withLock { values[key] = wireJson.encodeToString(StoredShape.serializer(), shape) } }
    override suspend fun clear(key: String) { mutex.withLock { values.remove(key) } }
}
/** Adapter for a transactional database or platform key-value store. Callbacks must not log contents. */
class JsonShapeStore(
    private val read: suspend (String) -> String?,
    private val write: suspend (String, String) -> Unit,
    private val remove: suspend (String) -> Unit,
) : ShapeStore {
    override suspend fun load(key: String): StoredShape? = read(key)?.let { wireJson.decodeFromString<StoredShape>(it) }
    override suspend fun save(key: String, shape: StoredShape) = write(key, wireJson.encodeToString(StoredShape.serializer(), shape))
    override suspend fun clear(key: String) = remove(key)
}
