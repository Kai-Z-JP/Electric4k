package electric4k

import kotlinx.datetime.Clock
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/** Optional synchronous persistence for the small protocol cache, separate from application row storage. */
interface ProtocolCachePersistence { fun read(): String?; fun write(value: String) }
internal expect fun platformProtocolCachePersistence(): ProtocolCachePersistence?
@Serializable
private data class ProtocolCacheEntry(val expired: String? = null, val cursor: String? = null, val syncedAt: Long? = null, val lastUsed: Long = 0)
class ProtocolCache(private val persistence: ProtocolCachePersistence? = null, private val clock: () -> Long = { Clock.System.now().toEpochMilliseconds() }) {
    private val mutex = Mutex()
    private val entries = runCatching { persistence?.read()?.let { wireJson.decodeFromString<Map<String, ProtocolCacheEntry>>(it) } }.getOrNull().orEmpty().toMutableMap()
    internal suspend fun expired(key: String): String? = mutex.withLock { entries[key]?.let { entries[key] = it.copy(lastUsed = clock()); it.expired } }
    internal suspend fun markExpired(key: String, handle: String) = mutex.withLock { entries[key] = (entries[key] ?: ProtocolCacheEntry()).copy(expired = handle, lastUsed = clock()); save() }
    internal suspend fun replayCursor(key: String): String? = mutex.withLock { entries[key]?.takeIf { it.syncedAt != null && clock() - it.syncedAt < 60_000 }?.cursor }
    internal suspend fun synced(key: String, cursor: String) = mutex.withLock { entries[key] = (entries[key] ?: ProtocolCacheEntry()).copy(cursor = cursor, syncedAt = clock(), lastUsed = clock()); save() }
    internal suspend fun forgetExpired(key: String) = mutex.withLock { entries[key]?.let { entries[key] = it.copy(expired = null) }; save() }
    suspend fun clear() = mutex.withLock { entries.clear(); save() }
    internal suspend fun delete(key: String) = mutex.withLock { entries.remove(key); save() }
    private fun save() {
        while (entries.size > 250) entries.remove(entries.minBy { it.value.lastUsed }.key)
        runCatching { persistence?.write(wireJson.encodeToString(kotlinx.serialization.builtins.MapSerializer(String.serializer(), ProtocolCacheEntry.serializer()), entries)) }
    }
}

internal object SharedProtocolCache { val value by lazy { ProtocolCache(platformProtocolCachePersistence()) } }
