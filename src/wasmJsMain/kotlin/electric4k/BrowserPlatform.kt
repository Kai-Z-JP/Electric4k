package electric4k

import kotlin.js.JsAny
import kotlin.JsFun

@JsFun("() => typeof globalThis.localStorage !== 'undefined'")
private external fun hasStorage(): Boolean
@JsFun("(key) => globalThis.localStorage.getItem(key)")
private external fun readStorage(key: String): String?
@JsFun("(key, value) => globalThis.localStorage.setItem(key, value)")
private external fun writeStorage(key: String, value: String)
@JsFun("(key) => globalThis.localStorage.removeItem(key)")
private external fun removeStorage(key: String)
@JsFun("() => typeof document !== 'undefined'")
private external fun hasDocument(): Boolean
@JsFun("() => document.visibilityState !== 'hidden'")
private external fun visible(): Boolean
@JsFun("(callback) => { const listener = () => callback(document.visibilityState !== 'hidden'); document.addEventListener('visibilitychange', listener); return listener; }")
private external fun listen(callback: (Boolean) -> Unit): JsAny
@JsFun("(listener) => document.removeEventListener('visibilitychange', listener)")
private external fun unlisten(listener: JsAny)

class LocalStorageShapeStore(private val prefix: String = "electric4k:") : ShapeStore {
    override suspend fun load(key: String): StoredShape? = readStorage(prefix + key)?.let { wireJson.decodeFromString<StoredShape>(it) }
    override suspend fun save(key: String, shape: StoredShape) = writeStorage(prefix + key, wireJson.encodeToString(StoredShape.serializer(), shape))
    override suspend fun clear(key: String) = removeStorage(prefix + key)
}

internal actual fun platformProtocolCachePersistence(): ProtocolCachePersistence? = if (hasStorage()) object : ProtocolCachePersistence {
    override fun read(): String? = readStorage("electric4k_protocol_cache")
    override fun write(value: String) = writeStorage("electric4k_protocol_cache", value)
} else null

internal actual fun platformRuntimeVisibility(): RuntimeVisibilityAdapter? = if (hasDocument()) object : RuntimeVisibilityAdapter {
    override fun isVisible(): Boolean = visible()
    override fun subscribe(callback: (Boolean) -> Unit): () -> Unit {
        val listener = listen(callback)
        return { unlisten(listener) }
    }
} else null
