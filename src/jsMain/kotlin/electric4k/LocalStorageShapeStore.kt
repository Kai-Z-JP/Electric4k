package electric4k

/** Browser localStorage adapter. Quota/security errors propagate; not available in Node. */
class LocalStorageShapeStore(private val prefix: String = "electric4k:") : ShapeStore {
    private val storage: dynamic get() = js("globalThis.localStorage")
    override suspend fun load(key: String): StoredShape? {
        val value = storage.getItem(prefix + key) as String?
        return value?.let { wireJson.decodeFromString<StoredShape>(it) }
    }
    override suspend fun save(key: String, shape: StoredShape) {
        storage.setItem(prefix + key, wireJson.encodeToString(StoredShape.serializer(), shape))
    }
    override suspend fun clear(key: String) { storage.removeItem(prefix + key) }
}
