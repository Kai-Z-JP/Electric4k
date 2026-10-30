@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package electric4k

import platform.Foundation.NSUserDefaults

/** Single-value checkpoint for small shapes. Use a transactional JsonShapeStore for large datasets. */
class UserDefaultsShapeStore(private val defaults: NSUserDefaults = NSUserDefaults.standardUserDefaults, private val prefix: String = "electric4k:") : ShapeStore {
    override suspend fun load(key: String): StoredShape? = defaults.stringForKey(prefix + key)?.let { wireJson.decodeFromString<StoredShape>(it) }
    override suspend fun save(key: String, shape: StoredShape) {
        defaults.setObject(wireJson.encodeToString(StoredShape.serializer(), shape), forKey = prefix + key)
    }
    override suspend fun clear(key: String) { defaults.removeObjectForKey(prefix + key) }
}
