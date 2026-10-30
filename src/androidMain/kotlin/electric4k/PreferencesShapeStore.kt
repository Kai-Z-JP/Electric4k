package electric4k

import android.content.SharedPreferences
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/** Dedicated private SharedPreferences recommended. A complete checkpoint is one atomic value. */
class PreferencesShapeStore(private val preferences: SharedPreferences, private val prefix: String = "electric4k:") : ShapeStore {
    override suspend fun load(key: String): StoredShape? = withContext(Dispatchers.IO) {
        preferences.getString(prefix + key, null)?.let { wireJson.decodeFromString<StoredShape>(it) }
    }
    override suspend fun save(key: String, shape: StoredShape) = withContext(Dispatchers.IO) {
        check(preferences.edit().putString(prefix + key, wireJson.encodeToString(StoredShape.serializer(), shape)).commit()) { "Checkpoint could not be persisted" }
    }
    override suspend fun clear(key: String) = withContext(Dispatchers.IO) {
        check(preferences.edit().remove(prefix + key).commit()) { "Checkpoint could not be cleared" }
    }
}
