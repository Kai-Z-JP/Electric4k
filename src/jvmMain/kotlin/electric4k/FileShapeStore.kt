package electric4k

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.*
import java.security.MessageDigest
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/** Atomic single-file checkpoint per key. Use one writer per key; files contain application data. */
class FileShapeStore(private val directory: Path) : ShapeStore {
    private fun path(key: String): Path {
        val name = MessageDigest.getInstance("SHA-256").digest(key.toByteArray(Charsets.UTF_8))
            .joinToString("") { "%02x".format(it) }
        return directory.resolve("$name.json")
    }
    override suspend fun load(key: String): StoredShape? = withContext(Dispatchers.IO) {
        val file = path(key)
        if (Files.notExists(file)) null else wireJson.decodeFromString<StoredShape>(Files.readString(file))
    }
    override suspend fun save(key: String, shape: StoredShape) = withContext(Dispatchers.IO) {
        Files.createDirectories(directory)
        val temporary = Files.createTempFile(directory, ".shape-", ".tmp")
        try {
            val bytes = wireJson.encodeToString(StoredShape.serializer(), shape).toByteArray(Charsets.UTF_8)
            FileChannel.open(temporary, StandardOpenOption.WRITE).use { channel ->
                val buffer = ByteBuffer.wrap(bytes)
                while (buffer.hasRemaining()) channel.write(buffer)
                channel.force(true)
            }
            Files.move(temporary, path(key), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        } finally { Files.deleteIfExists(temporary) }
        Unit
    }
    override suspend fun clear(key: String) = withContext(Dispatchers.IO) { Files.deleteIfExists(path(key)); Unit }
}
