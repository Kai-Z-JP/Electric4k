package electric4k

import java.nio.file.Files
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class FileShapeStoreTest {
    @Test fun survivesNewInstanceAndReplacesAtomically() = runTest {
        val directory = Files.createTempDirectory("electric4k-test")
        try {
            val first = StoredShape("shape", cursor = ShapeCursor("10_0", "handle"))
            FileShapeStore(directory).save("../tenant", first)
            assertEquals(first, FileShapeStore(directory).load("../tenant"))
            val second = first.copy(cursor = ShapeCursor("20_0", "handle"))
            FileShapeStore(directory).save("../tenant", second)
            assertEquals(second, FileShapeStore(directory).load("../tenant"))
            assertEquals(1, Files.list(directory).use { it.count() })
            FileShapeStore(directory).clear("../tenant")
            assertNull(FileShapeStore(directory).load("../tenant"))
        } finally { directory.toFile().deleteRecursively() }
    }
}
