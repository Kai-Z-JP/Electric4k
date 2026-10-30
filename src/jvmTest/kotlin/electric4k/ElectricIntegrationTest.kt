package electric4k

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.serialization.json.*
import java.nio.file.Files
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import io.ktor.http.*
import io.ktor.client.statement.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.*

/** Explicit integrationTest task only. Requires the documented local Docker fixture. */
class ElectricIntegrationTest {
    private fun sql(statement: String) {
        val process = ProcessBuilder("docker", "--host=unix:///var/run/docker.sock", "exec", "electric4k-test-pg", "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "electric4k", "-c", statement).redirectErrorStream(true).start()
        val output = process.inputStream.bufferedReader().readText()
        check(process.waitFor() == 0) { output }
    }
    @Test fun snapshotInsertUpdateDeleteAndResume() = runBlocking {
        val table = "kmp_${System.nanoTime()}"
        val directory = Files.createTempDirectory("electric4k-integration")
        sql("CREATE TABLE $table (id integer PRIMARY KEY, title text NOT NULL, done boolean NOT NULL DEFAULT false); INSERT INTO $table VALUES (1, 'first', false);")
        val client = ShapeClient(System.getenv("ELECTRIC_TEST_URL") ?: "http://127.0.0.1:33001/v1/shape")
        try {
            withTimeout(90_000) {
                val updates = Channel<ShapeSnapshot<JsonObject>>(Channel.UNLIMITED)
                val store = FileShapeStore(directory)
                val job = launch { client.sync(ShapeRequest(table), "integration", store).snapshots().filter { it.status == SyncStatus.UpToDate }.collect { updates.send(it) } }
                try {
                    suspend fun nextMatching(predicate: (ShapeSnapshot<JsonObject>) -> Boolean): ShapeSnapshot<JsonObject> {
                        while (true) { val snapshot = updates.receive(); if (predicate(snapshot)) return snapshot }
                    }
                    val initial = nextMatching { it.rows.size == 1 }
                    assertEquals("first", initial.rows.values.single()["title"]!!.jsonPrimitive.content)
                    sql("INSERT INTO $table VALUES (2, 'second', true)")
                    nextMatching { it.rows.size == 2 }
                    sql("UPDATE $table SET title = 'changed' WHERE id = 1")
                    nextMatching { it.rows.values.any { row -> row["title"]?.jsonPrimitive?.content == "changed" } }
                    sql("DELETE FROM $table WHERE id = 2")
                    val final = nextMatching { it.rows.size == 1 }
                    assertEquals("changed", final.rows.values.single()["title"]!!.jsonPrimitive.content)
                    job.cancelAndJoin()
                    val restored = client.sync(ShapeRequest(table), "integration", FileShapeStore(directory)).snapshots().first()
                    assertEquals(final.rows, restored.rows); assertEquals(final.cursor, restored.cursor)
                } finally { job.cancelAndJoin(); updates.close() }
            }
        } finally { client.close(); sql("DROP TABLE $table"); directory.toFile().deleteRecursively() }
    }
    @Test fun currentServerSseAndSubsetSnapshots() = runBlocking {
        val table = "subset_${System.nanoTime()}"
        sql("CREATE TABLE $table (id integer PRIMARY KEY, title text NOT NULL); INSERT INTO $table VALUES (1, 'first'), (2, 'second');")
        val sseStarted = CompletableDeferred<Unit>()
        val sseResponses = AtomicInteger()
        val transport = HttpClient(CIO)
        transport.plugin(HttpSend).intercept { request ->
            if (request.url.parameters["live_sse"] == "true") sseStarted.complete(Unit)
            val call = execute(request)
            if (call.response.contentType()?.withoutParameters() == ContentType.Text.EventStream) sseResponses.incrementAndGet()
            call
        }
        val client = ShapeClient(System.getenv("ELECTRIC_TEST_URL") ?: "http://127.0.0.1:33001/v1/shape", transport)
        val stream = ShapeStream(client, ShapeRequest(table, log = LogMode.ChangesOnly), this, ShapeStreamOptions(liveSse = true, detectSystemWake = false))
        val shape = Shape(stream)
        val changes = Channel<ShapeChange<JsonObject>>(Channel.UNLIMITED)
        shape.subscribe { changes.trySend(it) }
        try {
            withTimeout(90_000) {
                assertTrue(shape.awaitRows().isEmpty())
                shape.requestSnapshot(SubsetParams(where = "id = $1", params = mapOf("1" to JsonPrimitive(1)), method = "POST"))
                suspend fun matching(predicate: (ShapeChange<JsonObject>) -> Boolean): ShapeChange<JsonObject> {
                    while (true) { val change = changes.receive(); if (predicate(change)) return change }
                }
                matching { it.rows.size == 1 }
                assertEquals("first", shape.currentRows.single()["title"]!!.jsonPrimitive.content)
                val get = stream.fetchSnapshot(SubsetParams(where = "id = $1", params = mapOf("1" to JsonPrimitive(2)), limit = 1, orderBy = "id", method = "GET"))
                assertEquals("second", get.data.single().jsonObject["value"]!!.jsonObject["title"]!!.jsonPrimitive.content)
                sseStarted.await()
                // The unrequested row's update must not enter changes_only materialization.
                sql("UPDATE $table SET title = 'not-loaded' WHERE id = 2; UPDATE $table SET title = 'changed' WHERE id = 1;")
                val changed = matching { it.rows.any { row -> row["title"]?.jsonPrimitive?.content == "changed" } }
                assertEquals(1, changed.rows.size)
                assertTrue(sseResponses.get() > 0, "Updates must use a real SSE response")
                sql("DELETE FROM $table WHERE id = 1")
                matching { it.rows.isEmpty() }
                Unit
            }
        } finally { shape.close(); stream.close(); client.close(); transport.close(); changes.close(); sql("DROP TABLE $table") }
    }

}
