@file:OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)

package electric4k

import io.ktor.client.*
import io.ktor.client.engine.mock.*
import io.ktor.http.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.test.*
import kotlinx.datetime.Instant
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import kotlin.test.*

private const val UP = """{"headers":{"control":"up-to-date","global_last_seen_lsn":"20"}}"""
private const val INSERT = """{"headers":{"operation":"insert"},"key":"row1","value":{"id":"1","title":"before","done":"f"}}"""
private const val SCHEMA = """{"id":{"type":"int8"},"done":{"type":"bool"},"title":{"type":"text"}}"""
internal fun responseHeaders(offset: String = "10_0", handle: String = "h1", type: String = "application/json") = headersOf(
    "electric-offset" to listOf(offset), "electric-handle" to listOf(handle),
    "electric-cursor" to listOf("cache-1"), "electric-schema" to listOf(SCHEMA), "Content-Type" to listOf(type),
)
class ClientTest {
    @Test fun queryAndMetadata() = runTest {
        val http = HttpClient(MockEngine { request ->
            assertEquals("a b", request.url.parameters["table"])
            assertEquals("title = $1", request.url.parameters["where"])
            assertEquals("a&b", request.url.parameters["params[1]"])
            assertEquals("cache-0", request.url.parameters["cursor"])
            assertEquals("secret", request.headers["Authorization"])
            respond("[$UP]", headers = responseHeaders())
        })
        try {
            val client = ShapeClient("https://example.test/v1/shape", http, headers = { mapOf("Authorization" to "secret") })
            val result = client.fetch(ShapeRequest("a b", "title = $1", params = mapOf("1" to "a&b")), ShapeCursor("5_0", "h1", "cache-0"), true)
            assertTrue(result.upToDate); assertEquals("cache-1", result.cursor.cacheBuster)
            assertEquals("int8", result.schema["id"]?.type)
        } finally { http.close() }
    }
    @Test fun noContentKeepsCursor() = runTest {
        val http = HttpClient(MockEngine { respond("", HttpStatusCode.NoContent, responseHeaders()) })
        try {
            val cursor = ShapeCursor("10_0", "h1", "cache-1")
            val batch = ShapeClient("https://example.test/shape", http).fetch("todos", cursor, true)
            assertEquals(cursor, batch.cursor); assertTrue(batch.upToDate); assertTrue(batch.messages.isEmpty())
        } finally { http.close() }
    }
    @Test fun retriesRespectRetryAfter() = runTest {
        var calls = 0
        var failedAttempts = 0
        val http = HttpClient(MockEngine {
            if (++calls < 3) respond("", HttpStatusCode.TooManyRequests, headersOf("Retry-After", "2"))
            else respond("[$UP]", headers = responseHeaders())
        })
        try {
            ShapeClient("https://example.test/shape", http, RetryPolicy(onFailedAttempt = { failedAttempts++ })).fetch("todos")
            assertEquals(2, failedAttempts)
            assertEquals(3, calls); assertEquals(4000, testScheduler.currentTime)
        } finally { http.close() }
    }
    @Test fun authRefreshOnce() = runTest {
        var token = "expired"; var refreshes = 0; var calls = 0
        val http = HttpClient(MockEngine { request ->
            calls++
            if (request.headers["Authorization"] == "expired") respond("", HttpStatusCode.Unauthorized)
            else respond("[$UP]", headers = responseHeaders())
        })
        try {
            val client = ShapeClient("https://example.test/shape", http, headers = { mapOf("Authorization" to token) }, refreshAuth = { refreshes++; token = "fresh"; true })
            client.fetch("todos"); assertEquals(2, calls); assertEquals(1, refreshes)
        } finally { http.close() }
    }
    @Test fun permanentFailureDoesNotRetry() = runTest {
        var calls = 0
        val http = HttpClient(MockEngine { calls++; respond("", HttpStatusCode.BadRequest) })
        try {
            assertEquals(400, assertFailsWith<ShapeHttpException> { ShapeClient("https://example.test/shape", http).fetch("todos") }.status)
            assertEquals(1, calls)
        } finally { http.close() }
    }
    @Test fun retryBudgetIsBounded() = runTest {
        var calls = 0
        val http = HttpClient(MockEngine { calls++; respond("", HttpStatusCode.ServiceUnavailable) })
        try {
            assertFailsWith<ShapeHttpException> { ShapeClient("https://example.test/shape", http, RetryPolicy(3, 0, 0)).fetch("todos") }
            assertEquals(3, calls)
        } finally { http.close() }
    }
    @Test fun malformedPayloadAndMissingMetadataFail() = runTest {
        val http = HttpClient(MockEngine { respond("[]") })
        try { assertFailsWith<ShapeProtocolException> { ShapeClient("https://example.test/shape", http).fetch("todos") } }
        finally { http.close() }
    }
    @Test fun changedHandleIsAcceptedUnlessKnownExpired() = runTest {
        val http = HttpClient(MockEngine { respond("[$UP]", headers = responseHeaders(handle = "new")) })
        try { assertEquals("new", ShapeClient("https://example.test/shape", http).fetch("todos", ShapeCursor("0_0", "old")).cursor.handle) }
        finally { http.close() }
    }
    @Test fun liveRequiresCursor() = runTest {
        val http = HttpClient(MockEngine { error("Must not call network") })
        try { assertFailsWith<IllegalArgumentException> { ShapeClient("https://example.test/shape", http).fetch("todos", live = true) } }
        finally { http.close() }
    }
    @Test fun sseFramesAndIncompleteTail() = runTest {
        val body = ": keepalive\r\ndata: $INSERT\r\n\r\ndata: $UP\r\n\r\ndata: $INSERT\r\n\r\n"
        val http = HttpClient(MockEngine { request ->
            assertEquals("true", request.url.parameters["live_sse"])
            respond(body, headers = responseHeaders(type = "text/event-stream"))
        })
        try {
            val batches = ShapeClient("https://example.test/shape", http).events(ShapeRequest("todos"), ShapeCursor("10_0", "h1")).toList()
            assertEquals(1, batches.size); assertEquals(2, batches[0].messages.size); assertEquals("20_0", batches[0].cursor.offset)
        } finally { http.close() }
    }
    @Test fun sseInvalidationDiscardsPendingBatch() = runTest {
        val http = HttpClient(MockEngine { respond("data: $INSERT\n\ndata: {\"headers\":{\"control\":\"must-refetch\"}}\n\n", headers = responseHeaders(type = "text/event-stream")) })
        try {
            var emitted = 0
            assertFailsWith<ShapeInvalidatedException> { ShapeClient("https://example.test/shape", http).events(ShapeRequest("todos"), ShapeCursor("10_0", "h1")).collect { emitted++ } }
            assertEquals(0, emitted)
        } finally { http.close() }
    }
    @Test fun httpDateRetryAfter() {
        assertEquals(2000, retryAfterMillis("Wed, 21 Oct 2015 07:28:00 GMT", Instant.parse("2015-10-21T07:27:58Z")))
        assertNull(retryAfterMillis("invalid"))
        assertEquals(1500, retryAfterMillis("1.5"))
        assertEquals(3_600_000, retryAfterMillis("Thu, 22 Oct 2015 07:28:00 GMT", Instant.parse("2015-10-21T07:27:58Z")))
    }
    @Test fun cancellationIsNotRetried() = runTest {
        var calls = 0
        val started = CompletableDeferred<Unit>()
        val http = HttpClient(MockEngine { calls++; started.complete(Unit); awaitCancellation() })
        try {
            val job = launch { ShapeClient("https://example.test/shape", http).fetch("todos") }
            started.await(); job.cancelAndJoin(); assertEquals(1, calls)
        } finally { http.close() }
    }
}

@Serializable private data class Todo(val id: Long, val title: String, val done: Boolean)
class SyncTest {
    @Test fun materializesChangesAndPersistsResume() = runTest {
        var calls = 0
        val http = HttpClient(MockEngine { request ->
            calls++
            when (calls) {
                1 -> respond("[$INSERT,$UP]", headers = responseHeaders())
                2 -> { assertEquals("true", request.url.parameters["live"]); respond("""[{"headers":{"operation":"update"},"key":"row1","value":{"title":"after"}},$UP]""", headers = responseHeaders("20_0")) }
                else -> { assertEquals("20_0", request.url.parameters["offset"]); respond("""[{"headers":{"operation":"delete"},"key":"row1"},$UP]""", headers = responseHeaders("30_0")) }
            }
        })
        val store = MemoryShapeStore()
        try {
            val client = ShapeClient("https://example.test/shape", http)
            val states = ShapeSync(client, ShapeRequest("todos"), serializedRows(Todo.serializer()), store, "user1", reconnectDelayMillis = 0).snapshots().take(3).toList()
            assertEquals(Todo(1, "after", false), states.last().rows["row1"])
            assertEquals("20_0", store.load("user1")?.cursor?.offset)
            val resumed = client.sync(ShapeRequest("todos"), "user1", store).snapshots().take(2).toList()
            assertEquals("20_0", resumed.first().cursor?.offset); assertTrue(resumed.last().rows.isEmpty())
        } finally { http.close() }
    }
    @Test fun snapshotChunksAreNotPublishedEarly() = runTest {
        var calls = 0
        val http = HttpClient(MockEngine {
            if (++calls == 1) respond("[$INSERT]", headers = responseHeaders())
            else respond("[$UP]", headers = responseHeaders("20_0"))
        })
        try {
            val states = ShapeClient("https://example.test/shape", http).sync(ShapeRequest("todos"), "one").snapshots().take(2).toList()
            assertTrue(states.first().rows.isEmpty()); assertEquals(1, states.last().rows.size); assertEquals(2, calls)
        } finally { http.close() }
    }
    @Test fun invalidationResetsAndUsesReplacementHandle() = runTest {
        var calls = 0
        val http = HttpClient(MockEngine { request ->
            when (++calls) {
                1 -> respond("[$INSERT,$UP]", headers = responseHeaders())
                2 -> respond("", HttpStatusCode.Conflict, headersOf("electric-handle", "new"))
                else -> {
                    assertEquals("-1", request.url.parameters["offset"]); assertEquals("new", request.url.parameters["handle"])
                    respond("[$UP]", headers = responseHeaders(handle = "new"))
                }
            }
        })
        try {
            val states = ShapeClient("https://example.test/shape", http).sync(ShapeRequest("todos"), "one").snapshots().take(4).toList()
            assertEquals(SyncStatus.Reset, states[2].status); assertTrue(states[2].rows.isEmpty()); assertTrue(states.last().rows.isEmpty())
        } finally { http.close() }
    }
    @Test fun failedPersistenceDoesNotPublishCheckpoint() = runTest {
        val memory = MemoryShapeStore()
        val failing = object : ShapeStore by memory { override suspend fun save(key: String, shape: StoredShape) { error("disk full") } }
        val http = HttpClient(MockEngine { respond("[$INSERT,$UP]", headers = responseHeaders()) })
        try {
            var count = 0
            assertFailsWith<IllegalStateException> { ShapeClient("https://example.test/shape", http).sync(ShapeRequest("todos"), "one", failing).snapshots().collect { count++ } }
            assertEquals(1, count); assertNull(memory.load("one"))
        } finally { http.close() }
    }
    @Test fun rejectsDifferentShapeForStoredKey() = runTest {
        val http = HttpClient(MockEngine { respond("[$UP]", headers = responseHeaders()) })
        val store = MemoryShapeStore()
        try {
            val client = ShapeClient("https://example.test/shape", http)
            client.sync(ShapeRequest("todos"), "one", store).snapshots().take(2).collect()
            assertFailsWith<IllegalArgumentException> { client.sync(ShapeRequest("other"), "one", store).snapshots().first() }
        } finally { http.close() }
    }
    @Test fun conversionFailureDoesNotAdvanceCursor() = runTest {
        val http = HttpClient(MockEngine { respond("[$INSERT,$UP]", headers = responseHeaders()) })
        val store = MemoryShapeStore()
        try {
            val sync = ShapeSync(ShapeClient("https://example.test/shape", http), ShapeRequest("todos"), RowDecoder<Unit> { _, _ -> error("conversion") }, store, "one")
            assertFailsWith<IllegalStateException> { sync.snapshots().collect() }
            assertNull(store.load("one"))
        } finally { http.close() }
    }
}
class TypesTest {
    @Test fun scalarsArraysAndNulls() {
        val schema = mapOf("numbers" to ColumnInfo("int8", dims = 2), "json" to ColumnInfo("jsonb"), "decimal" to ColumnInfo("numeric"), "strings" to ColumnInfo("text", dims = 1))
        val row = buildJsonObject {
            put("numbers", "{{1,NULL},{9223372036854775807,2}}")
            put("json", "{\"nested\":true}"); put("decimal", "12345678901234567890.123456789")
            put("strings", "{\"NULL\",NULL,\"a,b\",\"a\\\"b\"}")
        }
        val converted = PostgresTypes().convert(row, schema)
        assertEquals(Long.MAX_VALUE, converted["numbers"]!!.jsonArray[1].jsonArray[0].jsonPrimitive.long)
        assertEquals(JsonNull, converted["numbers"]!!.jsonArray[0].jsonArray[1])
        assertTrue(converted["json"]!!.jsonObject["nested"]!!.jsonPrimitive.boolean)
        assertEquals(row["decimal"], converted["decimal"])
        assertEquals("NULL", converted["strings"]!!.jsonArray[0].jsonPrimitive.content)
        assertEquals(JsonNull, converted["strings"]!!.jsonArray[1])
        assertEquals("a\"b", converted["strings"]!!.jsonArray[3].jsonPrimitive.content)
    }
    @Test fun customTypeParser() {
        assertEquals(JsonPrimitive(42), PostgresTypes(mapOf("custom" to { JsonPrimitive(it.toInt() * 2) })).convert(buildJsonObject { put("value", "21") }, mapOf("value" to ColumnInfo("custom")))["value"])
    }
    @Test fun jsonStoreRoundTrip() = runTest {
        val values = mutableMapOf<String, String>()
        val store = JsonShapeStore({ values[it] }, { key, value -> values[key] = value }, { values.remove(it); Unit })
        val shape = StoredShape("id", cursor = ShapeCursor("10_0", "h"))
        store.save("key", shape); assertEquals(shape, store.load("key")); store.clear("key"); assertNull(store.load("key"))
    }
}

class StreamingTest {
    @Test fun sseRefreshesAuth() = runTest {
        var token = "old"; var calls = 0
        val http = HttpClient(MockEngine { request ->
            calls++
            if (request.headers["Authorization"] == "old") respond("", HttpStatusCode.Unauthorized)
            else respond("data: $UP\n\n", headers = responseHeaders(type = "text/event-stream"))
        })
        try {
            val client = ShapeClient("https://example.test/shape", http, headers = { mapOf("Authorization" to token) }, refreshAuth = { token = "new"; true })
            assertEquals(1, client.events(ShapeRequest("todos"), ShapeCursor("10_0", "h1")).toList().size)
            assertEquals(2, calls)
        } finally { http.close() }
    }
    @Test fun sseReconnectsAtLastCommittedBoundary() = runTest {
        var calls = 0
        val http = HttpClient(MockEngine { request ->
            when (++calls) {
                1 -> respond("[$UP]", headers = responseHeaders())
                2 -> respond("data: $INSERT\n\ndata: $UP\n\ndata: $INSERT\n\n", headers = responseHeaders(type = "text/event-stream"))
                else -> {
                    assertEquals("20_0", request.url.parameters["offset"])
                    respond("data: {\"headers\":{\"control\":\"up-to-date\",\"global_last_seen_lsn\":\"30\"}}\n\n", headers = responseHeaders(type = "text/event-stream"))
                }
            }
        })
        try {
            val states = ShapeClient("https://example.test/shape", http).sync(ShapeRequest("todos"), "sse", mode = SyncMode.Sse).snapshots().take(4).toList()
            assertEquals("30_0", states.last().cursor?.offset); assertEquals(1, states.last().rows.size)
            assertEquals(3, calls)
        } finally { http.close() }
    }
    @Test fun authQueryCannotOverrideOffset() = runTest {
        val http = HttpClient(MockEngine { error("No request expected") })
        try {
            assertFailsWith<IllegalArgumentException> { ShapeClient("https://example.test/shape", http, queryParams = { mapOf("offset" to "wrong") }).fetch("todos") }
        } finally { http.close() }
    }
    @Test fun cloudAuthQueryIsEncoded() = runTest {
        val http = HttpClient(MockEngine { request ->
            assertEquals("a&b", request.url.parameters["secret"])
            assertEquals("source", request.url.parameters["source_id"])
            respond("[$UP]", headers = responseHeaders())
        })
        try { ShapeClient("https://example.test/shape", http, queryParams = { mapOf("secret" to "a&b", "source_id" to "source") }).fetch("todos") }
        finally { http.close() }
    }
}
