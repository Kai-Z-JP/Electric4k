package electric4k

import kotlinx.serialization.json.*
import kotlin.test.*

class UpstreamCompatibilityTest {
    private fun JsonObject.string(key: String): String? = this[key]?.jsonPrimitive?.contentOrNull
    private fun JsonObject.bool(key: String): Boolean = this[key]?.jsonPrimitive?.booleanOrNull ?: false
    private fun JsonObject.long(key: String): Long = this[key]?.jsonPrimitive?.longOrNull ?: 0
    private fun describe(state: StreamState): JsonObject = buildJsonObject {
        put("kind", when (state.kind) { StreamKind.StaleRetry -> "stale-retry"; else -> state.kind.name.lowercase() })
        put("handle", state.fields.handle); put("offset", state.fields.offset)
        put("schema", state.fields.schema?.let { wireJson.encodeToJsonElement(it) } ?: JsonNull)
        put("cursor", state.fields.liveCacheBuster); put("lastSyncedAt", state.fields.lastSyncedAt?.let(::JsonPrimitive) ?: JsonNull)
        put("up", state.isUpToDate); put("stale", state.staleCacheBuster); put("retries", state.staleCacheRetryCount)
        put("fallback", state.sseFallback); put("short", state.consecutiveShortSseConnections); put("replay", state.replayCursor)
        put("params", JsonObject(state.parameters().mapValues { JsonPrimitive(it.value) }))
        put("previous", state.previous?.let(::describe) ?: JsonNull)
    }
    private fun apply(state: StreamState, e: JsonObject): StreamState = when(e.string("type")) {
        "response" -> state.response(ResponseMetadata(e.long("status").toInt(), e.string("handle"), e.string("offset"), e.string("cursor"), e["schema"]?.let { wireJson.decodeFromJsonElement<Map<String, ColumnInfo>>(it) }, e.string("expired"), e.long("now"), cacheBuster = { e.string("buster")!! })).state
        "messages" -> state.messages(e.bool("hasMessages"), e.bool("upToDate"), e.bool("sse"), e.string("offset"), e.long("now"), e.string("cursor")!!).state
        "pause" -> state.pause(); "resume" -> state.resume(); "error" -> state.toError(Exception("test")); "retry" -> state.retry()
        "reset" -> state.reset(e.string("handle")); "handle" -> state.withHandle(e.string("handle")!!); "replay" -> state.enterReplay(e.string("cursor")!!)
        "sseClose" -> state.sseClosed(e.long("duration"), e.bool("aborted")).state
        else -> error("Unknown oracle event")
    }
    @Test fun allSeventyStateEventPairsAndGeneratedSequencesMatchUpstream() {
        upstreamStateVectors.forEachIndexed { case, json ->
            var state = StreamState.initial()
            wireJson.parseToJsonElement(json).jsonArray.forEachIndexed { index, raw ->
                val step = raw.jsonObject; val before = state
                state = apply(state, step.getValue("event").jsonObject)
                assertEquals(step["expected"], describe(state), "Upstream state trace $case step $index")
                assertEquals(step["same"]!!.jsonPrimitive.boolean, state === before, "No-op identity at $case:$index")
            }
        }
    }
    @Test fun materializerNotificationsMatchUpstream() {
        upstreamShapeVectors.forEachIndexed { case, json ->
            val input = wireJson.parseToJsonElement(json).jsonObject
            val shape = ShapeMaterializer(if (input.string("mode") == "full") LogMode.Full else LogMode.ChangesOnly)
            var notifications = 0
            input["steps"]!!.jsonArray.forEachIndexed { index, raw ->
                val step = raw.jsonObject
                if (shape.process(step["batch"]!!.jsonArray)) notifications++
                assertEquals(step["value"], JsonObject(shape.value), "Upstream materializer data $case:$index")
                assertEquals(step.bool("up"), shape.isUpToDate)
                assertEquals(step.long("notifications").toInt(), notifications, "Upstream notification count $case:$index")
            }
        }
    }
    @Test fun columnMappingsMatchUpstream() {
        upstreamMappingVectors.forEach { json ->
            val row = wireJson.parseToJsonElement(json).jsonObject
            assertEquals(row.string("snake"), snakeToCamel(row.string("input")!!))
            assertEquals(row.string("camel"), camelToSnake(row.string("input")!!))
        }
    }
    @Test fun snapshotVisibilityMatchesUpstreamAcrossXidEpochs() {
        upstreamTrackerVectors.forEachIndexed { case, json ->
            val tracker = SnapshotTracker()
            wireJson.parseToJsonElement(json).jsonArray.forEachIndexed { index, raw ->
                val event = raw.jsonObject
                when (event.string("type")) {
                    "add" -> tracker.add(wireJson.decodeFromJsonElement(SnapshotMetadata.serializer(), event.getValue("metadata")), event.getValue("keys").jsonArray.map { it.jsonPrimitive.content }.toSet())
                    "lastSeen" -> tracker.lastSeen(event.string("lsn")!!.toULong())
                    "reject" -> assertEquals(event.bool("expected"), tracker.reject(event.getValue("message").jsonObject), "Snapshot trace $case:$index")
                }
            }
        }
    }
    private fun compareParsed(expected: JsonElement, actual: JsonElement) {
        when {
            expected is JsonObject && expected.containsKey("\$bigint") -> assertEquals(expected["\$bigint"]!!.jsonPrimitive.content, actual.jsonPrimitive.content)
            expected is JsonObject && expected.containsKey("\$number") -> assertEquals(expected["\$number"]!!.jsonPrimitive.content, actual.jsonPrimitive.content)
            expected is JsonObject -> { assertEquals(expected.keys, actual.jsonObject.keys); expected.forEach { (key, value) -> compareParsed(value, actual.jsonObject.getValue(key)) } }
            expected is JsonArray -> { assertEquals(expected.size, actual.jsonArray.size); expected.zip(actual.jsonArray).forEach { (e, a) -> compareParsed(e, a) } }
            expected is JsonPrimitive && expected !is JsonNull && !expected.isString && expected.doubleOrNull != null -> assertEquals(expected.double, actual.jsonPrimitive.double)
            else -> assertEquals(expected, actual)
        }
    }
    @Test fun allParserValuesAndOldValuesMatchUpstream() {
        upstreamParserVectors.forEach { json ->
            val test = wireJson.parseToJsonElement(json).jsonObject
            val schema = wireJson.decodeFromJsonElement<Map<String, ColumnInfo>>(test.getValue("schema"))
            val messages = test.getValue("messages").jsonArray
            if (test.bool("error")) assertFails { MessageParser().parse(messages, schema) }
            else compareParsed(test.getValue("expected"), MessageParser().parse(messages, schema))
        }
    }

    @Test fun everyExpressionOperatorMatchesUpstream() {
        upstreamExpressionVectors.forEach { json ->
            val test = wireJson.parseToJsonElement(json).jsonObject
            val expression = wireJson.decodeFromJsonElement(SerializedExpression.serializer(), test.getValue("expression"))
            assertEquals(test.string("expected"), compileExpression(expression, snakeCamelMapper()))
        }
    }

    @Test fun canonicalUrlsMatchPublishedClient() {
        upstreamUrlVectors.forEach { json ->
            val test = wireJson.parseToJsonElement(json).jsonObject
            assertEquals(test.string("expected"), canonicalShapeKey(test.string("input")!!))
        }
    }

}
