package electric4k

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.serialization.json.*

sealed interface ShapeMessage<out T> { val headers: JsonObject }
data class ChangeMessage<T>(val key: String, val value: T, val oldValue: JsonObject?, override val headers: JsonObject) : ShapeMessage<T> {
    val operation: String get() = headers.getValue("operation").jsonPrimitive.content
    val transactionIds: List<UInt> get() = headers["txids"]?.jsonArray?.map { it.jsonPrimitive.content.toUInt() }.orEmpty()
    val tags: List<String> get() = headers["tags"]?.jsonArray?.map { it.jsonPrimitive.content }.orEmpty()
    val removedTags: List<String> get() = headers["removed_tags"]?.jsonArray?.map { it.jsonPrimitive.content }.orEmpty()
}
data class ControlMessage(override val headers: JsonObject) : ShapeMessage<Nothing> {
    val control: String get() = headers.getValue("control").jsonPrimitive.content
}
data class MovePattern(val pos: Int, val value: String)
data class EventMessage(override val headers: JsonObject) : ShapeMessage<Nothing> {
    val event: String get() = headers.getValue("event").jsonPrimitive.content
    val patterns: List<MovePattern> get() = headers["patterns"]?.jsonArray?.map { MovePattern(it.jsonObject.getValue("pos").jsonPrimitive.int, it.jsonObject.getValue("value").jsonPrimitive.content) }.orEmpty()
}
fun <T> ShapeStream.typedMessages(decode: (JsonObject) -> T): Flow<List<ShapeMessage<T>>> = messages().map { batch ->
    batch.map { value ->
        val message = value.jsonObject
        val headers = message.getValue("headers").jsonObject
        when {
            "operation" in headers -> ChangeMessage(message.getValue("key").jsonPrimitive.content, decode(message["value"]?.jsonObject ?: JsonObject(emptyMap())), message["old_value"]?.jsonObject, headers)
            "control" in headers -> ControlMessage(headers)
            "event" in headers -> EventMessage(headers)
            else -> throw ShapeProtocolException("Unknown message kind")
        }
    }
}
