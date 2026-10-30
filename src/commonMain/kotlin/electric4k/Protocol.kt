package electric4k

import kotlinx.serialization.Serializable
import kotlinx.serialization.SerialName
import kotlinx.serialization.json.*

internal val wireJson = Json { ignoreUnknownKeys = true; allowSpecialFloatingPointValues = true }

@Serializable
data class ShapeCursor(val offset: String, val handle: String, val cacheBuster: String? = null) {
    init { require(offset.isNotBlank() && handle.isNotBlank()) }
}
@Serializable
data class ColumnInfo(
    val type: String, val dims: Int = 0, val dimensions: Int = 0,
    @SerialName("not_null") val notNull: Boolean = false,
    val precision: Int? = null, val scale: Int? = null,
    @SerialName("max_length") val maxLength: Int? = null,
    val length: Int? = null, val fields: String? = null,
)
typealias ShapeSchema = Map<String, ColumnInfo>

data class ShapeRequest(
    val table: String? = null,
    val where: String? = null,
    val columns: List<String> = emptyList(),
    val params: Map<String, String> = emptyMap(),
    val replica: String = "default",
    val log: LogMode = LogMode.Full,
) {
    init { require(table == null || table.isNotBlank()); require(replica in setOf("default", "full")) }
}
data class ShapeBatch(
    val messages: JsonArray,
    val cursor: ShapeCursor,
    val upToDate: Boolean,
    val schema: ShapeSchema = emptyMap(),
    val httpStatus: Int = 200,
    val requestUrl: String? = null,
)
class ShapeInvalidatedException(val replacementHandle: String? = null, val requestUrl: String? = null) : Exception("Shape invalidated")
class ShapeHttpException(val status: Int, val retryAfterMillis: Long? = null, val responseHeaders: Map<String, String> = emptyMap(), val responseBody: String? = null) : Exception("Electric returned HTTP $status")
class ShapeProtocolException(message: String) : Exception(message)

internal fun JsonElement.control(): String? = jsonObject["headers"]?.jsonObject?.get("control")?.jsonPrimitive?.content
internal fun JsonArray.isUpToDate(): Boolean = lastOrNull()?.control() == "up-to-date"
internal fun JsonArray.checkInvalidation(handle: String?, requestUrl: String? = null) {
    if (any { it.control() == "must-refetch" }) throw ShapeInvalidatedException(handle, requestUrl)
}

enum class LogMode(val wireValue: String) { Full("full"), ChangesOnly("changes_only") }
val ELECTRIC_PROTOCOL_QUERY_PARAMS: Set<String> = setOf("live", "live_sse", "experimental_live_sse", "handle", "offset", "cursor", "expired_handle", "log", "subset__where", "subset__limit", "subset__offset", "subset__order_by", "subset__params", "subset__where_expr", "subset__order_by_expr", "cache-buster")
fun isChangeMessage(message: JsonElement): Boolean = message.jsonObject["headers"]?.jsonObject?.containsKey("operation") == true
fun isControlMessage(message: JsonElement): Boolean = message.jsonObject["headers"]?.jsonObject?.containsKey("control") == true
fun isEventMessage(message: JsonElement): Boolean = message.jsonObject["headers"]?.jsonObject?.containsKey("event") == true
