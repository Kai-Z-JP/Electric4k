package electric4k

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.json.*

fun interface RowDecoder<T> { fun decode(row: JsonObject, schema: ShapeSchema): T }
fun <T> serializedRows(serializer: DeserializationStrategy<T>, json: Json = wireJson, types: PostgresTypes = PostgresTypes()): RowDecoder<T> =
    RowDecoder { row, schema -> json.decodeFromJsonElement(serializer, types.convert(row, schema)) }

/** Preserves numeric/decimal, timestamps, UUID and unknown types as strings, without precision loss. */
class PostgresTypes(private val custom: Map<String, (String) -> JsonElement> = emptyMap(), private val parsers: Map<String, (String, ColumnInfo) -> JsonElement> = emptyMap()) {
    fun convert(row: JsonObject, schema: ShapeSchema): JsonObject = JsonObject(row.mapValues { (name, value) ->
        val info = schema[name]
        if (value == JsonNull && info?.notNull == true) throw ShapeProtocolException("NULL in NOT NULL column: $name")
        if (info == null || value == JsonNull || value !is JsonPrimitive || !value.isString) value
        else if (maxOf(info.dims, info.dimensions) > 0) PgArray(value.content, info.notNull) { scalar(it, info) }.parse()
        else scalar(value.content, info)
    })
    private fun scalar(value: String, info: ColumnInfo): JsonElement {
        val type = info.type
        parsers[type]?.let { return it(value, info) }
        custom[type]?.let { return it(value) }
        return when (type) {
            "int2", "int4", "int8" -> JsonPrimitive(value.toLong())
            "float4", "float8" -> JsonPrimitive(value.toDouble())
            "bool" -> JsonPrimitive(when (value) { "t", "true" -> true; "f", "false" -> false; else -> throw ShapeProtocolException("Invalid PostgreSQL boolean") })
            "json", "jsonb" -> wireJson.parseToJsonElement(value)
            else -> JsonPrimitive(value)
        }
    }
}
private class PgArray(private val text: String, private val notNull: Boolean, private val scalar: (String) -> JsonElement) {
    private var index = 0
    fun parse(): JsonArray {
        if (text.startsWith('[')) { index = text.indexOf('=') + 1; require(index > 0) }
        val result = array()
        require(index == text.length) { "Trailing PostgreSQL array content" }
        return result
    }
    private fun array(): JsonArray {
        require(index < text.length && text[index++] == '{')
        val values = mutableListOf<JsonElement>()
        if (text.getOrNull(index) == '}') { index++; return JsonArray(values) }
        while (true) {
            while (text.getOrNull(index)?.isWhitespace() == true) index++
            if (text.getOrNull(index) == '{') values.add(array())
            else {
                val quoted = text.getOrNull(index) == '"'
                val token = StringBuilder()
                if (quoted) {
                    index++
                    while (true) {
                        require(index < text.length) { "Unterminated PostgreSQL array string" }
                        val c = text[index++]
                        if (c == '"') break
                        if (c == '\\') { require(index < text.length); token.append(text[index++]) } else token.append(c)
                    }
                } else {
                    while (index < text.length && text[index] !in charArrayOf(',', '}')) {
                        val c = text[index++]
                        if (c == '\\') { require(index < text.length); token.append(text[index++]) } else token.append(c)
                    }
                    require(token.isNotEmpty())
                }
                if (!quoted && token.toString() == "NULL" && notNull) throw ShapeProtocolException("NULL in NOT NULL array")
                values.add(if (!quoted && token.toString() == "NULL") JsonNull else scalar(token.toString()))
            }
            require(index < text.length)
            when (text[index++]) { '}' -> return JsonArray(values); ',' -> Unit; else -> error("Malformed PostgreSQL array") }
        }
    }
}

/** Parses both value and old_value, then maps column names and finally transforms values. */
class MessageParser(
    private val types: PostgresTypes = PostgresTypes(),
    private val mapper: ColumnMapper? = null,
    private val transformer: ((JsonObject) -> JsonObject)? = null,
) {
    fun row(value: JsonObject, schema: ShapeSchema): JsonObject {
        val parsed = types.convert(value, schema)
        val renamed = if (mapper == null) parsed else JsonObject(parsed.entries.associate { mapper.decode(it.key) to it.value })
        return transformer?.invoke(renamed) ?: renamed
    }
    fun parse(messages: JsonArray, schema: ShapeSchema): JsonArray = JsonArray(messages.map { message ->
        JsonObject(message.jsonObject.mapValues { (key, value) ->
            if ((key == "value" || key == "old_value") && value is JsonObject) row(value, schema) else value
        })
    })
}
