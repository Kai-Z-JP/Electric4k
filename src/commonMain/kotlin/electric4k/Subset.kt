package electric4k

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*

@Serializable
sealed class SerializedExpression {
    @Serializable @SerialName("ref") data class Ref(val column: String) : SerializedExpression()
    @Serializable @SerialName("val") data class Value(val paramIndex: Int) : SerializedExpression()
    @Serializable @SerialName("func") data class Function(val name: String, val args: List<SerializedExpression>) : SerializedExpression()
}
@Serializable
data class SerializedOrderByClause(val column: String, val direction: String? = null, val nulls: String? = null) {
    init { require(direction == null || direction in setOf("asc", "desc")); require(nulls == null || nulls in setOf("first", "last")) }
}
fun compileExpression(expression: SerializedExpression, mapper: ColumnMapper? = null): String = when (expression) {
    is SerializedExpression.Ref -> quoteIdentifier(mapper?.encode(expression.column) ?: expression.column)
    is SerializedExpression.Value -> "$${expression.paramIndex}"
    is SerializedExpression.Function -> {
        val args = expression.args.map { compileExpression(it, mapper) }
        fun binary(operator: String): String { require(args.size == 2); return "${args[0]} $operator ${args[1]}" }
        fun unary(format: (String) -> String): String { require(args.size == 1); return format(args[0]) }
        when (expression.name) {
            "eq" -> binary("="); "gt" -> binary(">"); "gte" -> binary(">="); "lt" -> binary("<"); "lte" -> binary("<=")
            "and" -> args.joinToString(" AND ") { "($it)" }; "or" -> args.joinToString(" OR ") { "($it)" }
            "not" -> unary { "NOT ($it)" }
            "in" -> { require(args.size == 2); "${args[0]} = ANY(${args[1]})" }
            "like" -> binary("LIKE"); "ilike" -> binary("ILIKE")
            "isNull", "isUndefined" -> unary { "$it IS NULL" }
            "upper", "lower", "length" -> unary { "${expression.name.uppercase()}($it)" }
            "concat", "coalesce" -> "${expression.name.uppercase()}(${args.joinToString(", ")})"
            else -> throw IllegalArgumentException("Unknown function: ${expression.name}")
        }
    }
}
fun compileOrderBy(clauses: List<SerializedOrderByClause>, mapper: ColumnMapper? = null): String = clauses.joinToString(", ") {
    quoteIdentifier(mapper?.encode(it.column) ?: it.column) + (if (it.direction == "desc") " DESC" else "") + (it.nulls?.let { n -> " NULLS ${n.uppercase()}" } ?: "")
}
@Serializable
data class SubsetParams(
    val where: String? = null,
    val params: Map<String, JsonPrimitive>? = null,
    val limit: Int? = null,
    val offset: Int? = null,
    val orderBy: String? = null,
    val whereExpr: SerializedExpression? = null,
    val orderByExpr: List<SerializedOrderByClause>? = null,
    val method: String? = null,
) {
    init { require(method == null || method in setOf("GET", "POST")); require(limit == null || limit >= 0); require(offset == null || offset >= 0) }
    internal fun body(mapper: ColumnMapper?): JsonObject = buildJsonObject {
        if (whereExpr != null) { put("where", compileExpression(whereExpr, mapper)); put("where_expr", wireJson.encodeToJsonElement(SerializedExpression.serializer(), whereExpr)) }
        else where?.let { put("where", encodeWhereClause(it, mapper)) }
        params?.let { put("params", JsonObject(it)) }; limit?.let { put("limit", it) }; offset?.let { put("offset", it) }
        if (orderByExpr != null) {
            put("order_by", compileOrderBy(orderByExpr, mapper))
            put("order_by_expr", JsonArray(orderByExpr.map { wireJson.encodeToJsonElement(SerializedOrderByClause.serializer(), it) }))
        } else orderBy?.let { put("order_by", encodeWhereClause(it, mapper)) }
    }
}
@Serializable
data class SnapshotMetadata(
    val xmin: String, val xmax: String, val xip_list: List<String>,
    val snapshot_mark: Long, val database_lsn: String,
)
data class SubsetSnapshot(val metadata: SnapshotMetadata, val data: JsonArray, val responseOffset: String?, val responseHandle: String?, val schema: ShapeSchema)
fun isVisibleInSnapshot(xid: ULong, metadata: SnapshotMetadata): Boolean = xid < metadata.xmin.toULong() || xid < metadata.xmax.toULong() && metadata.xip_list.none { it.toULong() == xid }

internal class SnapshotTracker {
    private data class Tracked(val metadata: SnapshotMetadata, val keys: Set<String>)
    private val snapshots = mutableMapOf<Long, Tracked>()
    fun add(metadata: SnapshotMetadata, keys: Set<String>) { snapshots[metadata.snapshot_mark] = Tracked(metadata, keys.toSet()) }
    fun clear() { snapshots.clear() }
    fun lastSeen(lsn: ULong) { snapshots.entries.removeAll { it.value.metadata.database_lsn.toULong() <= lsn } }
    private data class Xid(val overflow: Int, val value: ULong) : Comparable<Xid> {
        override fun compareTo(other: Xid): Int = if (overflow != other.overflow) overflow.compareTo(other.overflow) else value.compareTo(other.value)
    }
    private fun latest(txids: List<ULong>, reference: ULong): Xid = txids.maxOf { xid ->
        val delta = (xid - reference).toUInt().toInt().toLong()
        if (delta >= 0) {
            val value = reference + delta.toULong()
            Xid(if (value < reference) 1 else 0, value)
        } else {
            val amount = (-delta).toULong()
            Xid(if (reference < amount) -1 else 0, reference - amount)
        }
    }
    fun reject(message: JsonObject): Boolean {
        val txids = message["headers"]?.jsonObject?.get("txids")?.jsonArray?.map { it.jsonPrimitive.content.toULong() }.orEmpty()
        if (txids.isEmpty()) return false
        snapshots.entries.removeAll { latest(txids, it.value.metadata.xmax.toULong()) >= Xid(0, it.value.metadata.xmax.toULong()) }
        val key = message["key"]?.jsonPrimitive?.content ?: return false
        return snapshots.values.any {
            val xid = latest(txids, it.metadata.xmax.toULong())
            key in it.keys && (xid.overflow < 0 || xid.overflow == 0 && isVisibleInSnapshot(xid.value, it.metadata))
        }
    }
}
