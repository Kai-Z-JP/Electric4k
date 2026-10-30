package electric4k

interface ColumnMapper {
    fun decode(databaseColumn: String): String
    fun encode(applicationColumn: String): String
}
fun quoteIdentifier(identifier: String): String = "\"${identifier.replace("\"", "\"\"")}\""
fun snakeToCamel(value: String): String {
    val leading = value.takeWhile { it == '_' }
    val remainder = value.drop(leading.length)
    val trailing = remainder.takeLastWhile { it == '_' }
    val core = remainder.dropLast(trailing.length).lowercase()
    return leading + Regex("_+([a-z])").replace(core) { "_".repeat(it.value.length - 2) + it.groupValues[1].uppercase() } + trailing
}
fun camelToSnake(value: String): String = Regex("([A-Z]+)([A-Z][a-z])").replace(
    Regex("([a-z_])([A-Z])").replace(value, "$1_$2"), "$1_$2",
).lowercase()
fun createColumnMapper(mapping: Map<String, String>): ColumnMapper = object : ColumnMapper {
    private val forward = mapping.toMap()
    private val reverse = mapping.entries.associate { it.value to it.key }
    override fun decode(databaseColumn: String) = forward[databaseColumn] ?: databaseColumn
    override fun encode(applicationColumn: String) = reverse[applicationColumn] ?: applicationColumn
}
fun snakeCamelMapper(schema: ShapeSchema? = null): ColumnMapper = schema?.let { createColumnMapper(it.keys.associateWith(::snakeToCamel)) } ?: object : ColumnMapper {
    override fun decode(databaseColumn: String) = snakeToCamel(databaseColumn)
    override fun encode(applicationColumn: String) = camelToSnake(applicationColumn)
}
private val sqlKeywords = "SELECT FROM WHERE AND OR NOT IN IS NULL NULLS FIRST LAST TRUE FALSE LIKE ILIKE BETWEEN ASC DESC LIMIT OFFSET ORDER BY GROUP HAVING DISTINCT AS ON JOIN LEFT RIGHT INNER OUTER CROSS CASE WHEN THEN ELSE END CAST LOWER UPPER COALESCE NULLIF".split(' ').toSet()
fun encodeWhereClause(sql: String, mapper: ColumnMapper?): String {
    if (mapper == null) return sql
    val result = StringBuilder()
    var i = 0
    while (i < sql.length) {
        val c = sql[i]
        if (c == '\'' || c == '"') {
            result.append(c); i++
            while (i < sql.length) {
                val char = sql[i++]; result.append(char)
                if (char == c) {
                    if (sql.getOrNull(i) == c) { result.append(c); i++ } else break
                }
            }
        } else if ((c in 'a'..'z' || c in 'A'..'Z' || c == '_') && (i == 0 || !sql[i - 1].isLetterOrDigit() && sql[i - 1] != '_')) {
            val start = i++
            while (i < sql.length && (sql[i].isLetterOrDigit() || sql[i] == '_')) i++
            val token = sql.substring(start, i)
            result.append(if (token.uppercase() in sqlKeywords) token else mapper.encode(token))
        } else { result.append(c); i++ }
    }
    return result.toString()
}
