package com.magicreg.resql

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.net.URI
import java.nio.charset.Charset
import java.sql.*
import kotlin.reflect.KClass

class Database(originalUrl: String, originalPrefix: String? = null): Namespace {
    override val uri = extractSecureUrl(originalUrl)
    override var prefix = originalPrefix ?: (URI(uri).path.split("/").firstOrNull{it.isNotBlank()} ?: "")
    override val readOnly: Boolean = false
    private val catalog = URI(uri).path.split("/")[1]
    private val connection = DriverManager.getConnection(originalUrl)
    private enum class ResultMode { ANY, SINGLE, MULTIPLE }

    override val names: List<String> get() {
        val rs = connection.metaData.getTables(catalog, null, null, arrayOf("TABLE"))
        val tables = mutableListOf<String>()
        while (rs.next())
            tables.add(rs.getString("TABLE_NAME").lowercase())
        rs.close()
        return tables
    }

    override fun hasName(name: String): Boolean {
        return names.contains(name)
    }

    override fun value(name: String): Any? {
        return if (name.isBlank()) this else getTable(name)
    }

    override fun setValue(name: String, value: Any?): Boolean {
        if (readOnly || name.isBlank())
            return false
        try {
            createTable(name, toMap(value) as Map<String,Any?>)
            return true
        } catch (e: Exception) {}
        return false
    }

    override fun apply(method: UriMethod, path: List<String>, query: Query, value: Any?): Response {
        return when(val state = method.name.lowercase()+path.size) {
            "get0" -> if (query.filter.isEmpty) Response(names) else selectRows(query, value?.toString())
            "get1" -> {
                if (getTable(path[0]) == null)
                    Response(StatusCode.NotFound)
                else
                    selectRows(query.addQueryPart(".source", path[0]), value?.toString(), ResultMode.MULTIPLE)
            }
            "get2" -> {
                val pkeys = getPrimaryKeys(path[0])
                when (pkeys.size) {
                    0 -> return Response(StatusCode.NotFound)
                    1 -> query.addQueryPart(pkeys[0], path[1])
                    else -> {
                        val ids = path[1].split(",")
                        if (ids.size != pkeys.size)
                            return Response(StatusCode.BadRequest)
                        for ((index, value) in pkeys.withIndex())
                            query.addQueryPart(pkeys[index], ids[index])
                    }
                }
                selectRows(query.addQueryPart(".source", path[0]), value?.toString(), ResultMode.SINGLE)
            }
            "post0" -> selectRows(query.addQueryPart(".query", value))
            "post1" -> insertRows(path[0], formatRows(value))
            "post2" -> updateRows(path[0], mergeRows(formatRows(value)), query.filter) // TODO: check for id conflict
            "put0" -> selectRows(query.addQueryPart(".query", value))
            "put1" -> Response(if (createTable(path[0], mergeRows(formatRows(value)))) StatusCode.OK else StatusCode.BadRequest)
            "put2" -> updateRows(path[0], mergeRows(formatRows(value)), query.addQueryPart(getPrimaryKeys(path[0])[0], path[1]).filter) // TODO: merge with existing row with same id
            "delete0" -> deleteRows(query)
            "delete1" -> deleteRows(query.addQueryPart(".source", path[0]))
            "delete2" -> deleteRows(query.addQueryPart(".source", path[0]).addQueryPart(getPrimaryKeys(path[0])[0], path[1]))
            else -> Response(StatusCode.NotFound)
        }
    }

    override fun toString(): String {
        return uri
    }

    fun getTable(table: String): Map<String,KClass<*>>? {
        if (names.indexOf(table) < 0)
            return null
        val columns = mutableMapOf<String,KClass<*>>()
        val rs = connection.metaData.getColumns(catalog,null, table,null)
        while (rs.next()) {
            val name = rs.getString("COLUMN_NAME").lowercase()
            columns[name] = getResqlType(rs.getObject("DATA_TYPE"))
        }
        rs.close();
        return columns;
    }

    fun createTable(table: String, data: Map<String,Any?>): Boolean {
        if (hasName(table))
            return false
        val columns = mutableListOf<String>()
        for (key in data.keys)
            columns.add(key+" "+getSqlType(data[key].toString()))
        val sql = "create table "+table+" (\n  "+columns.joinToString(",\n  ")+"\n)\n"
        return execute(sql)
    }

    fun getPrimaryKeys(table: String): List<String> {
        val pkeys = mutableListOf<String>()
        val rs = connection.metaData.getPrimaryKeys(catalog, null, table)
        while (rs.next())
            pkeys.add(rs.getString("COLUMN_NAME").lowercase())
        if (pkeys.isEmpty()) {
            val columns = getTable(table)!!
            if (columns["id"] != null)
                pkeys.add("id")
            else if (columns["code"] != null)
                pkeys.add("code")
            else if (columns.isNotEmpty())
                pkeys.iterator().next()
        }
        return pkeys
    }

    fun query(sql: String): List<Map<String,Any?>> {
        val statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        val rs = statement.executeQuery(sql)
        val rows = mutableListOf<Map<String,Any?>>()
        while (rs.next())
            rows.add(resultToMap(rs))
        statement.close()
        return rows;
    }

    fun update(sql: String): Int {
        val statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        val result = statement.executeUpdate(sql)
        statement.close()
        return result
    }

    fun execute(sql: String): Boolean {
        val statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        val result = statement.execute(sql)
        statement.close()
        return result
    }

    private fun selectRows(query: Query, format: String? = null, mode: ResultMode = ResultMode.ANY): Response {
        val res = validateQuerySource(query.source)
        if (res.status != StatusCode.OK)
            return res
        val table = res.type!!
        val clauses = buildSqlClauses(query.filter)
        val keys = if (query.keys.isEmpty()) "*" else query.keys.joinToString(", ")
        val limit = if (query.page.size <= 0) "" else " limit ${query.page.size} offset ${query.page.range.first}"
        // TODO: missing group->group_by and sort->order_by
        val result = query("select $keys from $table$clauses$limit")
        if (mode == ResultMode.MULTIPLE) return createResponse(table, result, format)
        return when (result.size) {
            0 -> Response(StatusCode.NotFound)
            1 -> Response(result[0])
            else -> if (mode == ResultMode.SINGLE) Response(result) else createResponse(table, result, format)
        }
    }

    private fun insertRows(table: String, rows: List<Map<String,Any?>>): Response {
        val columns = getTable(table) ?: return Response("Invalid table: $table", null, defaultCharset(), StatusCode.NotFound)
        var count = 0
        for (row in rows) {
            val keys = mutableListOf<String>()
            val values = mutableListOf<String>()
            for (key in row.keys) {
                val type = columns[key]
                if (type != null) {
                    keys.add(key)
                    values.add(printSqlValue(row[key], type))
                }
            }
            val sql = "insert into "+table+" ("+keys.joinToString(",")+") values ("+values.joinToString(",")+")"
            count += update(sql)
        }
        return Response(if (count > 0) StatusCode.OK else StatusCode.NotFound)
        // TODO: replace OK with links to inserted rows
    }

    private fun updateRows(table: String, data: Map<String,Any?>, filter: Filter): Response {
        val columns = getTable(table) ?: return Response("Invalid table: $table", null, defaultCharset(), StatusCode.NotFound)
        val query = buildSqlClauses(filter)
        var pkeys = getPrimaryKeys(table)
        var count = 0
        val values = mutableListOf<String>()
        for (key in data.keys) {
            if (pkeys.indexOf(key) >= 0)
                continue
            val type = columns[key]
            if (type != null)
                values.add(key+" = "+printSqlValue(data[key], type))
        }
        val clauses = if (query.isEmpty()) getUpdateClause(data, pkeys) else query
        // TODO: check if we need to create a new record or merge with an existing one
        val sql = "update "+table+" set "+values.joinToString(", ")+clauses
        count += update(sql)
        return Response(if (count > 0) StatusCode.OK else StatusCode.NotFound)
        // TODO: replace OK with links to updated rows
    }

    private fun deleteRows(query: Query): Response {
        val res = validateQuerySource(query.source)
        if (res.status != StatusCode.OK)
            return res
        val table = res.type
        val clauses = buildSqlClauses(query.filter)
        val count = update("delete from $table$clauses")
        return Response(if (count > 0) StatusCode.NoContent else StatusCode.NotFound)
    }

    private fun createResponse(table: String, rows: List<Map<String,Any?>>, format: String?): Response {
        return if (format == "text/html") {
            val links = mutableListOf<String>()
            val pkeys = getPrimaryKeys(table)
            for (row in rows) {
                val ids = mutableListOf<String>()
                for (pkey in pkeys)
                    ids.add(row[pkey].toString())
                val uri = "/$prefix/$table/${ids.joinToString(",")}"
                links.add("<a href='$uri'>${row.label()}</a>")
            }
            return Response(ByteArrayInputStream(links.joinToString("<br>\n").toByteArray(Charset.forName(defaultCharset()))), format)
        }
        else if (format != null) {
            val output = ByteArrayOutputStream()
            getFormat(format)!!.encode(rows, output, defaultCharset())
            Response(ByteArrayInputStream(output.toByteArray()), format)
        }
        else
            Response(rows)
    }

    private fun validateQuerySource(source: Collection<String>): Response {
        if (source.isEmpty())
            return Response("No source specified in query", null, defaultCharset(), StatusCode.BadRequest)
        if (source.size > 1)
            return Response("Multiple sources in query not supported yet", null, defaultCharset(), StatusCode.BadRequest)
        val table = source.iterator().next()
        if (!hasName(table))
            return Response("Invalid table: $table", null, defaultCharset(), StatusCode.BadRequest)
        return Response(null, table)
    }

    private fun extractSecureUrl(originalUrl: String): String {
        if (!originalUrl.startsWith("jdbc:"))
            throw RuntimeException("Invalid database url $originalUrl")
        val trimurl = originalUrl.substring(5)
        val uri = URI(trimurl)
        if (uri.userInfo == null)
            return trimurl.split("?")[0]
        val rawUserInfo = uri.rawUserInfo
        val index = trimurl.indexOf(rawUserInfo)
        return (trimurl.substring(0, index) + trimurl.substring(index+rawUserInfo.length+1)).split("?")[0]
    }

    private fun formatRows(value: Any?): List<Map<String,Any?>> {
        return if (value is Map<*,*>)
            listOf(value as Map<String,Any?>)
        else if (value is Collection<*> || value is Array<*>)
            value.toCollection().map{x -> toMap(x).mapKeys{k -> k.toString()}}
        else if (value == null)
            emptyList()
        else if (value is CharSequence && value.isBlank())
            emptyList()
        else
            listOf(toMap(value).mapKeys{it.toString()})
    }

    private fun mergeRows(rows: List<Map<String,Any?>>): Map<String,Any?> {
        if (rows.isEmpty())
            return emptyMap()
        if (rows.size == 1)
            return rows[0]
        val map = mutableMapOf<String,Any?>()
        for (row in rows)
            map.putAll(row)
        return map
    }

    private fun resultToMap(rs: ResultSet): Map<String,Any?> {
        val map = mutableMapOf<String,Any?>()
        val meta = rs.metaData
        val nc = meta.columnCount
        for (c in 1 .. nc) {
            val name = meta.getColumnName(c)
            val type = meta.getColumnType(c)
            map[name.lowercase()] = rs.getObject(c)
        }
        return map
    }

    private fun getSqlType(def: String): String {
        return when (def) {
            "int", "date", "time", "timestamp" -> def
            "integer" -> "int"
            "number" -> "double"
            "boolean" -> "bit"
            "string" -> "varchar(255)"
            else -> "text"
        }
    }

    private fun getResqlType(sqlType: Any?): KClass<*> {
        if (sqlType == null)
            return Any::class
        return when (sqlType.toString().toInt()) {
            Types.BIGINT -> Long::class
            Types.INTEGER -> Int::class
            Types.SMALLINT -> Short::class
            Types.TINYINT -> Byte::class

            Types.DECIMAL,
            Types.DOUBLE,
            Types.FLOAT,
            Types.NUMERIC,
            Types.REAL -> Double::class

            Types.BIT,
            Types.BOOLEAN -> Boolean::class

            Types.BINARY,
            Types.BLOB,
            Types.VARBINARY,
            Types.LONGVARBINARY -> ByteArray::class

            Types.CHAR,
            Types.CLOB,
            Types.NCLOB,
            Types.NCHAR,
            Types.LONGVARCHAR,
            Types.VARCHAR -> String::class

            Types.DATE -> Date::class

            Types.TIMESTAMP,
            Types.TIMESTAMP_WITH_TIMEZONE -> Timestamp::class

            Types.TIME,
            Types.TIME_WITH_TIMEZONE -> Time::class

            else -> Any::class
        }
    }

    private fun getUpdateClause(item: Map<String,Any?>, keys: List<String>): String {
        val clauses = mutableListOf<String>()
        for (key in keys) {
            val value = item[key]
            clauses.add(buildSqlExpression(key, value))
        }
        return if (clauses.isEmpty()) "" else " where "+clauses.joinToString(" and ")
    }

    private fun buildSqlClauses(filter: Filter): String {
        val clauses = mutableListOf<String>()
        val iterator = filter.iterator()
        while (iterator.hasNext()) {
            val item = iterator.next()
            val clause = mutableListOf<String>()
            for (key in item.keys) {
                val value = item[key]
                clause.add(buildSqlExpression(key, value))
            }
            if (clause.isNotEmpty())
                clauses.add(clause.joinToString(" and "))
        }
        return when (clauses.size) {
            0 -> ""
            1 -> " where "+clauses[0]
            else -> " where ("+clauses.joinToString(") or (")+")"
        }
    }

    private fun buildSqlExpression(key: String, value: Any?): String {
        if (value == null)
            return "$key is null"
        if (value is List<*>) {
            val values = mutableListOf<String>()
            for (item in value)
                values.add(printSqlValue(value))
            return key+(if (values.isEmpty()) " is not null" else " in ("+values.joinToString(", ")+")")
        }
        if (value is Array<*>)
            return buildSqlExpression(key, value.toList())
        if (value is Map<*,*>) {
            if (value.size != 1)
                throw RuntimeException("Map value in clause must have only one single operator key")
            val op = value.keys.iterator().next().toString()
            return when (op.toCompareOperator(true)!!) {
                CompareOperator.MATCH -> "$key like ${printSqlValue(value[op])}"
                CompareOperator.NOT_MATCH -> "not($key like ${printSqlValue(value[op])})"
                CompareOperator.BETWEEN -> "($key between ${printSqlRange(value[op])})"
                CompareOperator.NOT_BETWEEN -> "not($key between ${printSqlRange(value[op])})"
                CompareOperator.IN -> buildSqlExpression(key, toList(value[op]))
                CompareOperator.NOT_IN -> "not(${buildSqlExpression(key, toList(value[op]))})"
                else -> "$key $op ${printSqlValue(value[op])}"
            }
        }
        return "$key = ${printSqlValue(value)}"
    }

    private fun printSqlValue(value: Any?, type: KClass<*>? = null): String {
        if (value == null)
            return "null"
        if (type == null)
            return printSqlValue(value, value::class)
        if (Number::class.java.isAssignableFrom(type.java))
            return value.toString()
        if (Boolean::class.java.isAssignableFrom(type.java))
            return if (isTrue(value)) "1" else "0"
        if (java.util.Date::class.java.isAssignableFrom(type.java))
            return printSqlDate(value)
        return "'"+toString(value).split("'").joinToString("''")+"'"
    }

    private fun printSqlDate(value: Any?): String {
        if (value == null)
            return "null"
        val txt = toDateTime(value).toString().lowercase().replace('t', ' ').split(".")[0]
        return "'$txt'"
    }

    private fun printSqlRange(value: Any?): String {
        val list = if (value is List<*>) value else if (value is Array<*>) value.toList() else listOf(value)
        if (list.isEmpty())
            return "null and null"
        val first: Any? = list[0]
        val last: Any? = list[list.size-1]
        return printSqlValue(first)+" and "+printSqlValue(last)
    }

    private fun isTrue(value: Any?): Boolean {
        if (value == null)
            return false
        if (value is Number)
            return value != 0
        if (value is Array<*>)
            return value.size > 0
        if (value is List<*>)
            return value.size > 0
        if (value is Map<*,*>)
            return value.size > 0
        return value.toString().trim() != ""
    }
}
