package com.magicreg.resql

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.net.URI
import java.nio.charset.Charset
import java.sql.*
import kotlin.reflect.KClass

class Database(private val originalUrl: String, originalPrefix: String? = null): Namespace {
    override val uri = extractSecureUrl(originalUrl)
    private val catalog = extractCatalogName()
    override val prefix = originalPrefix ?: catalog
    override val readOnly: Boolean = false
    private var connection: Connection? = null
    private enum class ResultMode { ANY, SINGLE, MULTIPLE }

    override val names: List<String> get() {
        val rs = checkConnection().metaData.getTables(catalog, null, null, arrayOf("TABLE"))
        val tables = mutableListOf<String>()
        while (rs.next())
            tables.add(rs.getString("TABLE_NAME").lowercase())
        rs.close()
        return tables
    }

    override fun hasName(name: String): Boolean {
        return names.contains(name)
    }

    override fun getValue(name: String): Any? {
        return if (name.isBlank()) this else tableColumns(name)
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

    override fun execute(method: RestMethod, path: List<String>, query: Query, value: Any?): Response {
        return when(val state = method.name.lowercase()+path.size) {
            "get0" -> if (query.filter.isEmpty) Response(names) else selectRows(query, value?.toString())
            "get1" -> {
                if (tableColumns(path[0]) == null)
                    Response(StatusCode.NotFound)
                else
                    selectRows(query.addQueryPart(".source", path[0]), value?.toString(), ResultMode.MULTIPLE)
            }
            "get2" -> {
                val pkeys = primaryKeys(path[0])
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
            "put2" -> updateRows(path[0], mergeRows(formatRows(value)), query.addQueryPart(primaryKeys(path[0])[0], path[1]).filter) // TODO: merge with existing row with same id
            "delete0" -> deleteRows(query)
            "delete1" -> deleteRows(query.addQueryPart(".source", path[0]))
            "delete2" -> deleteRows(query.addQueryPart(".source", path[0]).addQueryPart(primaryKeys(path[0])[0], path[1]))
            else -> Response(StatusCode.NotFound)
        }
    }

    override fun toString(): String {
        return uri
    }

    fun copyWithPrefix(prefix: String): Database {
        return Database(originalUrl, prefix)
    }

    fun primaryKeys(table: String): List<String> {
        val pkeys = mutableListOf<String>()
        val rs = checkConnection().metaData.getPrimaryKeys(catalog, null, table)
        while (rs.next())
            pkeys.add(rs.getString("COLUMN_NAME").lowercase())
        if (pkeys.isEmpty()) {
            val pkey = tableColumns(table)!!.primaryKey(false)
            if (pkey.isNotBlank())
                pkeys.add(pkey)
        }
        return pkeys
    }

    fun query(sql: String): List<Map<String,Any?>> {
        val statement = checkConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        val rs = statement.executeQuery(sql)
        val rows = mutableListOf<Map<String,Any?>>()
        while (rs.next())
            rows.add(resultToMap(rs))
        statement.close()
        return rows;
    }

    fun update(sql: String): Int {
        val statement = checkConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        val result = statement.executeUpdate(sql)
        statement.close()
        return result
    }

    fun execute(sql: String): Boolean {
        try {
            val statement = checkConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.execute(sql)
            statement.close()
            return true
        }
        catch (e: Exception) { return false }
    }

    private fun checkConnection(): Connection {
        try {
            if (connection!!.isValid(100))
                return connection!!
        }
        catch (e: Exception) {}
        connection = DriverManager.getConnection(originalUrl)
        return connection!!
    }

    private fun tableColumns(table: String): Map<String,KClass<*>>? {
        if (names.indexOf(table) < 0)
            return null
        val columns = mutableMapOf<String,KClass<*>>()
        val rs = checkConnection().metaData.getColumns(catalog,null, table,null)
        while (rs.next()) {
            val name = rs.getString("COLUMN_NAME").lowercase()
            columns[name] = getKotlinType(rs.getObject("DATA_TYPE"))
        }
        rs.close();
        return columns;
    }

    private fun createTable(table: String, data: Map<String,Any?>): Boolean {
        if (hasName(table))
            return false
        val columns = mutableListOf<String>()
        var foundPrimary = false
        for (key in data.keys) {
            val def = getSqlDefinition(data[key])
            columns.add("$key $def")
            if (def.lowercase().contains("primary"))
                foundPrimary = true
        }
        if (!foundPrimary) {
            val primary = data.primaryKey(false)
            if (primary.isNotBlank())
                columns.add("primary key ($primary)")
        }
        val sql = "create table "+table+" (\n  "+columns.joinToString(",\n  ")+"\n)\n"
        return execute(sql)
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
            1 -> if (mode == ResultMode.MULTIPLE) Response(result) else Response(result[0])
            else -> if (mode == ResultMode.SINGLE) Response(result) else createResponse(table, result, format)
        }
    }

    private fun insertRows(table: String, rows: List<Map<String,Any?>>): Response {
        val columns = tableColumns(table) ?: return Response("Invalid table: $table", null, defaultCharset(), StatusCode.NotFound)
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
        val columns = tableColumns(table) ?: return Response("Invalid table: $table", null, defaultCharset(), StatusCode.NotFound)
        val query = buildSqlClauses(filter)
        var pkeys = primaryKeys(table)
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
            val style = getContext().configuration().style
            if (!style.isNullOrBlank())
                links.add("<link href='$style' rel='stylesheet'>")
            val pkeys = primaryKeys(table)
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

    private fun extractCatalogName(): String {
        val parts = uri.split("#")[0].split("?")[0].split("/")
        return if (parts.isEmpty()) "" else parts[parts.size-1]
    }

    private fun formatRows(value: Any?): List<Map<String,Any?>> {
        return if (value is Map<*,*>)
            listOf(value as Map<String,Any?>)
        else if (value is Collection<*> || value is Array<*>)
            value.toCollection().map{toMap(it).mapKeys{e->e.key.toString()}}
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

    private fun getSqlDefinition(value: Any?): String {
        var type: Any? = null
        var size: Int? = null
        var def = mutableListOf<String>()
        val iterator = if (value.isIterable())
            value.toIterator()
        else if (value.isMappable())
            value.toMap()?.values?.iterator()
        else
            null
        if (iterator == null)
            type = value
        else {
            while (iterator.hasNext()) {
                val item = iterator.next()
                if (item.isEmpty())
                    continue
                else if (item is Number)
                    size = item.toInt()
                else if (item.isIterable())
                    def.addAll(item.toCollection().map{it.toText()})
                else if (item.isMappable())
                    def.addAll(item.toMap()!!.values.map{it.toText()})
                else if (type == null)
                    type = item
                else if (item is KClass<*> || item is Class<*>) {
                    if (type is String)
                        def.add(type)
                    type = item
                }
                else
                    def.add(item.toText(true))
            }
        }
        if (type.isEmpty() && def.isNotEmpty())
            type = def.removeAt(0)
        val sqlType = getSqlType(type)
        return when (sqlType) {
            "int", "double", "date", "time", "timestamp" -> sqlType
            "integer", "long", "short", "byte" -> "int"
            "number", "float" -> "double"
            "boolean" -> "bit"
            "localdate" -> "date"
            "localdatetime" -> "timestamp"
            "localtime" -> "time"
            "duration" -> "double"
            "string" -> "varchar("+(size ?: 255)+")"
            else -> if (size == null) "text" else "varchar($size)"
        }+" "+def.joinToString(" ")
    }

    private fun getSqlType(value: Any?): String {
        return if (value is Number)
            getClassName(getKotlinType(value))
        else if (value is Type)
            getClassName(value.classes[0])
        else if (value is KClass<*>)
            getClassName(value)
        else if (value is Class<*>)
            getClassName(value.kotlin)
        else
            value.toText().lowercase()
    }

    private fun getKotlinType(sqlType: Any?): KClass<*> {
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

    private fun getClassName(klass: KClass<*>): String {
        if (klass.qualifiedName == "java.util.Date")
            return "timestamp"
        return klass.simpleName?.lowercase() ?: ""
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
