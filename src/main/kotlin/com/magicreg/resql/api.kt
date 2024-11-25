package com.magicreg.resql

import io.ktor.util.*
import org.apache.commons.beanutils.BeanMap
import org.w3c.dom.Document
import java.io.*
import java.lang.reflect.Member
import java.net.URI
import java.net.URL
import java.net.URLDecoder
import java.net.URLEncoder
import java.nio.charset.Charset
import java.text.Normalizer
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.time.temporal.Temporal
import java.util.*
import kotlin.reflect.KCallable
import kotlin.reflect.KClass
import kotlin.reflect.full.superclasses
import kotlin.text.toCharArray

enum class Type(vararg val classes: KClass<*>): Function {
    ANY(Any::class),
    NUMBER(Number::class, Boolean::class),
    FUNCTION(Function::class),
    ENTITY(Map::class),
    COLLECTION(List::class, Array::class, Iterable::class, Iterator::class, Enumeration::class),
    TEXT(String::class),
    DOCUMENT(Document::class);
    // TODO: where does type, namespace, format, property, mapentry, expression fit into all this ?

    override val id = this.name.lowercase()
    override val symbol: String = ""
    override val lambda = { args: List<Any?> ->
        if (this == TEXT)
            args.joinToString("") { it.resolve(true).toText(false) }
        else {
            val value = args.simplify()
            if (isInstance(value))
                value
            else
                convert(value, classes[0])
        }
    }
    override fun toString(): String { return "$id()" }

    fun isInstance(value: Any?): Boolean {
        if (this == ANY)
            return true
        if (this == TEXT)
            return value.isText()
        for (c in classes) {
            if (c.isInstance(value))
                return true
        }
        return false
    }
}

enum class RestMethod(override val symbol: String, override val lambda: (List<Any?>) -> Any?): Function {
    GET("?=", ::get_func),
    POST("+=", ::post_func),
    PUT(":=", ::put_func),
    DELETE("-=", ::delete_func);

    override val id = this.name.lowercase()
    override fun toString(): String { return "$id()" }
}

enum class CompareOperator(override val symbol: String, override val lambda: (List<Any?>) -> Any?): Function {
    LESS("<", ::less_func), NOT_LESS(">=", ::not_less_func),
    EQUAL("=", ::equal_func), NOT_EQUAL("!=", ::not_equal_func),
    MORE(">", ::more_func), NOT_MORE("<=", ::not_more_func),
    IN("@", ::in_func), NOT_IN("!@", ::not_in_func),
    BETWEEN("><", ::between_func), NOT_BETWEEN("!><", ::not_between_func),
    MATCH("~", ::match_func), NOT_MATCH("!~", ::not_match_func);

    override val id = this.name.lowercase()
    override fun toString(): String { return "$id()" }
}

enum class LogicOperator(override val symbol: String, override val lambda: (List<Any?>) -> Any?): Function {
    OR("|", ::or_func),
    AND("&", ::and_func);

    override val id = this.name.lowercase()
    override fun toString(): String { return "$id()" }
}

enum class MathOperator(override val symbol: String, override val lambda: (List<Any?>) -> Any?): Function {
    ADD("+", ::add_func),
    REMOVE("-", ::remove_func),
    MULTIPLY("*", ::multiply_func),
    DIVIDE("/", ::divide_func),
    MODULO("%", ::modulo_func),
    EXPONENT("^", ::exponent_func),
    ROOT("^/", ::root_func),
    LOGARITHM("^%", ::logarithm_func);

    override val id = this.name.lowercase()
    override fun toString(): String { return "$id()" }
}

enum class MiscOperator(override val symbol: String, override val lambda: (List<Any?>) -> Any?): Function {
    JOIN(",", ::join_func),
    DO(";", ::do_func),
    HAVE("#", ::have_func),
    OF("@#", ::of_func),
    WITH("?", ::with_func),
    SORT(">>", ::sort_func),
    EACH("?*", ::each_func),
    SCHEMA("", ::schema_func),
    PRINT("", ::print_func);

    override val id = this.name.lowercase()
    override fun toString(): String { return "$id()" }
}

data class Configuration(
    val host: String = "localhost",
    val port: Int = 0,
    val routes: Map<String,Namespace> = emptyMap(),
    val maxPageSize: Int = 100,
    val scripting: Boolean = false,
    var home: String? = null,
    var style: String? = null
)

enum class ErrorMode {
    SILENT, PRINT, RESPONSE, THROW;

    fun wrap(value: Any?): Any? {
        val code = if (value is Response) value.status else if (value is Throwable) StatusCode.InternalServerError else StatusCode.OK
        return wrap(code, value)
    }

    fun wrap(code: StatusCode, value: Any?): Any? {
        when (this) {
            SILENT -> {}
            PRINT -> {
                if (code.code >= 400) {
                    if (value is Response)
                        println("${value.status.code} ${code.message}")
                    else
                        println("${code.code} ${code.message}")
                }
            }
            RESPONSE -> return if (value is Response) value else Response(value, null,  defaultCharset(), code)
            THROW -> {
                if (code.code >= 400) {
                    val message = value ?: code.message
                    throw RuntimeException("$code $message")
                }
            }
        }
        return if (value is Response) value.renderValue() else value
    }
}

data class Expression(
    val function: Function? = null,
    val parameters: List<Any?> = emptyList()
) {
    fun execute(): Any? {
        return if (function == null)
            parameters.simplify(true)
        else if (parameters.isEmpty())
            function
        else
            function.execute(parameters)
    }

    override fun toString(): String {
        val name = function?.id ?: ""
        val space = if (function == null) "" else " "
        return "("+name+space+parameters.joinToString(" "){it.toText(true)}+")"
    }
}

interface Function {
    val id: String
    val symbol: String
    val lambda: (List<Any?>) -> Any?
    fun execute(args: List<Any?>): Any? {
        return lambda(args)
    }
}

interface Format {
    val mimetype: String
    val extensions: List<String>
    val supported: Boolean
    val scripting: Boolean

    fun decode(input: InputStream, charset: String): Any?
    fun encode(value: Any?, output: OutputStream, charset: String)

    fun decodeText(txt: String): Any? {
        return decode(ByteArrayInputStream(txt.toByteArray(Charset.forName(defaultCharset()))), defaultCharset())
    }

    fun encodeText(value: Any?, charset: String = defaultCharset()): String {
        val output = ByteArrayOutputStream()
        encode(value, output, charset)
        return output.toString(charset)
    }
}

class GeoLocation(var uri: URI) {
    init {
        if (uri.scheme != "geo")
            throw RuntimeException("Invalid uri scheme for geolocation: ${uri.scheme}")
    }
    private val parts = uri.toString().substring(4).split(";")[0].split(",").map {it.toDouble()}

    val latitude = getCoordinate(0)
    val longitude = getCoordinate(1)
    val altitude = getCoordinate(2)

    override fun toString(): String {
        return "geo:"+parts.joinToString(",")
    }

    fun getCoordinate(index: Int): Double {
        return if (index < 0 || index >= parts.size) 0.0 else parts[index]
    }
}

interface Namespace {
    val prefix: String
    val uri: String
    val readOnly: Boolean
    val keepUrlEncoding: Boolean get() = false
    val isEmpty: Boolean get() { return names.isEmpty() }
    val names: List<String>
    fun hasName(name: String): Boolean
    fun getValue(name: String): Any?
    fun setValue(name: String, value: Any?): Boolean

    fun execute(method: RestMethod, path: List<String>, query: Query, value: Any?): Response {
        if (path.isEmpty())
            return Response(this)
        var parent: Any? = this
        val last = path.size - (if (method == RestMethod.PUT || method == RestMethod.DELETE) 2 else 1)
        for (index in 0 .. last) {
            val key = path[index]
            parent =  parent.property(key).getValue()
        }

        val result = when (method) {
            RestMethod.GET -> if (query.isEmpty()) parent else query.filter.filter(parent)
            RestMethod.POST -> {
                if (parent is MutableCollection<*>) {
                    val c = parent as MutableCollection<Any?>
                    c.add(value)
                    (if (c is List<*>) c else c.toList())[c.size-1]
                }
                else
                    null
            }
            RestMethod.PUT -> {
                if (parent == null)
                    null
                else {
                    val key = path[path.size - 1]
                    val property = parent.property(key)
                    property.setValue(value)
                    property.getValue()
                }
            }
            RestMethod.DELETE -> parent?.property(path[path.size-1])?.removeValue() ?: false
        }

        return if (result is Response)
            result
        else if (result == null)
            Response(StatusCode.NotFound)
        else if (method == RestMethod.DELETE && result is Boolean)
            Response(if (result) StatusCode.NoContent else StatusCode.NotFound)
        else
            Response(result)
    }
}

interface Property {
    val instance: Any?
    val key: String
    fun getValue(): Any?
    fun setValue(value: Any?): Boolean
    fun removeValue(): Boolean
}

data class Response(val data: Any?, val type: String? = null, val charset: String = defaultCharset(), val status: StatusCode = StatusCode.OK) {
    constructor(statusCode: StatusCode): this(statusCode.message, null, defaultCharset(), statusCode)
    constructor(statusCode: StatusCode, message: String): this(message, null, defaultCharset(), statusCode)
    fun renderValue(): Any? {
        return if (this.status.code >= 400)
            this.status
        else if (this.data is InputStream)
            getFormat(this.type ?: "text/plain")!!.decode(this.data, this.charset ?: defaultCharset())
        else
            this.data
    }
}

enum class StatusCode(val code: Int) {
    OK(200),
    Created(201),
    Accepted(202),
    NoContent(204),
    MovedPermanently(301),
    Found(302),
    BadRequest(400),
    Unauthorized(401),
    Forbidden(403),
    NotFound(404),
    MethodNotAllowed(405),
    Conflict(409),
    InternalServerError(500),
    NotImplemented(501),
    BadGateway(502),
    ServiceUnavailable(503),
    GatewayTimeout(504);

    val message: String = this.name.uncamel(" ").lowercase()
}

data class WebSession(val id: String)

fun defaultCharset(charset: String? = null): String {
    if (charset != null)
        defaultCharset = charset
    return defaultCharset
}

fun defaultContentType(contentType: String? = null): String {
    if (contentType != null)
        defaultContentType = contentType
    return defaultContentType
}

fun <T: Any> convert(value: Any?, type: KClass<T>): T {
    if (type.isInstance(value))
        return value as T
    val converter = getConverter(type)
    if (converter != null)
        return converter.call(value) as T
    val srcClass = if (value == null) Void::class else value::class
    throw RuntimeException("Cannot convert ${srcClass.qualifiedName} to ${type.qualifiedName}")
}

fun Any?.resolve(deepResolving: Boolean = false): Any? {
    if (this is Expression)
        return this.execute().resolve(deepResolving)
    if (this is Map.Entry<*,*>)
        return this.value.resolve(deepResolving)
    if (this is Property)
        return this.getValue().resolve(deepResolving)
    if (this is URI)
        return this.get().resolve(deepResolving)
    if (this is URL)
        return this.toURI().get().resolve(deepResolving)
    if (this is File)
        return this.toURI().get().resolve(deepResolving)
    if (this is Response)
        return this.renderValue()
    if (deepResolving) {
        if (this.isText()) {
            val txt = this.toText()
            val uri = txt.toUri()
            if (uri != null)
                return uri.get().resolve(true)
            val cx = getContext()
            if (cx.hasName(txt))
                return cx.getValue(txt).resolve(true)
            return txt
        }
        if (this is Collection<*>)
            return this.map { it.resolve(false) }
        if (this != null && this::class.java.isArray)
            return ListArrayAdapter(this).map { it.resolve(false) }
    }
    return this
}

fun Any?.simplify(resolve: Boolean = false): Any? {
    val c = if (this is Collection<*>)
        this
    else if (this is Array<*>)
        this.toList()
    else if (this is CharSequence)
        if (this.isBlank()) emptyList() else listOf(this)
    else if (this == null)
        emptyList()
    else
        listOf(this)
    val value = when (c.size) {
        0 -> null
        1 -> c.iterator().next()
        else -> this
    }
    if (!resolve)
        return value
    if (value !is Collection<*>)
        return value.resolve()
    return value.map{it.simplify(true)}
}

fun Any?.property(keys: Any?): Property {
    val key = keys.simplify()
    return if (key.isIterable())
        CollectionProperty(this, key.toCollection().map { it.toText() })
    else if (key.isMappable())
        CollectionProperty(this, key.toMap()!!.keys.map { it.toText() })
    else
        GenericProperty(this, key.toText())
}

fun Any?.isReference(): Boolean {
    return this is Expression || this is Property || this is Map.Entry<*,*> || this is URL || this is URI || this is File
}

fun Any?.isEmpty(): Boolean {
    if (this == null)
        return true
    if (this is CharSequence)
        return this.isBlank()
    if (this is Number)
        return this == 0
    if (this is Boolean)
        return this == false
    if (this is Namespace)
        return this.isEmpty
    if (this is Query)
        return this.isEmpty
    if (this is Filter)
        return this.isEmpty
    if (this is Page)
        return this.range.isEmpty()
    if (this is Map<*,*>)
        return this.isEmpty()
    if (this is Iterable<*>)
        return !this.iterator().hasNext()
    if (this is Array<*>)
        return this.size == 0
    if (this::class.java.isArray)
        return java.lang.reflect.Array.getLength(this) == 0
    if (this is Iterator<*>)
        return !this.hasNext()
    if (this is Enumeration<*>)
        return !this.hasMoreElements()
    return BeanMap(this).size == 0
}

fun Any?.isNotEmpty(): Boolean {
    return !this.isEmpty()
}

fun Any?.isText(): Boolean {
    return this is CharSequence || this is Char || this is File || this is URI || this is URL || this is CharArray || this is ByteArray
                                || (this is Array<*> && (this.isArrayOf<Char>() || this.isArrayOf<Byte>()))
}

fun Any?.toText(printNull: Boolean = false, joinChar: String = ",", keyValue: String? = "="): String {
    if (this is CharSequence || this is Char)
        return this.toString()
    if (this is CharArray)
        return this.joinToString("")
    if (this is ByteArray)
        return this.toString(Charset.forName("utf8"))
    if (this is Array<*>) {
        if (this.isArrayOf<Char>())
            return this.joinToString("")
        if (this.isArrayOf<Byte>()) {
            val array = this as Array<Byte>
            return ByteArray(this.size){array[it]}.toString(Charset.forName("utf8"))
        }
    }
    if (this is Date) {
        if (this is java.sql.Timestamp || this is java.sql.Time || this is java.sql.Date)
            return this.toString()
        return java.sql.Timestamp(this.time).toString()
    }
    if (this is File)
        return this.toURI().toString()
    if (this is URI)
        return this.toString()
    if (this is URL)
        return this.toURI().toString()
    if (this is Function)
        return this.id
    if (this is Property)
        return this.key
    if (this is KCallable<*>)
        return this.name
    if (this is KClass<*>)
        return this.qualifiedName ?: this.java.name
    if (this is Class<*>)
        return this.kotlin.qualifiedName ?: this.name
    if (this is Member)
        return "${toString(this.declaringClass)}.${this.name}"
    if (this is Namespace)
        return if (this.prefix.isBlank()) this.uri else this.prefix+":/"
    if (this is Map.Entry<*,*>)
        return this.key.toText(printNull, joinChar, keyValue)
    if (this.isMappable()) {
        return if (keyValue == null)
            "(" + this.toMap()!!.entries.joinToString(joinChar) { it.value.toText(printNull, joinChar, keyValue) } + ")"
        else
            "(" + this.toMap()!!.entries.joinToString(joinChar) { it.key.toText( printNull, joinChar, keyValue) + keyValue + it.value.toText(printNull, joinChar, keyValue) } + ")"
    }
    if (this.isIterable())
        return "("+this.toCollection().joinToString(joinChar){it.toText(printNull, joinChar, keyValue)}+")"
    if (this is InputStream)
        return this.readBytes().toString(Charset.forName(defaultCharset()))
    if (this is Reader)
        return this.readText()
    if (this == null)
        return if (printNull) "null" else ""
    return this.toString()
}

fun Any?.isMappable(): Boolean {
    return this is Map<*,*> || this is Namespace || this is Map.Entry<*,*> || this is Property || this is Pair<*,*> || this is StringValues || (this != null && this::class.isData)
}

fun Any?.toMap(): Map<Any?,Any?>? {
    if (this is Map<*,*>)
        return this as Map<Any?,Any?>
    if (this is Namespace)
        return MapAdapter(
            { this.names.toSet() },
            { this.getValue(toString(it)) },
            if (this.readOnly) { _, _ -> null } else { k, v -> this.setValue(toString(k), v) },
            if (this.readOnly) { _ -> null } else { k -> this.setValue(toString(k), null) }
        )
    if (this is Map.Entry<*,*>)
        return mapOf(this.key to this.value)
    if (this is Property)
        return mapOf(this.key to this.getValue())
    if (this is Pair<*,*>)
        return mapOf(this)
    if (this is StringValues) {
        val map = mutableMapOf<Any?,Any?>()
        this.forEach { key, list ->
            map[key] =  list.map{it.keyword(it)}.simplify()
        }
        return map
    }
    if (this != null && this::class.isData)
        return BeanMap(this)
    if (this is Collection<*>) {
        val map = mutableMapOf<Any?,Any?>()
        for (item in this) {
            if (item is Map.Entry<*,*>)
                map[item.key] = item.value
            else if (item is Property)
                map[item.key] = item.getValue()
            else if (item is Pair<*,*>)
                map[item.first] = item.second
            else if (item.isMappable()) {
                val src = item.toMap()!!
                val key = src.primaryKey(true)
                if (key.isNotBlank())
                    map[key] = src
            }
            else
                map[map.size] = item
        }
        return map
    }
    if (this is Array<*>)
        return this.toList().toMap()
    return null
}

fun Any?.isIterable(): Boolean {
    if (this == null)
        return false
    return this is Iterator<*> || this is Enumeration<*> || this is Iterable<*> || this::class.java.isArray
}

fun Any?.toIterator(): Iterator<Any?>? {
    if (this is Iterator<*>)
        return this
    if (this is Enumeration<*>)
        return this.iterator()
    if (this is Iterable<*>)
        return this.iterator()
    if (this != null && this::class.java.isArray)
        return ArrayIterator(this)
    if (this is Map.Entry<*,*> || this is Property)
        return null
    if (this.isMappable())
        return this.toMap()?.entries?.iterator()
    return null
}

fun Any?.toCollection(): List<Any?> {
    return if (this == null)
        emptyList()
    else if (this is List<*>)
        this
    else if (this is Collection<*>)
        this.toList()
    else if (this is Array<*>)
        this.toList()
    else
        this.toIterator()?.asSequence()?.toList() ?: listOf(this)
}

fun Any?.toCompareOperator(throwOnFail: Boolean = false): CompareOperator? {
    if (this is CompareOperator)
        return this
    if (this is String) {
        val txt = this.lowercase()
        for (op in CompareOperator.entries) {
            if (op.symbol == txt || op.id == txt)
                return op
        }
    }
    if (throwOnFail)
        throw RuntimeException("Invalid compare operator: $this")
    else
        return null
}

fun Any?.toLogicOperator(throwOnFail: Boolean = false): LogicOperator? {
    if (this is LogicOperator)
        return this
    if (this is String) {
        val txt = this.lowercase()
        for (op in LogicOperator.entries) {
            if (op.symbol == txt || op.id == txt)
                return op
        }
    }
    if (throwOnFail)
        throw RuntimeException("Invalid logic operator: $this")
    else
        return null
}

fun Any?.toMathOperator(throwOnFail: Boolean = false): MathOperator? {
    if (this is MathOperator)
        return this
    if (this is String) {
        val txt = this.lowercase()
        for (op in MathOperator.entries) {
            if (op.symbol == txt || op.id == txt)
                return op
        }
    }
    if (throwOnFail)
        throw RuntimeException("Invalid math operator: $this")
    else
        return null
}

fun Any?.toUri(): URI? {
    if (this is URI)
        return this
    if (this is URL)
        return this.toURI()
    if (this is File)
        return this.canonicalFile.toURI()
    if (this.isText()) {
        val txt = this.toText().trim()
        try {
            if (txt.startsWith("./") || txt.startsWith("../") || (txt.startsWith("/") && txt.length > 1))
                return File(txt).canonicalFile.toURI()
            val scheme = if (txt.contains(":")) txt.substring(0, txt.indexOf(":")) else null
            if (scheme != null && isValidUriScheme(scheme))
                return URI(txt)
        } catch (e: Exception) {}
    }
    return null
}

fun URI.contentType(): String? {
    return when (this.scheme) {
        "data" -> this.toString().split(",")[0].split(";")[0].split(":")[1]
        "file" -> if (File(this.path).isDirectory) "inode/directory" else detectFileType(this.path)
        "ftp", "sftp", "ftps" -> detectFileType(this.path)
        "http", "https" -> {
            val type = this.toURL().openConnection().contentType
            if (type == null) null else type.split(";")[0].trim()
        }
        else -> null
    }
}

fun URI.get(headers: Map<String,String> = mapOf()): Any? {
    return resolveUri(this, RestMethod.GET, headers, null)
}

fun URI.post(value: Any?, headers: Map<String,String> = mapOf()): Any? {
    return resolveUri(this, RestMethod.POST, headers, value)
}

fun URI.put(value: Any?, headers: Map<String,String> = mapOf()): Any? {
    return resolveUri(this, RestMethod.PUT, headers, value)
}

fun URI.delete(headers: Map<String,String> = mapOf()): Any? {
    return resolveUri(this, RestMethod.DELETE, headers, null)
}

fun String.keyword(defaultValue: Any?): Any? {
    return when (this.trim().lowercase()) {
        "null", "" -> null
        "true" -> true
        "false" -> false
        else -> this.toLongOrNull()
             ?: this.toDoubleOrNull()
             ?: this.toTemporal()
             ?: this.toUri()
             ?: defaultValue
    }
}

fun String.detectFormat(createDataUri: Boolean = false): String? {
    val txt = this.trim()
    if (txt.isBlank())
        return null
    val type = if (txt.startsWith("---\n"))
        "application/yaml"
    else if ((txt[0] == '{' && txt[txt.length-1] == '}') || (txt[0] == '[' && txt[txt.length-1] == ']') || (txt[0] == '"' && txt[txt.length-1] == '"'))
        "application/json"
    else if (txt.toCharArray().none { it <= ' ' } && txt.toCharArray().any { it == '=' })
        "application/x-www-form-urlencoded"
    else if (txt.toCharArray().any { it == '\n' }) {
        val lines = txt.split("\n")
        if (lines[0].toCharArray().none { it <= ' ' } && lines[0].toCharArray().any { it == ',' })
            "text/csv"
        else
            null
    }
    else
        null
    return if (type == null)
        null
    else if (!createDataUri)
        type
    else {
        try { "data:$type,${txt.urlEncode()}" }
        catch (e: Exception) { null }
    }
}

fun String.normalize(): String {
    val txt = if (Normalizer.isNormalized(this, NORMALIZED_FORM))
        this
    else
        Normalizer.normalize(this, NORMALIZED_FORM).replace("\\p{M}".toRegex(), "")
    return txt.lowercase().trim()
}

fun String.uncamel(separator: String): String {
    val words = mutableListOf<String>()
    var word: String = ""
    for (c in this.toCharArray()) {
        if (!c.isLetterOrDigit()) {
            if (word.isNotEmpty()) {
                words.add(word)
                word = ""
            }
        }
        else if (c.isUpperCase()) {
            if (word.isEmpty() || word[word.length-1].isUpperCase())
                word += c
            else {
                words.add(word)
                word = "$c"
            }
        }
        else
            word += c
    }
    if (word.isNotEmpty())
        words.add(word)
    return words.joinToString(separator)
}

fun String.urlEncode(charset: String = defaultCharset()): String {
    return URLEncoder.encode(this, charset).replace("+", "%20")
}

fun String.urlDecode(charset: String = defaultCharset()): String {
    return URLDecoder.decode(this, charset)
}

fun String.toExpression(): Expression {
    return parseText(this)
}

fun String.toClass(): KClass<*>? {
    try { return Class.forName(this).kotlin }
    catch (e: Exception) {
        try {
            if (this.startsWith("kotlin.collections."))
                return Class.forName("java.util."+this.substring("kotlin.collections.".length)).kotlin
            if (this.startsWith("kotlin."))
                return Class.forName("java.lang."+this.substring("kotlin.".length)).kotlin
        }
        catch (e: Exception) {}
    }
    return null
}

fun String.toTemporal(): Temporal? {
    try { return LocalDateTime.parse(this, TIMESTAMP_FORMAT) }
    catch (e: Exception) {
        try { return LocalDate.parse(this, DATE_FORMAT) }
        catch (e2: Exception) {
            try { return LocalTime.parse(this, TIME_FORMAT) }
            catch (e3: Exception) {
                try { return LocalTime.parse(this, HOUR_FORMAT) }
                catch (e4: Exception) {}
            }
        }
    }
    return null
}

fun Temporal.toDateTime(): LocalDateTime {
    if (this is LocalDateTime)
        return this
    if (this is LocalDate)
        return this.toDateTime()
    if (this is LocalTime)
        return this.toDateTime()
    if (this is Instant)
        return this.toDateTime()
    throw RuntimeException("Temporal format not supported: ${this::class.simpleName}")
// TODO: HijrahDate, JapaneseDate, MinguoDate, OffsetDateTime, OffsetTime, ThaiBuddhistDate, Year, YearMonth, ZonedDateTime
}

fun Temporal.toDate(): LocalDate {
    if (this is LocalDate)
        return this
    if (this is LocalDateTime)
        return this.toDate()
    if (this is LocalTime)
        return this.toDate()
    if (this is Instant)
        return this.toDate()
    throw RuntimeException("Temporal format not supported: ${this::class.simpleName}")
}

fun Temporal.toTime(): LocalTime {
    if (this is LocalTime)
        return this
    if (this is LocalDate)
        return this.toTime()
    if (this is LocalDateTime)
        return this.toTime()
    if (this is Instant)
        return this.toTime()
    throw RuntimeException("Temporal format not supported: ${this::class.simpleName}")
}

fun Number.toBytes(): ByteArray {
    val bytes = ByteArrayOutputStream()
    var n = this.toLong()
    while (n > 0) {
        bytes.write((n % 256).toInt())
        n /= 256
    }
    return bytes.toByteArray()
}

fun Map<out Any?,Any?>.primaryKey(forceFirst: Boolean): String {
    val keys = this.keys
    if (keys.isEmpty())
        return ""
    if (keys.contains("id"))
        return "id"
    if (keys.contains("code"))
        return "code"
    for (key in keys) {
        val value = this[key]
        if (value is String || value == String::class)
            return key.toString()
    }
    return if (forceFirst) keys.iterator().next().toString() else ""
}

fun Map<out Any?,Any?>.label(): String {
    val map = this
    return when (map.size) {
        0 -> map::class.toString()
        1 -> map.iterator().next().value.toString()
        2 -> map[map.keys.filter { it != "id" }[0]].toString()
        else -> {
            val cx = getContext()
            var name = ""
            val lists = listOf(LABEL_KEYS, cx.getValue("languages").toCollection(), VALUE_KEYS)
            for (list in lists) {
                for (key in list) {
                    val value = map[key]
                    if (value != null) {
                        name = value.toString()
                        break
                    }
                }
                if (name.isNotBlank())
                    break
            }
            if (name.isBlank())
                name = map.values.filterIsInstance<String>().joinToString(" ")
            name
        }
    }
}

fun KClass<*>.parents(): Set<KClass<*>> {
    val classes = LinkedHashSet<KClass<*>>()
    var parent: KClass<*>? = this
    while (parent != null) {
        classes.add(parent)
        classes.addAll(parent!!.superclasses)
    }
    return classes
}

private val NORMALIZED_FORM = Normalizer.Form.NFD
private val DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd")
private val HOUR_FORMAT = DateTimeFormatter.ofPattern("HH:mm")
private val TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss")
private val TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
private val VIEW_TYPES = "imane,audio,video,model".split(",")
private val LABEL_KEYS = "name,label,symbol".split(",")
private val VALUE_KEYS = "value,id,type".split(",")
private var defaultCharset = "utf8"
private var defaultContentType = "application/json"
