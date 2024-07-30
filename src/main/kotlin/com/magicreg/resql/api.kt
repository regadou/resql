package com.magicreg.resql

import org.apache.commons.beanutils.BeanMap
import java.io.*
import java.lang.reflect.Executable
import java.net.URI
import java.net.URL
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
import kotlin.reflect.KClass
import kotlin.reflect.KFunction

enum class DataType { ANY, NUMBER, FUNCTION, ENTITY, COLLECTION, TEXT, VIEW }
enum class UriMethod { GET, POST, PUT, DELETE }
data class Response(val data: Any?, val type: String? = null, val charset: String = defaultCharset(), val status: StatusCode = StatusCode.OK) {
    constructor(statusCode: StatusCode): this(statusCode.message, null, defaultCharset(), statusCode)
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

    val message: String = LetterCase.LOWER.format(this.name.uncamel(" "))
}

enum class CompareOperator(override val symbol: String, override val function: (List<Any?>) -> Any?): Function {
    LESS("<", ::less_func), NOT_LESS(">=", ::not_less_func),
    EQUAL("=", ::equal_func), NOT_EQUAL("!=", ::not_equal_func),
    MORE(">", ::more_func), NOT_MORE("<=", ::not_more_func),
    IN("@", ::in_func), NOT_IN("!@", ::not_in_func),
    BETWEEN("><", ::between_func), NOT_BETWEEN("!><", ::not_between_func),
    MATCH("~", ::match_func), NOT_MATCH("!~", ::not_match_func);

    override val id = this.name.lowercase()
    override fun toString(): String { return "$id()" }
}

enum class LogicOperator(override val symbol: String, override val function: (List<Any?>) -> Any?): Function {
    OR("|", ::or_func),
    AND("&", ::and_func);

    override val id = this.name.lowercase()
    override fun toString(): String { return "$id()" }
}

enum class MathOperator(override val symbol: String, override val function: (List<Any?>) -> Any?): Function {
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

data class Configuration(
    val host: String = "localhost",
    val port: Int = 0,
    val routes: Map<String,Namespace> = emptyMap(),
    val maxPageSize: Int = 100
)

data class Expression(
    val function: Function? = null,
    val parameters: List<Any?> = emptyList()
) {
    fun value(): Any? {
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
        return "("+name+space+parameters.joinToString(" ")+")"
    }
}

interface Function {
    val id: String
    val symbol: String
    val function: (List<Any?>) -> Any?
    fun execute(args: List<Any?>): Any? {
        return function(args)
    }
}

interface Format {
    val mimetype: String
    val extensions: List<String>
    val supported: Boolean

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

enum class LetterCase {
    IGNORE, LOWER, UPPER;

    fun format(txt: String): String {
        return when(this) {
            IGNORE -> txt
            UPPER -> txt.uppercase()
            LOWER -> txt.lowercase()
        }
    }

    fun compare(t1: String, t2: String): Int {
        return when(this) {
            IGNORE -> t1.compareTo(t2)
            UPPER -> t1.uppercase().compareTo(t2.uppercase())
            LOWER -> t1.lowercase().compareTo(t2.lowercase())
        }
    }
}

interface Namespace {
    val prefix: String
    val uri: String
    val readOnly: Boolean
    val isEmpty: Boolean get() { return names.isEmpty() }
    val names: List<String>
    fun hasName(name: String): Boolean
    fun value(name: String): Any?
    fun setValue(name: String, value: Any?): Boolean

    fun toMap(): MutableMap<String,Any?> {
        return MapAdapter(
            { this.names.toSet() },
            { this.value(toString(it)) },
            if (this.readOnly) { _, _ -> null } else { k, v -> this.setValue(toString(k), v) },
            if (this.readOnly) { _ -> null } else { k -> this.setValue(toString(k), null) }
        )
    }

    fun apply(method: UriMethod, path: List<String>, query: Query, value: Any?): Response {
        val result = method.execute(toMap(), path, query, value)
        return if (result == null)
            Response(StatusCode.NotFound)
        else if (method == UriMethod.DELETE && result is Boolean)
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
}

fun defaultCharset(charset: String? = null): String {
    if (charset != null)
        defaultCharset = charset
    return defaultCharset
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

fun Any?.dataType(): DataType {
    return if (this == null)
        DataType.ANY
    else if (this is File || this is URI || this is URL) {
        val uri = this.toUri()
        if (uri == null)
            DataType.TEXT
        else if (uri.scheme == "geo")
            DataType.ENTITY
        else {
            val type = uri.contentType()
            if (type != null && VIEW_TYPES.contains(type.split("/")[0])) DataType.VIEW else DataType.TEXT
        }
    }
    else if (this.isText())
        DataType.TEXT
    else if (this is Number || this is Boolean)
        DataType.NUMBER
    else if (this is Function || this is UriMethod || this is KFunction<*> || this is Executable)
        DataType.FUNCTION
    else if (this.isMappable() || this is Temporal || this is java.util.Date)
        DataType.ENTITY
    else if (this.isIterable())
        DataType.COLLECTION
    else {
        val bean = BeanMap(this)
        return if (bean.size > 0) DataType.ENTITY else DataType.ANY
    }
}

fun Any?.resolve(deepResolving: Boolean = false): Any? {
    if (this is Expression)
        return this.value().resolve(deepResolving)
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
    if (deepResolving) {
        if (this.isText()) {
            val txt = this.toText()
            val uri = txt.toUri()
            if (uri != null)
                return uri.get().resolve(true)
            val cx = getContext()
            if (cx.hasName(txt))
                return cx.value(txt).resolve(true)
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
    return value.map{it.simplify(true)}.simplify(true)
}

fun Any?.property(key: String): Property {
    return GenericProperty(this, key)
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

fun Any?.toText(): String {
    if (this is CharSequence || this is Char)
        return this.toString()
    if (this is File)
        return this.toURI().toString()
    if (this is URI)
        return this.toString()
    if (this is URL)
        return this.toURI().toString()
    if (this is Namespace)
        return this.uri
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
    if (this is InputStream)
        return this.readBytes().toString(Charset.forName("utf8"))
    if (this is Reader)
        return this.readText()
    if (this.isIterable())
        return "("+this.toIterator()!!.asSequence().joinToString(","){it.toText()}+")"
    if (this.isMappable())
        return "("+this.toMap()!!.entries.joinToString(","){it.key.toText()+"="+it.value.toText()}+")"
    if (this == null)
        return ""
    return this.toString()
}

fun Any?.isMappable(): Boolean {
    return this is Map<*,*> || this is Namespace || this is Map.Entry<*,*> || this is Property || (this != null && this::class.isData)
}

fun Any?.toMap(): Map<Any?,Any?>? {
    if (this is Map<*,*>)
        return this as Map<Any?,Any?>
    if (this is Namespace)
        return this.toMap() as Map<Any?,Any?>
    if (this is Map.Entry<*,*>)
        return mapOf(this.key to this.value)
    if (this is Property)
        return mapOf(this.key to this.getValue())
    if (this != null && this::class.isData)
        return BeanMap(this)
    if (this is Collection<*>) {
        val map = mutableMapOf<Any?,Any?>()
        for (item in this) {
            if (item is Map.Entry<*,*>)
                map[item.key] = item.value
            else if (item is Property)
                map[item.key] = item.getValue()
            else
                return null
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

fun Any?.toUri(): URI? {
    if (this is URI)
        return this
    if (this is URL)
        return this.toURI()
    if (this is File)
        return this.canonicalFile.toURI()
    if (this is CharSequence) {
        val txt = this.toString().trim()
        try {
            if (txt.startsWith("/") || txt.startsWith("./") || txt.startsWith("../"))
                return File(txt).canonicalFile.toURI()
            val scheme = if (txt.contains(":")) txt.substring(0, txt.indexOf(":")) else null
            if (scheme != null && (isBuiltinUriScheme(scheme) || getNamespace(scheme) != null))
                return URI(txt)
            // TODO: test word against all context configured default namespaces prefixes
        } catch (e: Exception) {}
    }
    return null
}

fun URI.readOnly(): Boolean {
    val uri = resolveRelativeTemplate(this)
    if (uri.scheme == "data" && uri.toString().indexOf(",") < 0)
        return false
    return getNamespace(uri.scheme)?.readOnly ?: arrayOf("data", "geo", "resql").contains(uri.scheme)
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
    return resolveUri(resolveRelativeTemplate(this), UriMethod.GET, headers, null)
}

fun URI.post(value: Any?, headers: Map<String,String> = mapOf()): Any? {
    return resolveUri(resolveRelativeTemplate(this), UriMethod.POST, headers, value)
}

fun URI.put(value: Any?, headers: Map<String,String> = mapOf()): Any? {
    return resolveUri(resolveRelativeTemplate(this), UriMethod.PUT, headers, value)
}

fun URI.delete(headers: Map<String,String> = mapOf()): Any? {
    return resolveUri(resolveRelativeTemplate(this), UriMethod.DELETE, headers, null)
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

// TODO: if method is delete and filter is not empty: use it to select items to delete
fun UriMethod.execute(root: MutableMap<String,Any?>, path: List<String>, query: Query, value: Any?): Any? {
    if (path.isEmpty())
        return root
    if (path.size == 1) {
        val key = path[0]
        return when (this) {
            UriMethod.GET -> query.filter.filter(root[key])
            UriMethod.POST -> {
                if (value is MutableCollection<*>) {
                    (value as MutableCollection<Any?>).add(value)
                    value.toList()[value.size-1]
                }
                else
                    null
            }
            UriMethod.PUT -> {
                root[key] = value
                root[key]
            }
            UriMethod.DELETE -> root.remove(key)
        }
    }

    val values = Array<Any?>(path.size){null}
    for (index in path.indices) {
        val key = path[index]
        val result = if (index == 0)
            root[key].resolve()
        else {
            val parent = values[index - 1]
            parent.property(key).getValue()
        }
        if (this == UriMethod.PUT) {
            if (result == null)
                break
            values[index] = result
            if (index+2 >= path.size)
                break
        }
        else if (result != null)
            values[index] = result
        else if (this == UriMethod.DELETE)
            return false
        else
            return null
    }

    return when (this) {
        UriMethod.GET -> query.filter.filter(values[values.size-1])
        UriMethod.POST -> {
            val result = values[values.size-1]
            if (result is MutableCollection<*>) {
                (result as MutableCollection<Any?>).add(value)
                value
            }
            else
                null
        }
        UriMethod.PUT -> {
            val parent = values[values.size - 2]
            if (parent == null)
                null
            else {
                val key = path[path.size - 1]
                val property = parent.property(key)
                property.setValue(value)
                property.getValue()
            }
        }
        UriMethod.DELETE -> {
            values[values.size-2].property(path[path.size-1]).setValue(null)
        }
    }
}

fun String.parse(): Any? {
    return when (this.trim()) {
        "null", "" -> null
        "true" -> true
        "false" -> false
        else -> this.toLongOrNull()
             ?: this.toDoubleOrNull()
             ?: this.toTemporal()
             ?: this.toUri()
             ?: this
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
        try { "data:$type,${URLEncoder.encode(txt, defaultCharset())}" }
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

fun Number.toBytes(): ByteArray {
    val bytes = ByteArrayOutputStream()
    var n = this.toLong()
    while (n > 0) {
        bytes.write((n % 256).toInt())
        n /= 256
    }
    return bytes.toByteArray()
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

private val NORMALIZED_FORM = Normalizer.Form.NFD
private val DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd")
private val HOUR_FORMAT = DateTimeFormatter.ofPattern("HH:mm")
private val TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss")
private val TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
private val VIEW_TYPES = "imane,audio,video,model".split(",")
private var defaultCharset = "utf8"