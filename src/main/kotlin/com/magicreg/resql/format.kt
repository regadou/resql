package com.magicreg.resql

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.apache.commons.csv.CSVFormat
import java.io.*
import java.lang.reflect.Member
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.*
import kotlin.reflect.KCallable
import kotlin.reflect.KClass
import kotlin.reflect.KProperty

fun getSupportedFormats(): List<Format> {
    return MIMETYPES.values.filter{it.supported}.toSortedSet(::compareFormats).toList()
}

fun getFormat(txt: String): Format? {
    val lower = txt.lowercase()
    return MIMETYPES[lower] ?: EXTENSIONS[lower]
}

fun getExtensionMimetype(txt: String): String? {
    return EXTENSIONS[txt.lowercase()]?.mimetype
}

fun getMimetypeExtensions(txt: String): List<String> {
    val format = MIMETYPES[txt.lowercase()]
    return format?.extensions ?: listOf()
}

fun detectFileType(path: String?): String? {
    if (path == null)
        return null
    var parts = path.trim().split("/")
    val filename = parts[parts.size-1]
    if (filename == "" || filename.indexOf(".") < 0)
        return null
    if (filename[0] == '.')
        return "text/plain"
    parts = filename.split(".")
    return getExtensionMimetype(parts[parts.size-1])
}

fun loadAllMimetypes(): Map<String,List<String>> {
    val map = mutableMapOf<String,MutableList<String>>()
    val reader = BufferedReader(InputStreamReader(GenericFormat::class.java.getResource("/mime.types").openStream()))
    reader.lines().forEach { line ->
        val escape = line.indexOf('#')
        val txt = if (escape >= 0) line.substring(0, escape) else line
        var mimetype: String? = null
        val extensions = mutableListOf<String>()
        val e = StringTokenizer(txt.lowercase())
        while (e.hasMoreElements()) {
            val token = e.nextElement().toString()
            if (mimetype == null)
                mimetype = token
            else
                extensions.add(token)
        }
        if (mimetype != null)
            map[mimetype] = extensions
    }
    reader.close()
    return map
}

class GenericFormat(
    override val mimetype: String,
    override val extensions: List<String>,
    private val decoder: (InputStream, String) -> Any?,
    private val encoder: (Any?, OutputStream, String) -> Unit,
    override val supported: Boolean,
    override val scripting: Boolean
): Format {
    override fun decode(input: InputStream, charset: String): Any? {
        return decoder(input, charset)
    }

    override fun encode(value: Any?, output: OutputStream, charset: String) {
        encoder(value, output, charset)
    }

    override fun toString(): String {
        return "Format($mimetype)"
    }
}

private const val CSV_MINIMUM_CONSECUTIVE_FIELDS_SIZE = 5
private const val CSV_BUFFER_SIZE = 1024
private const val JSON_EXPRESSION_START = "{[\""
private val CSV_SEPARATORS = ",;|:\t".toCharArray()
private val JSON_SERIALIZER_CLASSES = listOf(KClass::class, KCallable::class, Class::class, Member::class, KProperty::class, Exception::class,
                                             InputStream::class, OutputStream::class, Reader::class, Writer::class, ByteArray::class, CharArray::class,
                                             Property::class, Namespace::class, Format::class)
private val CSV_FORMAT = configureCsvFormat()
private val JSON_MAPPER = configureJsonMapper(ObjectMapper())
private val YAML_MAPPER = configureJsonMapper(ObjectMapper(YAMLFactory()))
private val EXTENSIONS = mutableMapOf<String,Format>()
private val MIMETYPES = loadMimeTypesFile()

private fun configureJsonMapper(mapper: ObjectMapper): ObjectMapper {
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true)
        .configure(SerializationFeature.WRITE_CHAR_ARRAYS_AS_JSON_ARRAYS, false)
        .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
        .configure(SerializationFeature.FAIL_ON_SELF_REFERENCES, false)
        .configure(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS, false)
        .configure(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS, true)
        .configure(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS, false)
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .configure(SerializationFeature.WRITE_DATES_WITH_ZONE_ID, false)
        .configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
        .configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, true)
        .configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true)
        .configure(SerializationFeature.WRITE_ENUMS_USING_INDEX, false)
        .configure(SerializationFeature.WRITE_NULL_MAP_VALUES, true)
        .configure(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED, false)
        .setDateFormat(SimpleDateFormat("yyyy-MM-dd hh:mm:ss"))
    val module = SimpleModule()
    for (klass in JSON_SERIALIZER_CLASSES)
        addSerializer(module, klass.java)
    return mapper.registerModule(module).registerModule(JavaTimeModule())
}

private fun loadMimeTypesFile(): MutableMap<String,Format> {
    val mimetypes = initManagedMimetypes()
    for (entry in loadAllMimetypes().entries) {
        val mimetype = entry.key
        val extensions = entry.value
        if (mimetypes[mimetype] != null)
            ;
        else if (mimetype.split("/")[0] == "text")
            addMimetype(mimetypes, ::decodeString, ::encodeString, mimetype, extensions, false)
        else
            addMimetype(mimetypes, ::decodeBytes, ::encodeBytes, mimetype, extensions, false)
    }
    return mimetypes
}

private fun addMimetype(mimetypes: MutableMap<String,Format>,
                        decoder: (InputStream, String) -> Any?,
                        encoder: (Any?, OutputStream, String) -> Unit,
                        mimetype: String,
                        extensions: List<String>,
                        supported: Boolean = true,
                        scripting: Boolean = false) {
    val format = GenericFormat(mimetype, extensions, decoder, encoder, supported, scripting)
    mimetypes[mimetype] = format
    addExtensions(format)
}

private fun addExtensions(format: Format) {
    for (extension in format.extensions) {
        if (!EXTENSIONS.containsKey(extension))
            EXTENSIONS[extension] = format
    }
}

private fun initManagedMimetypes(): MutableMap<String,Format> {
    val formats = mutableMapOf<String,Format>()
    addMimetype(formats, ::decodeBytes, ::encodeBytes, "application/octet-stream", listOf("bin"))
    addMimetype(formats, ::decodeString, ::encodeString, "text/plain", listOf("txt"))
    addMimetype(formats, ::decodeHtml, ::encodeHtml, "text/html", listOf("html", "htm", "xhtml"))
    addMimetype(formats, ::decodeProperties, ::encodeProperties, "text/x-java-properties", listOf("properties"))
    addMimetype(formats, ::decodeCsv, ::encodeCsv, "text/csv", listOf("csv"))
    addMimetype(formats, ::decodeKotlin, ::encodeKotlin, "text/x-kotlin", listOf("kts", "kt"), true, true)
    addMimetype(formats, ::decodeResql, ::encodeResql, "text/x-resql", listOf("resql"), true, true)
    addMimetype(formats, ::decodeSql, ::encodeSql, "application/sql", listOf("sql"), true, true)
    addMimetype(formats, ::decodeJson, ::encodeJson, "application/json", listOf("json"))
    addMimetype(formats, ::decodeYaml, ::encodeYaml, "application/yaml", listOf("yaml"))
    addMimetype(formats, ::decodeForm, ::encodeForm, "application/x-www-form-urlencoded", listOf("form", "urlencoded"))
    return formats
}

private fun decodeString(input: InputStream, charset: String): Any? {
    return input.readAllBytes().toString(Charset.forName(charset))
}

private fun encodeString(value: Any?, output: OutputStream, charset: String) {
    if (value != null)
        output.write(toString(value).toByteArray(Charset.forName(charset)))
}

private fun decodeBytes(input: InputStream, charset: String): Any? {
    return input.readAllBytes()
}

private fun encodeBytes(value: Any?, output: OutputStream, charset: String) {
    if (value is ByteArray)
        output.write(value)
    else if (value != null)
        output.write(toString(value).toByteArray(Charset.forName(charset)))
}

private fun decodeJson(input: InputStream, charset: String): Any? {
    return JSON_MAPPER.readValue(InputStreamReader(input, charset), Any::class.java)
}

private fun encodeJson(value: Any?, output: OutputStream, charset: String) {
    encodeString(JSON_MAPPER.writeValueAsString(value)+"\n", output, charset)
}

private fun decodeYaml(input: InputStream, charset: String): Any? {
    return YAML_MAPPER.readValue(InputStreamReader(input, charset), Any::class.java)
}

private fun encodeYaml(value: Any?, output: OutputStream, charset: String) {
    encodeString(YAML_MAPPER.writeValueAsString(value)+"\n", output, charset)
}

private fun decodeProperties(input: InputStream, charset: String?): Any? {
    val props = Properties()
    props.load(InputStreamReader(input, charset))
    return props
}

private fun encodeProperties(value: Any?, output: OutputStream, charset: String?) {
    var properties: Properties?
    if (value is Properties)
        properties = value
    else if (value == null)
        properties = Properties()
    else {
        val map = toMap(value)
        properties = Properties()
        for (key in map.keys)
            properties.setProperty(toString(key), toString(map[key]))
    }
    properties.store(OutputStreamWriter(output, charset), "")
}

private fun decodeKotlin(input: InputStream, charset: String): Any? {
    return getKotlinParser().execute(input.readAllBytes().toString(Charset.forName(charset)))
}

private fun encodeKotlin(value: Any?, output: OutputStream, charset: String) {
    output.write((value.toString()+"\n").toByteArray(Charset.forName(charset)))
}

private fun decodeResql(input: InputStream, charset: String): Any? {
    val txt = input.readAllBytes().toString(Charset.forName(charset))
    return txt.toExpression().execute().resolve()
}

private fun encodeResql(value: Any?, output: OutputStream, charset: String) {
    val txt = value?.toText() ?: "null"
    output.write(("$txt\n").toByteArray(Charset.forName(charset)))
}

private fun decodeSql(input: InputStream, charset: String): Any? {
    val request = getContext().requestUri ?: throw RuntimeException("No current request to find database from")
    val prefix = request.split("/").firstOrNull { it.isNotBlank() } ?: throw RuntimeException("No database on empty request path")
    val ns = getContext().namespace(prefix) ?: throw RuntimeException("$prefix is not a namespace")
    if (ns is Database) {
        val sql = input.readAllBytes().toString(Charset.forName(charset))
        return when (sql.trim().substring(0, 4).lowercase()) {
            "sele", "show", "desc" -> ns.query(sql)
            "inse", "upda", "dele" -> ns.update(sql)
            else -> ns.execute(sql)
        }
    }
    throw RuntimeException("$prefix is not a database namespace")
}

private fun encodeSql(value: Any?, output: OutputStream, charset: String) {
    // TODO: we could check if it is a QUery object to convert to SQL select
    throw RuntimeException("SQL encoding is not supported")
}

private fun decodeCsv(input: InputStream, charset: String): Any {
    val dst = mutableListOf<Map<String,Any>>()
    val bufferedInput = BufferedInputStream(input, CSV_BUFFER_SIZE)
    val records = CSV_FORMAT.builder().setDelimiter(detectSeparator(bufferedInput)).build().parse(InputStreamReader(bufferedInput, charset))
    val fields = mutableListOf<String>()
    for (record in records) {
        if (fields.isEmpty()) {
            val it = record.iterator()
            while (it.hasNext())
                fields.add(it.next())
        }
        else {
            val map = mutableMapOf<String,Any>()
            val n = Math.min(fields.size, record.size())
            for (f in 0 until n)
                map[fields[f]] = record.get(f)
            dst.add(map)
        }
    }
    return dst
}

private fun encodeCsv(value: Any?, output: OutputStream, charset: String) {
    val printer = CSV_FORMAT.print(OutputStreamWriter(output, charset))
    val records = toList(value)
    val fields = mutableListOf<String>()
    var lastSize = 0
    var consecutives = 0
    for (record in records) {
        val map = toMap(record)
        for (key in map.keys) {
            if (fields.indexOf(key) < 0)
                fields.add(key.toString())
        }
        if (lastSize == fields.size)
            consecutives++
        else {
            consecutives = 0
            lastSize = fields.size
        }
        if (consecutives >= CSV_MINIMUM_CONSECUTIVE_FIELDS_SIZE)
            break;
    }
    printer.printRecord(fields)
    for (record in records) {
        val map = toMap(record)
        for (field in fields) {
            val cell = map[field];
            printer.print(toString(cell))
        }
        printer.println();
    }
    printer.flush()
}

private fun decodeForm(input: InputStream, charset: String): Any? {
    val map = mutableMapOf<String,Any?>()
    val txt = InputStreamReader(input, charset).readText()
    if (txt.isNotBlank()) {
        val entries = txt.split("&")
        for (entry in entries) {
            val eq = entry.indexOf('=')
            if (eq < 0)
                setValue(map, entry.urlDecode(charset), true)
            else
                setValue(map, entry.substring(0,eq).urlDecode(charset), entry.substring(eq+1).urlDecode(charset))
        }
    }
    return map
}

private fun encodeForm(value: Any?, output: OutputStream, charset: String) {
    val entries = mutableListOf<String>()
    val map = toMap(value)
    for (key in map.keys) {
        entries.add((key?.toString()?.urlEncode() ?: "null")+"="+
                    (map[key]?.toString()?.urlEncode() ?: "null"))
    }
    encodeString(entries.joinToString("&"), output, charset)
}

private fun decodeHtml(input: InputStream, charset: String): Any? {
    return input.readAllBytes().toString(Charset.forName(charset))
}

private fun encodeHtml(value: Any?, output: OutputStream, charset: String) {
    val style = getContext().configuration().style
    val head = arrayOf(
        if (style.isNullOrBlank()) "" else "<link href='$style' rel='stylesheet'>",
        if (charset.isNullOrBlank()) "" else "<meta charset='$charset'>"
    )
    output.write(encodeHtmlPart(value, head.joinToString("\n")).toByteArray(Charset.forName(charset)))
}

private fun encodeHtmlPart(value: Any?, head: String = ""): String {
    return if (value == null)
        "$head&nbsp;"
    else if (value.isMappable()) {
        val map = value.toMap() ?: toMap(value)
        val lines = mutableListOf<String>()
        for (key in map.keys) {
            val k = encodeHtmlPart(key)
            val v = encodeHtmlPart(map[key])
            lines.add("<tr><td valign=top>$k</td><td valign=top>$v</td></tr>")
        }
        "$head<table border=1 cellpadding=1 cellspacing=2>\n"+lines.joinToString("\n")+"\n</table>\n"
    }
    else if (value.isIterable()) {
        val it = value.toIterator() ?: listOf(value).iterator()
        val lines = mutableListOf<String>()
        while (it.hasNext())
            lines.add(encodeHtmlPart(it.next()))
        head+lines.joinToString("<br>\n")
    }
    else
        head+value.toText() // TODO: html encode
}

private fun configureCsvFormat(): CSVFormat {
    return CSVFormat.DEFAULT.builder().setDelimiter(CSV_SEPARATORS[0]).build()
}

private fun <T> addSerializer(module: SimpleModule, klass: Class<T>) {
    val serializer = object : JsonSerializer<T>() {
        override fun serialize(value:T, jgen: JsonGenerator, provider: SerializerProvider) {
            jgen.writeString(value.toText())
        }
    }
    module.addSerializer(klass, serializer)
}

private fun parseValue(src: Any?): Any? {
    if (src is CharSequence) {
        return when (val value = src.toString().urlDecode().trim()) {
            "null" -> null
            "true" -> true
            "false" -> false
            else -> value.toLongOrNull() ?: value.toDoubleOrNull() ?: getExpression(value.trim())
        }
    }
    return src
}

private fun getExpression(value: String): Any? {
    if (value.isEmpty())
        return ""
    if (JSON_EXPRESSION_START.indexOf(value[0]) < 0)
        return value
    val first = value[0]
    val last = value[value.length-1]
    if ((first == '[' && last == ']') || (first == '{' && last == '}') || (first == '"' && last == '"')) {
        try { return JSON_MAPPER.readValue(ByteArrayInputStream(value.toByteArray()), Any::class.java) }
        catch(e: Exception) {}
    }
    return value
}

private fun setValue(map: MutableMap<String,Any?>, name: String, value: Any?) {
    val key = name.urlDecode()
    val old = map[key]
    if (old == null)
        map[key] = parseValue(value)
    else if (old is MutableCollection<*>)
        (old as MutableCollection<Any?>).add(parseValue(value))
    else
        map[key] = mutableListOf<Any?>(old, parseValue(value))
}

private fun detectSeparator(input: InputStream): Char {
    val buffer = ByteArray(CSV_BUFFER_SIZE)
    input.mark(CSV_BUFFER_SIZE)
    input.read(buffer)
    input.reset()
    val txt = buffer.toString(Charset.forName(defaultCharset()))
    for (c in txt) {
        if (CSV_SEPARATORS.contains(c))
            return c
        if (c == '\n' || c == '\r')
            break
    }
    return CSV_SEPARATORS[0]
}

private fun compareFormats(f1: Format, f2: Format): Int {
    return f1.mimetype.compareTo(f2.mimetype)
}
