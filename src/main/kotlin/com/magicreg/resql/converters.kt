package com.magicreg.resql

import java.net.URI
import java.net.URL
import java.nio.charset.Charset
import org.apache.commons.beanutils.BeanMap
import java.io.*
import java.lang.reflect.*
import java.net.URLEncoder
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.Temporal
import java.util.*
import kotlin.reflect.*
import kotlin.reflect.full.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toKotlinDuration

/*************************************
 The difference between all the converters below and their counterparts in extensions.kt file is of the following:
 - converters should never fail and tries the conversion process, possibly returning inadequate data, but of the proper requested type
 - the only time the convert() function should fail is if no converter was found for the requested target type
 - the extensions functions try to convert to the requested type but will return null if conversion is not possible
 - you should always check for nullity after an extension conversion function call
 - an extension conversion function toX() should have a companion boolean function isX() to validate if conversion is possible
****************************************/

fun getConverter(type: KClass<*>): KFunction<Any?>? {
    return converters[type]
}

fun toByteArray(value: Any?): ByteArray {
    if (value is ByteArray)
        return value
    if (value == null)
        return byteArrayOf()
    if (value.isText())
        return value.toText().toByteArray(Charset.forName(defaultCharset()))
    if (value is Number)
        return value.toBytes()
    if (value is Boolean)
        return (if (value) 1 else 0).toByte().toBytes()
    if (value is Collection<*>) {
        val bytes = ByteArrayOutputStream()
        for (item in value)
            bytes.write(toByteArray(item))
        return bytes.toByteArray()
    }
    if (value::class.java.isArray)
        return toByteArray(ListArrayAdapter(value))
    return toString(value).toByteArray(Charset.forName(defaultCharset()))
}

fun toCharSequence(value: Any?): CharSequence {
    if (value is CharSequence)
        return value
    return toString(value)
}

fun toString(value: Any?): String {
    return value.toText()
}

fun toChar(value: Any?): Char {
    if (value is Char)
        return value
    if (value is CharSequence)
        return if (value.isEmpty()) ' ' else value[0]
    if (value is Number)
        return Char(value.toInt())
    if (value == null)
        return ' '
    if (value.isText())
        return toChar(value.toText())
    return toChar(toString(value))
}

fun toBoolean(value: Any?): Boolean {
    if (value is Boolean)
        return value
    if (value is Number)
        return value.toDouble() != 0.0
    if (value is CharSequence || value is Char) {
        val txt = value.toString().trim().lowercase()
        return txt.isNotBlank() && !FALSE_WORDS.contains(txt)
    }
    if (value == null)
        return false
    val iterator = value.toIterator()
    if (iterator != null) {
        if (!iterator.hasNext())
            return false
        val first = iterator.next()
        if (iterator.hasNext())
            return true
        return toBoolean(first)
    }
    return toBoolean(toString(value))
}

fun toByte(value: Any?): Byte {
    return toNumber(value).toByte()
}

fun toShort(value: Any?): Short {
    return toNumber(value).toShort()
}

fun toInt(value: Any?): Int {
    return toNumber(value).toInt()
}

fun toLong(value: Any?): Long {
    return toNumber(value).toLong()
}

fun toFloat(value: Any?): Float {
    return toNumber(value).toFloat()
}

fun toDouble(value: Any?): Double {
    return toNumber(value).toDouble()
}

fun toNumber(value: Any?): Number {
    val n = numericValue(value, null)
    if (n != null)
        return n
    val iterator = value.toIterator()
    if (iterator != null) {
        var count = 0
        while (iterator.hasNext())
            count++
        return count
    }
    return 1
}

fun toDateTime(value: Any?): LocalDateTime {
    if (value is LocalDateTime)
        return value
    if (value is Temporal)
        return LocalDateTime.from(value)
    if (value == null)
        return LocalDateTime.now()
    if (value is Date)
        return LocalDateTime.ofEpochSecond(value.time / 1000, (value.time % 1000).toInt() * 1000, toZoneOffset())
    if (value is Number)
        return LocalDateTime.ofEpochSecond(value.toLong(), 0, toZoneOffset("utc"))
    if (value is CharSequence)
        return value.toString().toTemporal()?.toDateTime() ?: LocalDateTime.now()
    // TODO: collection/array/map of year, month, day, hour, minute, second
    return LocalDateTime.parse(value.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))
}

fun toDate(value: Any?): LocalDate {
    if (value is LocalDate)
        return value
    if (value is Temporal)
        return LocalDate.from(value)
    if (value == null)
        return LocalDate.now()
    if (value is Date)
        return LocalDate.ofEpochDay(value.time / 86400000)
    if (value is Number)
        return LocalDate.ofEpochDay(value.toLong())
    if (value is CharSequence)
        return value.toString().toTemporal()?.toDate() ?: LocalDate.now()
    // TODO: collection/array/map of year, month, day
    return LocalDate.parse(value.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd"))
}

fun toTime(value: Any?): LocalTime {
    if (value is LocalTime)
        return value
    if (value is Temporal)
        return LocalTime.from(value)
    if (value == null)
        return LocalTime.now()
    if (value is Date)
        return LocalTime.ofNanoOfDay((value.time % 86400000)*1000)
    if (value is Number)
        return LocalTime.ofSecondOfDay(value.toLong())
    if (value is CharSequence)
        return value.toString().toTemporal()?.toTime() ?: LocalTime.now()
    // TODO: collection/array/map of hour, minute, second
    return LocalTime.parse(value.toString(), DateTimeFormatter.ofPattern("HH:mm:ss"))
}

fun toTemporal(value: Any?): Temporal {
    if (value is Temporal)
        return value
    if (value is java.sql.Time)
        return toTime(value)
    if (value is java.sql.Date)
        return toDate(value)
    if (value is java.sql.Timestamp)
        return toDateTime(value)
    return toDateTime(value)
}

fun toUtilDate(value: Any?): Date {
    if (value is Date)
        return value
    val temporal = toTemporal(value)
    if (temporal is LocalDateTime)
        return java.sql.Timestamp(temporal.toEpochSecond(toZoneOffset())*1000)
    if (temporal is LocalDate)
        return java.sql.Date(temporal.toEpochDay()*86400000)
    if (temporal is LocalTime)
        return java.sql.Time(temporal.toNanoOfDay()/1000)
    return Date(temporal.toDateTime().toEpochSecond(toZoneOffset())*1000)
}

fun toSqlDate(value: Any?): java.sql.Date {
    if (value is java.sql.Date)
        return value
    val temporal = toTemporal(value)
    if (temporal is LocalDateTime)
        return java.sql.Date(temporal.toEpochSecond(toZoneOffset())*1000)
    if (temporal is LocalDate)
        return java.sql.Date(temporal.toEpochDay()*86400000)
    if (temporal is LocalTime)
        return java.sql.Date(temporal.toNanoOfDay()/1000)
    return java.sql.Date(temporal.toDateTime().toEpochSecond(toZoneOffset())*1000)
}

fun toSqlTime(value: Any?): java.sql.Time {
    if (value is java.sql.Time)
        return value
    val temporal = toTemporal(value)
    if (temporal is LocalDateTime)
        return java.sql.Time(temporal.toEpochSecond(toZoneOffset())*1000)
    if (temporal is LocalDate)
        return java.sql.Time(temporal.toEpochDay()*86400000)
    if (temporal is LocalTime)
        return java.sql.Time(temporal.toNanoOfDay()/1000)
    return java.sql.Time(temporal.toDateTime().toEpochSecond(toZoneOffset())*1000)
}

fun toTimestamp(value: Any?): java.sql.Timestamp {
    if (value is java.sql.Timestamp)
        return value
    val temporal = toTemporal(value)
    if (temporal is LocalDateTime)
        return java.sql.Timestamp(temporal.toEpochSecond(toZoneOffset())*1000)
    if (temporal is LocalDate)
        return java.sql.Timestamp(temporal.toEpochDay()*86400000)
    if (temporal is LocalTime)
        return java.sql.Timestamp(temporal.toNanoOfDay()/1000)
    return java.sql.Timestamp(temporal.toDateTime().toEpochSecond(toZoneOffset())*1000)
}

fun toZoneOffset(value: Any? = null): ZoneOffset {
    if (value == null)
        return ZoneOffset.systemDefault().rules.getOffset(LocalDateTime.now())
    if (value is Number) {
        var n = value.toDouble()
        if (n < -86400 || n > 86400)
            n = (n / 3600) % 24
        else if (n < -24 || n > 24)
            n = (n / 3600) % 24
        return ZoneOffset.ofHoursMinutesSeconds(n.toInt(), (n * 60 % 60).toInt(), (n * 3600 % 60).toInt())
    }
    return ZoneOffset.of(value.toString())
}

fun toDuration(value: Any?): Duration {
    if (value is Duration)
        return value
    if (value is java.time.Duration)
        return value.toKotlinDuration()
    if (value is javax.xml.datatype.Duration)
        return value.getTimeInMillis(Date()).milliseconds
    val n = numericValue(value, null)
    if (n != null)
        return n.toDouble().milliseconds
    // TODO: check if it is a map or map entry collection of different time parts
    if (value == null)
        return (0.0).milliseconds
    return Duration.parseIsoString(value.toString())
}

fun toMap(value: Any?): Map<Any?,Any?> {
    if (value is Map<*,*>)
        return value as Map<Any?,Any?>
    val map = value.toMap()
    if (map != null)
        return map
    if (value is Collection<*>)
        return MapCollectionAdapter(value) as Map<Any?,Any?>
    if (value == null)
        return mutableMapOf()
    if (value::class.java.isArray)
        return MapCollectionAdapter(ListArrayAdapter(value)) as Map<Any?,Any?>
    val iterator = value.toIterator()
    if (iterator != null) {
        val list = toList(iterator)
        return list.toMap() ?: MapCollectionAdapter(list) as Map<Any?,Any?>
    }
    if (value is KClass<*>)
        return value.memberProperties.map { Pair(it.name, it.returnType) }.toMap()!!
    if (value is Class<*>)
        return toMap(value.kotlin)
    if (value.isText() || value is Number || value is Boolean)
        return mutableMapOf<Any?,Any?>("value" to value)
    return BeanMap(value)
}

fun toArray(value: Any?): Array<Any?> {
    if (value is Array<*>)
        return value as Array<Any?>
    return toList(value).toTypedArray<Any?>()
}

fun toCollection(value: Any?): Collection<Any?> {
    if (value is Collection<*>)
        return value
    return toList(value)
}

fun toSet(value: Any?): Set<Any?> {
    if (value is Set<*>)
        return value
    if (value is Collection<*>)
        return setOf(*value.toTypedArray())
    return setOf(*toList(value).toTypedArray())
}

fun toList(value: Any?): List<Any?> {
    if (value is List<*>)
        return value
    if (value is Collection<*>)
        return value.toList()
    val map = value.toMap()
    if (map != null)
        return map.entries.toList()
    if (value.isText()) {
        val txt = value.toText().trim()
        if (txt.isEmpty())
            return mutableListOf()
        if (txt.indexOf('\n') > 0)
            return txt.split("\n")
        return txt.split(",") // TODO: we could have other splitters like ; : = & | - / tab and blank
    }
    if (value == null)
        return mutableListOf()
    if (value::class.java.isArray)
        return ListArrayAdapter(value)
    val iterator = value.toIterator()
    if (iterator != null) {
        val list = mutableListOf<Any?>()
        while (iterator.hasNext())
            list.add(iterator.next())
        return list
    }
    return mutableListOf(value)
}

fun toNamespace(value: Any?): Namespace {
    if (value is Namespace)
        return value
    var prefix: String? = null
    var uri: String? = null
    var mapping: MutableMap<String,Any?>? = null
    var readOnly: Boolean? = null
    val args: Collection<Any?> = if (value is Collection<*>)
        value
    else if (value is Array<*>)
        value.toList()
    else if (value == null)
        emptyList()
    else
        listOf(value)
    for (arg in args) {
        if (arg is Boolean)
            readOnly = arg
        else if (arg is URI)
            uri = arg.toString()
        else if (arg is URL || arg is File)
            uri = arg.toUri().toString()
        else if (arg is CharSequence) {
            val goturi = arg.toUri()
            if (goturi == null)
                prefix = arg.toString()
            else
                uri = goturi.toString()
        }
        else if (arg is MutableMap<*,*>)
            mapping = arg as MutableMap<String,Any?>
        else if (arg is Map<*,*>)
            mapping = (arg as Map<String,Any?>).toMutableMap()
    }
    if (args.size == 1 && prefix != null)
        return getContext().namespace(prefix) ?: throw RuntimeException("Invalide namespace: $prefix")
    if (mapping == null && uri != null) {
        val value = URI(uri).get()
        if (value is Namespace)
            return value
        if (value is MutableMap<*,*>)
            mapping = value as MutableMap<String,Any?>
        else if (value is Map<*,*>)
            mapping = (value as Map<String,Any?>).toMutableMap()
    }
    if (mapping != null) {
        if (prefix == null)
            prefix = mapping.remove("prefix")?.toString()
        if (uri == null)
            uri = mapping.remove("uri")?.toString()
        if (readOnly == null)
            readOnly = toBoolean(mapping.remove("readOnly"))
    }
    return MemoryNamespace(prefix?:"", uri?:"http://localhost/", readOnly?:false).populate(mapping?:mutableMapOf())
}

fun toURI(value: Any?): URI {
    val uri = value.toUri()
    if (uri != null)
        return uri
    if (value == null)
        return URI("resql:null")
    if (value.isText()) {
        val txt = value.toText()
        if (getContext().hasName(txt))
            return URI("resql:$txt")
    }
    val json = getFormat("json")!!.encodeText(value)
    return URI("data:application/json,${URLEncoder.encode(json, defaultCharset())}")
}

fun toClass(value: Any?): KClass<*> {
    if (value is KClass<*>)
        return value
    if (value is Class<*>)
        return value.kotlin
    if (value is KType)
        return value.javaClass.kotlin
    if (value is CharSequence) {
        val txt = value.toString().trim()
        val c = txt.toClass()
        if (c != null)
            return c
    }
    if (value == null)
        return Void::class
    return value::class
}

fun toFunction(value: Any?): Function {
    return if (value is Function)
        value
    else if (value is KFunction<*>)
        FunctionWrapper(value.name, "") { value.call(*it.toTypedArray()) }
    else if (value is Method) {
        val isStatic = Modifier.isStatic(value.modifiers)
        FunctionWrapper(value.name, "") {
            val instance = if (isStatic || it.isEmpty()) null else it[0]
            val params = if (isStatic || it.isEmpty()) it else it.subList(1, it.size)
            value.invoke(instance, *params.toTypedArray())
        }
    }
    else if (value is Constructor<*>)
        FunctionWrapper(value.name, "") { value.newInstance(*it.toTypedArray()) }
    else
        FunctionWrapper(value.toString(), "") { value }
}

fun toExpression(value: Any?): Expression {
    if (value is Expression)
        return value
    if (value.isText())
        return value.toText().toExpression()
    if (value == null)
        return Expression()
    val tokens = value.toCollection().map {
        if (it is Property)
            it.getValue()
        else if (it is Map.Entry<*,*>)
            it.value
        else
            it
    }
    return compileExpression(tokens)
}

private val FALSE_WORDS = "false,no,0,none,empty".split(",")
private val converters = initConverters()

private fun initConverters(): MutableMap<KClassifier, KFunction<Any?>> {
    val map = mutableMapOf<KClassifier, KFunction<*>>()
    for (func in arrayOf(
        ::toString,
        ::toCharSequence,
        ::toByteArray,
        ::toChar,
        ::toBoolean,
        ::toByte,
        ::toShort,
        ::toInt,
        ::toLong,
        ::toFloat,
        ::toDouble,
        ::toNumber,
        ::toDateTime,
        ::toDate,
        ::toTime,
        ::toTemporal,
        ::toUtilDate,
        ::toSqlDate,
        ::toSqlTime,
        ::toTimestamp,
        ::toZoneOffset,
        ::toDuration,
        ::toMap,
        ::toList,
        ::toSet,
        ::toCollection,
        ::toArray,
        ::toNamespace,
        ::toURI,
        ::toClass,
        ::toFunction,
        ::toExpression
    )) {
        map[func.returnType.classifier!!] = func
    }
    return map
}

private fun numericValue(value: Any?, defaultValue: Number?): Number? {
    if (value is Number)
        return value
    if (value is Boolean)
        return (if (value) 1 else 0).toByte()
    if (value == null)
        return 0
    if (value.isText()) {
        val n = value.toText().toDoubleOrNull()
        if (n != null)
            return n
    }
    return defaultValue
}
