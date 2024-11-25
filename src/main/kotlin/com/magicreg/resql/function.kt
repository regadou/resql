package com.magicreg.resql

import org.apache.commons.beanutils.BeanMap
import java.net.URI
import java.util.*
import kotlin.collections.ArrayList
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf

fun getFunctionNames(): Collection<String> {
    return FUNCTIONS.keys
}

fun getFunctionByName(name: String): Function? {
    return FUNCTIONS[name.lowercase()]
}

fun getFunctionBySymbol(symbol: String): Function? {
    return SYMBOLS[symbol]
}

class FunctionWrapper(
    override val id: String,
    override val symbol: String,
    override val lambda: (List<Any?>) -> Any?
): Function {
    override fun toString(): String { return "$id()" }
}

fun and_func(args: List<Any?>): Any? {
    if (args.isEmpty())
        return false
    for (arg in args) {
        if (!toBoolean(arg.resolve()))
            return false
    }
    return true
}

fun or_func(args: List<Any?>): Any? {
    if (args.isEmpty())
        return true
    for (arg in args) {
        if (toBoolean(arg.resolve()))
            return true
    }
    return false
}

fun equal_func(args: List<Any?>): Any? {
    var minArgs = minimumResolvedArgs(args)
    for (i in 1 until minArgs.size) {
        if (compare(minArgs[i-1], minArgs[i]) != 0)
            return false
    }
    return true
}

fun not_equal_func(args: List<Any?>): Any? {
    var minArgs = minimumResolvedArgs(args)
    for (i in 1 until minArgs.size) {
        if (compare(minArgs[i-1], minArgs[i]) == 0)
            return false
    }
    return true
}

fun less_func(args: List<Any?>): Any? {
    var minArgs = minimumResolvedArgs(args)
    for (i in 1 until minArgs.size) {
        if (compare(minArgs[i-1], minArgs[i]) >= 0)
            return false
    }
    return true
}

fun not_more_func(args: List<Any?>): Any? {
    var minArgs = minimumResolvedArgs(args)
    for (i in 1 until minArgs.size) {
        if (compare(minArgs[i-1], minArgs[i]) > 0)
            return false
    }
    return true
}

fun more_func(args: List<Any?>): Any? {
    var minArgs = minimumResolvedArgs(args)
    for (i in 1 until minArgs.size) {
        if (compare(minArgs[i-1], minArgs[i]) <= 0)
            return false
    }
    return true
}

fun not_less_func(args: List<Any?>): Any? {
    var minArgs = minimumResolvedArgs(args)
    for (i in 1 until minArgs.size) {
        if (compare(minArgs[i-1], minArgs[i]) < 0)
            return false
    }
    return true
}

fun in_func(args: List<Any?>): Any? { return null }
fun not_in_func(args: List<Any?>): Any? { return null }
fun between_func(args: List<Any?>): Any? { return null }
fun not_between_func(args: List<Any?>): Any? { return null }
fun match_func(args: List<Any?>): Any? { return null }
fun not_match_func(args: List<Any?>): Any? { return null }
fun add_func(args: List<Any?>): Any? { return null }
fun remove_func(args: List<Any?>): Any? { return null }
fun multiply_func(args: List<Any?>): Any? { return null }
fun divide_func(args: List<Any?>): Any? { return null }
fun modulo_func(args: List<Any?>): Any? { return null }
fun exponent_func(args: List<Any?>): Any? { return null }
fun root_func(args: List<Any?>): Any? { return null }
fun logarithm_func(args: List<Any?>): Any? { return null }

fun join_func(args: List<Any?>): Any? {
    val list = mutableListOf<Any?>()
    for (arg in args)
        list.add(arg.resolve())
    return list
}

fun do_func(args: List<Any?>): Any? {
    var value: Any? = null
    for (arg in args)
        value = toExpression(arg).execute().resolve()
    return value
}

fun have_func(args: List<Any?>): Any? {
    if (args.isEmpty())
        return null
    var value = args[0].resolve()
    for (i in 1..<args.size)
        value = value.property(args[i].resolve().toText())
    return value
}

fun of_func(args: List<Any?>): Any? {
    if (args.isEmpty())
        return null
    var value = args[args.size-1].resolve()
    for (i in args.size-2 downTo 0)
        value = value.property(args[i].resolve())
    return value
}

fun with_func(args: List<Any?>): Any? { return null }
fun sort_func(args: List<Any?>): Any? { return null }

fun each_func(args: List<Any?>): Any? {
    var value: Any? = null
    if (args.size < 3)
        return value
    val items = args[0].resolve().toCollection()
    val key = args[1].resolve().toText()
    val cx = getContext().childContext(UUID.randomUUID().toString(), emptyMap())
    for (index in 2 until args.size) {
        val exp = toExpression(args[index])
        for (item in items) {
            cx.setValue(key, item)
            value = exp.resolve()
        }
    }
    cx.close(true)
    return value
}

fun schema_func(args: List<Any?>): Any? {
    val src = when (args.size) {
        0 -> emptyList<Any?>()
        1 -> args[0].resolve().toCollection()
        else -> {
            val all = mutableListOf<Any?>()
            args.forEach { all.addAll(it.resolve().toCollection()) }
            all
        }
    }
    val schema = mutableMapOf<String,KClass<*>>()
    for ((index, item) in src.withIndex())
        extractSchema(schema, item, index)
    return schema
}

fun print_func(args: List<Any?>): Any? {
    return print(args.joinToString("") { it.toText(true) })
}

fun get_func(args: List<Any?>): Any? { return restExecute(RestMethod.GET, args) }
fun post_func(args: List<Any?>): Any? { return restExecute(RestMethod.POST, args) }
fun put_func(args: List<Any?>): Any? { return restExecute(RestMethod.PUT, args) }
fun delete_func(args: List<Any?>): Any? { return restExecute(RestMethod.DELETE, args) }

private val FUNCTIONS = initFunctions()
private val SYMBOLS = initSymbols()

private class ArgList(vararg args: Any?): ArrayList<Any?>() {
    init {addAll(args.toList())}
}

private fun initFunctions(): Map<String, Function> {
    val map = mutableMapOf<String, Function>()
    for (entries in arrayOf(Type.entries, RestMethod.entries, CompareOperator.entries, LogicOperator.entries, MathOperator.entries, MiscOperator.entries))
        entries.forEach { map[it.id] = it }
    return map
}

private fun initSymbols(): Map<String, Function> {
    val map = mutableMapOf<String, Function>()
    for (entries in arrayOf(Type.entries, RestMethod.entries, CompareOperator.entries, LogicOperator.entries, MathOperator.entries, MiscOperator.entries))
        entries.forEach { if (it.symbol.isNotBlank()) map[it.symbol] = it }
    return map
}

private fun minimumResolvedArgs(args: List<Any?>): List<Any?> {
    return  when (args.size) {
        0 -> listOf(null, null)
        1 -> listOf(args[0].resolve(), null)
        else -> args.map { it.resolve() }
    }
}

private fun compare(v1: Any?, v2: Any?): Int {
    if (v1 == null || v2 == null) {
        if (v1 != null)
            return 1
        if (v2 != null)
            return -1
        return 0
    }
    if (v1 is Collection<*> || v1::class.java.isArray || v2 is Collection<*> || v2::class.java.isArray) {
        val c1 = v1.toCollection()
        val c2 = v2.toCollection()
        if (c1.size != c2.size)
            return c1.size - c2.size
        for ((index,item) in c1.withIndex()) {
            val dif = compare(item, c2[index])
            if (dif != 0)
                return dif
        }
        return 0
    }
    if (v1.isMappable() || v2.isMappable() ) {
        val m1 = v1.toMap() ?: toMap(v1)
        val m2 = v2.toMap() ?: toMap(v2)
        if (m1.size != m2.size)
            return m1.size - m2.size
        val k1 = m1.keys.map { it.toText() }.sorted()
        val k2 = m2.keys.map { it.toText() }.sorted()
        var dif = k1.joinToString(",").normalize().compareTo(k2.joinToString(",").normalize())
        if (dif != 0)
            return dif
        for (index in k1.indices) {
            dif = compare(m1[k1[index]], m2[k2[index]])
            if (dif != 0)
                return dif
        }
        return 0
    }
    if (v1 is Number || v1 is Boolean || v2 is Number || v2 is Boolean)
        return toDouble(v1).compareTo(toDouble(v2))
    return toString(v1).normalize().compareTo(toString(v2).normalize())
}

private fun restExecute(method: RestMethod, args: List<Any?>): Any? {
    var errorMode = ErrorMode.SILENT
    var uri: URI? = null
    var query: Query? = null
    var value: Any? = null
    if (args.isEmpty())
        return errorMode.wrap(StatusCode.OK, null)
    for (arg in args) {
        val item = if (arg is Expression) arg.resolve() else arg
        if (item == null)
            continue
        else if (item is ErrorMode)
            errorMode = item
        else if (item is Query)
            query = item
        else if (item is Filter)
            query = Query(item)
        else if (uri == null) {
            if (item is URI)
                uri = item
            else if (item.isText())
                uri = URI(item.toText())
            else if (item is Property) {
                val parent = item.instance.resolve()
                if (parent is Context)
                    uri = URI(item.key)
                else if (parent is Namespace)
                    uri = URI(parent.prefix+":/"+item.key)
            }
            else
                uri = item.toUri()
            if (uri == null)
                return errorMode.wrap(StatusCode.BadRequest, "Invalid URI value: $item")
        }
        else if (value == null)
            value = item.resolve()
        else if (value is ArgList)
            value.add(item.resolve())
        else
            value = ArgList(value, item.resolve())
    }
    if (uri == null)
       return errorMode.wrap(when (method) {
            RestMethod.GET -> args.simplify().resolve()
            RestMethod.POST, RestMethod.PUT -> null
            RestMethod.DELETE -> false
       })
    if (uri.scheme != null)
        return errorMode.wrap(resolveUri(uri, method, mapOf(), value))
    var parts = if (uri.path == null) emptyList() else uri.path.split("/").filter { it.isNotBlank() }
    val ns = if (parts.isEmpty())
        findNamespace(method, uri.path) ?: return errorMode.wrap(null)
    else
        findNamespace(method, uri.path, parts[0]) ?: return errorMode.wrap(StatusCode.NotFound, null)
    if (ns.readOnly && method != RestMethod.GET)
        return errorMode.wrap(StatusCode.MethodNotAllowed, if (method == RestMethod.DELETE) false else null)
    if (!ns.keepUrlEncoding)
        parts = parts.map { it.urlDecode() }
    if (!uri.query.isNullOrBlank())
        query = Query(uri.query)
    if (query != null && method == RestMethod.GET) {
        val maxPageSize = getContext().configuration().maxPageSize
        if (query.page.size > maxPageSize)
            query.addQueryPart(".page", mapOf("size" to maxPageSize))
    }
    if (value == null && method == RestMethod.GET)
        value = defaultContentType()
    val path = if (ns.prefix == "") parts else parts.subList(1, parts.size)
    return errorMode.wrap(ns.execute(method, path, query ?: Query(), value))
}

private fun findNamespace(method: RestMethod, path: String?, name: String = ""): Namespace? {
    val cx = getContext()
    val routes = cx.configuration().routes
    var ns = routes[name]
    if (ns != null)
        return ns
    ns = routes[""]
    if (ns != null && (name.isEmpty() || ns.hasName(name)))
        return ns
    if (path != null && path.startsWith(name) && (cx.hasName(name) || method == RestMethod.PUT))
        return cx
    return null
}

private fun extractSchema(schema: MutableMap<String, KClass<*>>, value: Any?, index: Int) {
    val map = value.toMap()?.mapKeys { it.toText() } ?: BeanMap(value).mapKeys { it.toText() }
    val keys = map.keys
    for (key in keys) {
        if (index == 0 || schema.keys.contains(key))
            schema[key] = getValueType(map[key], schema[key] ?: Void::class)
    }
    val keysToDelete = mutableListOf<String>()
    for (key in schema.keys) {
        if (!keys.contains(key))
            keysToDelete.add(key)
    }
    for (key in keysToDelete)
        schema.remove(key)
}

private fun getValueType(value: Any?, oldType: KClass<*>): KClass<*> {
    if (value == null)
        return oldType
    val newType = value::class
    if (oldType == Void::class)
        return newType
    if (oldType.isSubclassOf(newType))
        return newType
    if (newType.isSubclassOf(oldType))
        return oldType
    val parents = oldType.parents().toMutableList()
    parents.retainAll(newType.parents())
    return if (parents.isEmpty()) Any::class else parents[0]
}
