package com.magicreg.resql

fun getFunction(name: String): Function? {
    return FUNCTIONS[name.lowercase()]
}

class FunctionWrapper(
    override val id: String,
    override val symbol: String,
    override val function: (List<Any?>) -> Any?
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

fun execute_func(args: List<Any?>): Any? {
    var value: Any? = null
    for (arg in args)
        value = toExpression(arg).execute()
    return value
}

fun property_func(args: List<Any?>): Any? {
    var value = args[0]
    for (arg in args.subList(1,args.size))
        value = value.property(arg.toText())
    return value
}

fun all_func(args: List<Any?>): Any? {
    val cx = getContext()
    return when (args.size) {
        0 -> (cx.configuration().routes.keys.map { "$it:" } + cx.names + FUNCTIONS.keys.map{"$it()"}).sorted()
        1 -> args[0].resolve().property("keys")
        else -> Filter().addConditions(args.subList(1, args.size)).filter(args[0].resolve())
    }
}

private val FUNCTIONS = initFunctions()

private fun initFunctions(): Map<String, Function> {
    val map = mutableMapOf<String, Function>()
    for (entries in arrayOf(Type.entries, UriMethod.entries, CompareOperator.entries, LogicOperator.entries, MathOperator.entries, MiscOperator.entries))
        entries.forEach { map[it.id] = it }
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
