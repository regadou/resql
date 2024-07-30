package com.magicreg.resql

import org.apache.commons.beanutils.BeanMap

class Query(query: Map<String,Any?>) {
    private val data = parseQueryData(query)
    val source: Collection<String> get() { return stringCollection(data[sourceKey]) }
    val keys: Collection<String> get() { return stringCollection(data[keysKey]) }
    val filter: Filter get() { return data[filterKey] as Filter }
    val group: Collection<String> get() { return stringCollection(data[groupKey]) }
    val sort: Collection<String> get() { return stringCollection(data[sortKey]) }
    val page: Page get() { return data[pageKey] as Page }
    val isEmpty: Boolean get() {
        return source.isEmpty() && keys.isEmpty() && filter.isEmpty && group.isEmpty() && sort.isEmpty() && page.isEmpty()
    }

    fun addQueryPart(key: String, value: Any?): Query {
        if (key.isBlank() || value == null)
            return this
        else if (key[0] == '.')
            setQueryData(data, mapOf(key to  value))
        else if (value is Collection<*>) {
            for (item in value)
                filter.mapCondition(toMap(item).mapKeys { it.toString() })
        }
        else if (value is Array<*>)
            addQueryPart(key, value.toList())
        else if (value is Map<*,*>) {
            for (entry in value.entries)
                filter.and(key, entry.key.toString(), entry.value)
        }
        else if (value is Map.Entry<*,*>)
            filter.and(key, value.key.toString(), value.value)
        else
            filter.and(key, CompareOperator.EQUAL, value)
        return this
    }

    override fun toString(): String {
        val txt = getFormat("json")!!.encodeText(data)
        return "Query($txt)"
    }
}

class Filter(key: String? = null, compare: Any? = null, value: Any? = null): Iterable<Map<String,Any?>> {
    private val conditions: MutableList<MutableMap<String,Any?>> = mutableListOf()
    init {
        if (key != null && compare != null) {
            addCondition(LogicOperator.AND, key, compare.toCompareOperator(true)!!, value)
        }
    }

    val isEmpty: Boolean get() {
        for (condition in conditions) {
            if (condition.isNotEmpty())
                return false
        }
        return true
    }

    override fun toString(): String {
        val txt = getFormat("json")!!.encodeText(conditions)
        return "Filter($txt)"
    }

    override fun iterator(): Iterator<Map<String, Any?>> {
        return conditions.iterator()
    }

    fun filter(container: Any?): List<Any?> {
        if (conditions.isEmpty())
            return container.toCollection()
        if (container is Map<*,*>)
            return filterItems(listOf(container).iterator(), this)
        if (container.isText())
            return filterItems(container.toText().split("\n").iterator(), this)
        val iterator = container.toIterator()
        if (iterator != null)
            return filterItems(iterator, this)
        if (container == null)
            return emptyList()
        if (container is Expression)
            return filter(container.resolve())
        return filterItems(listOf(container).iterator(), this)
    }

    fun and(key: String, compare: String, value: Any?): Filter {
        return addCondition(LogicOperator.AND, key, compare.toCompareOperator(true)!!, value)
    }

    fun and(key: String, compare: CompareOperator, value: Any?): Filter {
        return addCondition(LogicOperator.AND, key, compare, value)
    }

    fun or(key: String, compare: String, value: Any?): Filter {
        return addCondition(LogicOperator.OR, key, compare.toCompareOperator(true)!!, value)
    }

    fun or(key: String, compare: CompareOperator, value: Any?): Filter {
        return addCondition(LogicOperator.OR, key, compare, value)
    }

    fun addCondition(logic: LogicOperator, key: String, compare: CompareOperator, value: Any?): Filter {
        if (conditions.isEmpty())
            conditions.add(mutableMapOf())
        else {
            when (logic) {
                LogicOperator.OR -> conditions.add(mutableMapOf())
                LogicOperator.AND -> {}
            }
        }
        conditions[conditions.size - 1][key] = mapOf(compare.symbol to value)
        return this
    }

    fun mapCondition(condition: Map<String,Any?>): Filter {
        if (condition.isNotEmpty()) {
            val map = mutableMapOf<String, Any?>()
            map.putAll(condition)
            conditions.add(map)
        }
        return this
    }
}

class Page(var index: Int = 0, var size: Int = 0) {
    val range: IntRange get() { return IntRange(index*size, (index+1)*size-1) }
    override fun toString(): String {
        return "Page(index=$index, size=$size)"
    }
}

private const val queryKey = "query"
private const val sourceKey = "source"
private const val keysKey = "keys"
private const val filterKey = "filter"
private const val groupKey = "group"
private const val sortKey = "sort"
private const val pageKey = "page"

private fun parseQueryData(query: Map<String,Any?>): MutableMap<String,Any?> {
    val data = mutableMapOf<String,Any?>(
        sourceKey to mutableSetOf<String>(),
        keysKey to mutableListOf<String>(),
        filterKey to Filter(),
        groupKey to mutableListOf<String>(),
        sortKey to mutableListOf<String>(),
        pageKey to Page()
    )
    setQueryData(data, query)
    return data
}

private fun setQueryData(data: MutableMap<String, Any?>, query: Map<String,Any?>) {
    val map = mutableMapOf<String,Any?>()
    val filter = data[filterKey] as Filter
    for (key in query.keys) {
        val value = query[key]
        if (key.isBlank() || value == null)
            continue
        else if (key[0] == '.')
            when (key.substring(1)) {
                sourceKey -> stringCollection(data[sourceKey]).addAll(stringCollection(value))
                keysKey -> stringCollection(data[keysKey]).addAll(stringCollection(value))
                groupKey -> stringCollection(data[groupKey]).addAll(stringCollection(value))
                sortKey -> stringCollection(data[sortKey]).addAll(stringCollection(value))
                pageKey -> setPageData(data[pageKey] as Page, value)
                filterKey -> setFilterData(filter, value)
                queryKey -> setQueryData(data, toMap(value).mapKeys{it.toString()})
                else -> throw RuntimeException("Invalid query key: .$key")
            }
        else
            map[key] = value
    }
    filter.mapCondition(map)
}

private fun setFilterData(filter: Filter, data: Any?) {
    if (data is Collection<*>) {
        for (item in data)
            setFilterData(filter, item)
    }
    else if (data is Array<*>) {
        for (item in data)
            setFilterData(filter, item)
    }
    else if (data is Map<*,*>)
        filter.mapCondition(data as Map<String,Any?>)
}

private fun filterItems(items: Iterator<Any?>, filter: Filter): List<Any?> {
    val matches = mutableListOf<Any?>()
    while (items.hasNext()) {
        val item = items.next()
        if (satisfyConditions(item, filter.iterator()))
            matches.add(item)
    }
    return matches
}

private fun satisfyConditions(item: Any?, conditions: Iterator<Any?>): Boolean {
    if (!conditions.hasNext())
        return true
    val map = toMap(item)
    while (conditions.hasNext()) {
        if (satisfyCondition(map, toMap(conditions.next()) as Map<String, Any?>))
            return true
    }
    return false
}

private fun satisfyCondition(item: Any?, condition: Map<String, Any?>): Boolean {
    for (entry in condition.entries) {
        val exp = entryExpression(entry)
        val key = exp.parameters[0].resolve()?.toText() ?: ""
        val value = exp.parameters[1]
        val params = listOf(item.property(key).getValue(), value)
        if (!toBoolean(exp.function!!.execute(params)))
            return false
    }
    return true
}

private fun entryExpression(entry: Map.Entry<String,Any?>): Expression {
    var operator = CompareOperator.EQUAL
    var value = entry.value
    if (value is Map<*,*> && value.size == 1) {
        val valueEntry = value.entries.iterator().next()
        val op = valueEntry.key.toCompareOperator(true)
        if (op != null) {
            operator = op
            value = valueEntry.value
        }
    }
    return Expression(operator, listOf(entry.key, value))
}

private fun stringCollection(value: Any?): MutableCollection<String> {
    if (value == null)
        return mutableListOf<String>()
    if (value is MutableCollection<*>)
        return value as MutableCollection<String>
    if (value is Collection<*>)
        return value.toMutableList() as MutableCollection<String>
    if (value is Array<*>)
        return value.toMutableList() as MutableCollection<String>
    if (value is Map<*,*>)
        return value.keys.toMutableList() as MutableCollection<String>
    if (value is CharSequence)
        return if (value.isBlank()) mutableListOf<String>() else value.toString().split(",").map{it.trim()}.toMutableList()
    return mutableListOf(value.toString())
}

private fun setPageData(page: Page, value: Any?) {
    if (value == null)
        return
    else if (value is Map<*, *>) {
        val index = value["index"]?.toString()?.toIntOrNull()
        if (index != null)
            page.index = index
        val size = value["size"]?.toString()?.toIntOrNull()
        if (size != null)
            page.size = size
    }
    else if (value is Collection<*>) {
        val it = value.iterator()
        val index = if (it.hasNext()) it.next() else null
        val size = if (it.hasNext()) it.next() else null
        setPageData(page, mapOf("index" to index, "size" to size))
    }
    else if (value is Array<*>)
        setPageData(page, value.toList())
    else if (value is Number)
        page.index = value.toInt()
    else if (value is Boolean)
        page.index = if (value) 1 else 0
    else if (value.isText()) {
        val n = value.toText().toIntOrNull()
        if (n != null)
            page.index = n
    }
    else
        setPageData(page, BeanMap(value))
}