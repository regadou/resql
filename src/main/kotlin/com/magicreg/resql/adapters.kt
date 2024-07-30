package com.magicreg.resql

import java.util.*

class MapAdapter<K, V>(
    private val keyLister: () -> Set<K>,
    private val getter: (K) -> (V),
    private val setter: ((K, V) -> Unit)? = null,
    private val remover: ((K) -> Unit)? = null
): MutableMap<K, V> {
    override val keys: MutableSet<K>
        get() = keyLister().toMutableSet()
    override val values: MutableCollection<V>
        get() = keyLister().map { getter(it) }.toMutableList()
    override val entries: MutableSet<MutableMap.MutableEntry<K, V>>
        get() = keyLister().map { java.util.AbstractMap.SimpleEntry(it, getter(it)) }.toMutableSet()
    override val size: Int get() {
        return keyLister().size
    }

    override fun isEmpty(): Boolean {
        return keyLister().isEmpty()
    }

    override fun containsKey(key: K): Boolean {
        return keyLister().contains(key)
    }

    override fun containsValue(value: V): Boolean {
        return values.contains(value as V)
    }

    override operator fun get(key: K): V? {
        return getter(key)
    }

    override fun put(key: K, value: V): V? {
        if (setter == null)
            throw UnsupportedOperationException("Not supported")
        val old: V = getter(key)
        setter?.let { it(key, value) }
        return old
    }

    override fun putAll(m: Map<out K, V>) {
        if (setter == null)
            throw UnsupportedOperationException("Not supported")
        for (key in m.keys) {
            val value = m[key]
            if (value != null)
                put(key, value)
            else if (remover != null)
                remove(key)
        }
    }

    override fun remove(key: K): V? {
        if (remover == null)
            throw UnsupportedOperationException("Not supported")
        val old: V = getter(key)
        remover?.let { it(key) }
        return old
    }

    override fun clear() {
        if (remover == null)
            throw UnsupportedOperationException("Not supported")
        for (key in keyLister())
            remover?.let { it(key) }
    }
}

class ArrayIterator(private val array: Any): Iterator<Any?> {
    var index: Int = 0
    var size = java.lang.reflect.Array.getLength(array)
    init {
        if (!array::class.java.isArray)
            throw RuntimeException("Value is not an array: $array")
    }

    override fun hasNext(): Boolean {
        return index < size
    }

    override fun next(): Any? {
        val value = java.lang.reflect.Array.get(array, index)
        index++
        return value
    }

}

class ClassIterator(private var currentClass: Class<*>? = Void::class.java) : Iterator<Class<*>> {
    private var interfaces: MutableList<Class<*>>? = null
    private var at = 0

    override fun hasNext(): Boolean {
        //TODO: each interface can extends parent interfaces
        if (interfaces != null) {
            if (at < interfaces!!.size) return true
            interfaces = null
            currentClass = getSuperClass(currentClass)
        }
        return currentClass != null
    }

    override fun next(): Class<*> {
        //TODO: each interface can extends parent interfaces
        if (interfaces != null) {
            if (at < interfaces!!.size) return interfaces!![at++]
            interfaces = null
            currentClass = getSuperClass(currentClass)
        }
        if (currentClass == null) throw NoSuchElementException("No more element to iterate over")
        interfaces = ArrayList()
        getInterfaces(currentClass!!.interfaces)
        at = 0
        return currentClass!!
    }

    private fun getSuperClass(src: Class<*>?): Class<*>? {
        return if (src == null) null else if (src.isArray) {
            var comp = src.componentType
            //TODO: we should check if component is an interface and loop through all implemented interfaces
            comp = comp!!.superclass
            if (comp == null) Any::class.java else java.lang.reflect.Array.newInstance(comp, 0)::class.java
        } else src.superclass
    }

    private fun getInterfaces(src: Array<Class<*>>) {
        for (type in src) {
            interfaces!!.add(type)
            getInterfaces(type.interfaces)
        }
    }
}

class ListArrayAdapter(private val array: Any): AbstractMutableList<Any?>() {
    override val size: Int get() = java.lang.reflect.Array.getLength(array)

    override fun add(element: Any?): Boolean {
        return false
    }

    override fun add(index: Int, element: Any?) {}

    override fun addAll(elements: Collection<Any?>): Boolean {
        return false
    }

    override fun clear() {}

    override fun get(index: Int): Any? {
        return java.lang.reflect.Array.get(array, index)
    }

    override fun remove(element: Any?): Boolean {
        return false
    }

    override fun removeAll(elements: Collection<Any?>): Boolean {
        return false
    }

    override fun removeAt(index: Int): Any? {
        return null
    }

    override fun removeRange(fromIndex: Int, toIndex: Int) {}

    override fun set(index: Int, element: Any?): Any? {
        val old = java.lang.reflect.Array.get(array, index)
        java.lang.reflect.Array.set(array, index, element)
        return old
    }

    override fun retainAll(elements: Collection<Any?>): Boolean {
        return false
    }
}

class MapCollectionAdapter(private val src: Collection<Any?>): AbstractMutableMap<String,Any?>() {
    override val entries
        get() = mutableSetOf<MutableMap.MutableEntry<String,Any?>>(MapEntry("size", src.size, true))
    override val size
        get() = src.size
    override val keys
        get() = mutableSetOf<String>("size")
    override val values
        get() = mutableSetOf<Any?>(src.size)

    override fun get(key: String): Any? {
        if (key == "size")
            return src.size
        val index = key.toIntOrNull()
        if (index != null && index >= 0 && index < src.size) {
            if (src is List<*>)
                return src[index]
            var i = 0
            val it = src.iterator()
            while (i < index) {
                it.next()
                i++
            }
            return it.next()
        }
        return null
    }

    override fun put(key: String, value: Any?): Any? {
        val index = key.toInt()
        val old = get(key)
        if (src is MutableList<*> && index != null && index >= 0 && index < src.size)
            (src as MutableList<Any?>)[index] = value
        return old
    }
}

class MultiMap(vararg originalMaps: MutableMap<String,Any?>) : MutableMap<String, Any?> {
    private val mapsList = mutableListOf<MutableMap<String,Any?>>()
    init {
        for (map in originalMaps) {
            if (map != null)
                this.mapsList.add(map)
        }
    }

    fun add(map: MutableMap<String, Any?>): MultiMap {
        mapsList.add(map)
        return this
    }

    override val entries: MutableSet<MutableMap.MutableEntry<String, Any?>> get() {
        val set = mutableSetOf<MutableMap.MutableEntry<String, Any?>>()
        for (map in mapsList)
            set.addAll(map.entries)
        return set
    }

    override val keys: MutableSet<String> get() {
        val set = mutableSetOf<String>()
        for (map in mapsList)
            set.addAll(map.keys)
        return set
    }

    override val size: Int get() {
        var count = 0
        for (map in mapsList)
            count += map.size
        return count
    }

    override val values: MutableCollection<Any?> get() {
        val list = mutableListOf<Any?>()
        for (map in mapsList)
            list.addAll(map.values)
        return list
    }

    override fun remove(key: String): Any? {
        val list = mutableListOf<Any?>()
        for (map in mapsList) {
            if (map.containsKey(key))
                list.add(map.remove(key))
        }
        return list.simplify()
    }

    override fun putAll(from: Map<out String, Any?>) {
        if (mapsList.isEmpty())
            mapsList.add(mutableMapOf())
        for (key in from.keys) {
            var found = false
            for (map in mapsList) {
                if (map.containsKey(key)) {
                    map[key] = from[key]
                    found = true
                    break
                }
            }
            if (!found)
                mapsList[mapsList.size-1][key] = from[key]
        }
    }

    override fun put(key: String, value: Any?): Any? {
        if (mapsList.isEmpty())
            mapsList.add(mutableMapOf())
        for (map in mapsList) {
            if (map.containsKey(key))
                return map.put(key, value)
        }
        return mapsList[mapsList.size-1].put(key, value)
    }

    override fun clear() {
        for (map in mapsList)
            map.clear()
    }

    override fun get(key: String): Any? {
        for (map in mapsList) {
            if (map.containsKey(key))
                return map[key]
        }
        return null
    }

    override fun containsValue(value: Any?): Boolean {
        for (map in mapsList) {
            if (map.containsValue(value))
                return true
        }
        return false
    }

    override fun containsKey(key: String): Boolean {
        for (map in mapsList) {
            if (map.containsKey(key))
                return true
        }
        return false
    }

    override fun isEmpty(): Boolean {
        for (map in mapsList) {
            if (map.isNotEmpty())
                return false
        }
        return true
    }

    override fun toString(): String {
        val map = TreeMap<String,Any?>()
        map.putAll(this)
        return map.toString()
    }
}

class MapEntry(
    override val key: String,
    private var _value: Any? = null,
    val readOnly: Boolean = false
): MutableMap.MutableEntry<String,Any?> {
    override val value: Any? get() { return _value }

    override fun setValue(newValue: Any?): Any? {
        val oldValue = _value
        if (!readOnly)
            _value = newValue
        return oldValue
    }

    override fun toString(): String {
        return "$key=$_value"
    }
}

