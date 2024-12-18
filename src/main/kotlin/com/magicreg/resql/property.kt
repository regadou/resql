package com.magicreg.resql

import org.apache.commons.beanutils.BeanMap

class GenericProperty(
    override val instance: Any?,
    override val key: String
): Property {
    override fun getValue(): Any? {
        val resolved = instance.resolve()
        return if (GENERIC_PROPERTIES.contains(key))
            genericPropertyValue(key, resolved)
        else if (resolved == null || key.isBlank())
            resolved
        else if (resolved is CharSequence || resolved is Collection<*> || resolved::class.java.isArray) {
            val index = key.toIntOrNull() ?: return null
            indexPropertyValue(index, resolved)
        }
        else if (resolved is Map<*,*>)
            resolved[key]
        else if (resolved is Namespace)
            resolved.getValue(key)
        else
            BeanMap(resolved)[key]
     }

    override fun setValue(value: Any?): Boolean {
        // TODO: check if reference so maybe call PUT or DELETE
        if (instance.isReference()) {

        }
        val resolved = instance.resolve()
        return if (resolved == null || key.isBlank())
            false
        else if (resolved is Collection<*> || resolved::class.java.isArray) {
            val index = key.toIntOrNull() ?: return false
            indexPropertySetValue(index, resolved, value)
        }
        else {
            try {
                if (resolved is MutableMap<*,*>)
                    (resolved as MutableMap<Any?,Any?>)[key] = value
                else if (resolved is Map<*,*> || resolved == null || resolved.isText() || resolved.isIterable())
                    return false
                else if (resolved is Namespace)
                    resolved.setValue(key, value)
                else
                    BeanMap(resolved)[key] = value
                true
            }
            catch (e: Exception) { false }
        }
    }

    override fun removeValue(): Boolean {
        // TODO: check if reference so maybe call PUT or DELETE
        if (instance.isReference()) {

        }
        val resolved = instance.resolve()
        return if (resolved == null || key.isBlank() || resolved::class.java.isArray)
            false
        else if (resolved is Collection<*> ) {
            if (resolved is MutableList<*>) {
                val index = key.toIntOrNull()
                if (index == null || index < 0 || index >= resolved.size)
                    false
                else {
                    resolved.removeAt(index)
                    true
                }
            }
            else
                false
        }
        else {
            try {
                if (resolved is MutableMap<*,*>)
                    (resolved as MutableMap<Any?,Any?>).remove(key)
                else if (resolved is Namespace)
                    resolved.setValue(key, null)
                else
                    false
                true
            }
            catch (e: Exception) { false }
        }
    }

    override fun toString(): String {
        val resolved = instance.resolve()
        val hash = resolved?.hashCode()?.toString() ?: "null"
        val type = (if (resolved == null) Void::class else resolved::class).simpleName
        return "$key@$$type#$hash"
    }
}

class CollectionProperty(
    override val instance: Any?,
    val keys: List<String>
): Property {
    override val key = keys.joinToString(",")

    override fun getValue(): Any? {
        return if (keys.isEmpty())
            instance.resolve()
        else
            keys.map { GenericProperty(this, it).getValue() }
    }

    override fun setValue(value: Any?): Boolean {
        val values = value.toCollection()
        var modified = 0
        for ((i, k) in keys.withIndex()) {
            val v = if (i < values.size) values[i] else null
            if (GenericProperty(this, k).setValue(v))
                modified++
        }
        return modified > 0
    }

    override fun removeValue(): Boolean {
        var modified = 0
        for ((i, k) in keys.withIndex()) {
            if (GenericProperty(this, k).removeValue())
                modified++
        }
        return modified > 0
    }

    override fun toString(): String {
        val resolved = instance.resolve()
        val hash = resolved?.hashCode()?.toString() ?: "null"
        val type = (if (resolved == null) Void::class else resolved::class).simpleName
        return "($key)@$$type#$hash"
    }
}

private val GENERIC_PROPERTIES = "type,class,size,keys".split(",")

private fun genericPropertyValue(key: String, instance: Any?): Any? {
    return when (key) {
        "type" -> Type.entries.reversed().firstOrNull { it.isInstance(instance) } ?: Type.ANY
        "class" -> if (instance == null) Void::class else instance::class
        "size" -> {
            if (instance == null)
                0
            else if (instance is Number)
                instance.toDouble()
            else if (instance is Boolean)
                if (instance) 1 else 0
            else if (instance is CharSequence)
                instance.length
            else if (instance is Collection<*>)
                instance.size
            else if (instance is Array<*>)
                instance.size
            else if (instance is Map<*,*>)
                instance.size
            else if (instance is Namespace)
                instance.names.size
            else {
                val map = BeanMap(instance)
                if (map.isEmpty()) 1 else map.size
            }
        }
        "keys" -> {
            if (instance is Map<*,*>)
                instance.keys.map{it.toString()}
            else if (instance is Namespace)
                instance.names
            else
                BeanMap(instance).keys.map{it.toString()}.filter{it!="class"}
        }
        else -> null
    }
}

private fun indexPropertyValue(index: Int, instance: Any?): Any? {
    if (instance is CharSequence) {
        return if (index < -instance.length || index >= instance.length)
            null
        else if (index >= 0)
            instance[index]
        else
            instance[instance.length+index]
    }
    else if (instance is List<*>) {
        return if (index < -instance.size || index >= instance.size)
            null
        else if (index >= 0)
            instance[index]
        else
            instance[instance.size+index]
    }
    else if (instance is Collection<*>) {
        return if (index < -instance.size || index >= instance.size)
            null
        else if (index >= 0)
            instance.toTypedArray()[index]
        else
            instance.toTypedArray()[instance.size+index]
    }
    else if (instance != null && instance::class.java.isArray) {
        val size = java.lang.reflect.Array.getLength(instance)
        return if (index < -size || index >= size)
            null
        else if (index >= 0)
            java.lang.reflect.Array.get(instance, index)
        else
            java.lang.reflect.Array.get(instance, size+index)
    }
    return null
}

private fun indexPropertySetValue(index: Int, instance: Any?, value: Any?): Boolean {
    try {
        if (instance is MutableCollection<*> && index >= instance.size)
            return (instance as MutableCollection<Any?>).add(value)
        if (instance is MutableList<*>) {
            val list = instance as MutableList<Any?>
            if (index >= list.size)
                list.add(value)
            else if (index >= 0)
                list[index] = value
            else if (index >= -list.size)
                list[list.size + index] = value
            else
                return false
            return true
        }
        if (instance != null && instance::class.java.isArray) {
            val size = java.lang.reflect.Array.getLength(instance)
            if (index >= size)
                false
            else if (index >= 0)
                java.lang.reflect.Array.set(instance, index, value)
            else if (index >= -size)
                java.lang.reflect.Array.set(instance, size + index, value)
            else
                return false
            return true
        }
    } catch (e: Exception) {}
    return false
}
