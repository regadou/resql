package com.magicreg.resql

import org.apache.commons.beanutils.BeanMap

class GenericProperty(
    override val instance: Any?,
    override val key: String
): Property {
    override fun getValue(): Any? {
        return if (GENERIC_PROPERTIES.contains(key))
            genericPropertyValue(key, instance)
        else if (instance == null)
            null
        else if (instance is CharSequence || instance is Collection<*> || instance::class.java.isArray) {
            val index = key.toIntOrNull() ?: return null
            indexPropertyValue(index, instance)
        }
        else if (instance is Map<*,*>)
            instance[key]
        else if (instance is Namespace)
            instance.toMap()[key]
        else
            BeanMap(instance)[key]
     }

    override fun setValue(value: Any?): Boolean {
        return if (instance == null)
            false
        else if (instance is Collection<*> || instance::class.java.isArray) {
            val index = key.toIntOrNull() ?: return false
            indexPropertySetValue(index, instance, value)
        }
        else {
            val map = if (instance is Map<*,*>)
                instance
            else if (instance is Namespace)
                instance.toMap()
            else
                BeanMap(instance)
            try {
                (map as MutableMap<Any?,Any?>)[key] = value
                true
            }
            catch (e: Exception) { false }
        }
    }

    override fun toString(): String {
        val hash = instance?.hashCode()?.toString() ?: "null"
        val type = (if (instance == null) Void::class else instance::class).simpleName
        return "$key@$$type#$hash"
    }
}

private val GENERIC_PROPERTIES = "class,size,keys".split(",")

private fun genericPropertyValue(key: String, instance: Any?): Any? {
    return when (key) {
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
                BeanMap(instance).keys.map{it.toString()}
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
