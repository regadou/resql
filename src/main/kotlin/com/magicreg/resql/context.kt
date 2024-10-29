package com.magicreg.resql

import java.io.File
import java.net.URI
import java.util.*

fun contextInitialized(): Boolean {
    return CURRENT_CONTEXT.get() != null
}

fun findContext(id: String?, makeCurrent: Boolean): Context? {
    if (id.isNullOrBlank())
        return null
    val cx = CONTEXT_MAP[id]
    if (cx != null && makeCurrent)
        CURRENT_CONTEXT.set(cx)
    return cx;
}

fun getContext(vararg values: Any): Context {
    val existingContext = CURRENT_CONTEXT.get()
    if (existingContext != null) {
        if (values.isEmpty())
            return existingContext
        // TODO: if only 1 string arg then search for it in active context map and return it if not null
        throw RuntimeException("Context already initialized on this thread")
    }

    var name: String? = null
    var parent: Context? = null
    var constants: Map<String,Any?>? = null
    var variables: MutableMap<String,Any?>? = null
    var uri: URI? = null
    var config: Configuration? = null
    for (value in values) {
        if (value is Context)
            parent = value
        else if (value is Namespace) {
            if (value.readOnly)
                constants = value.toMap()
            else if (variables == null)
                variables = value.toMap()
            else if (constants == null)
                constants = value.toMap()
            else if (variables is MultiMap)
                variables.add(value.toMap())
            else
                variables = value.toMap()
        }
        else if (value is MutableMap<*,*>)
            variables = value as MutableMap<String,Any?>
        else if (value is Map<*,*>)
            constants = value as Map<String,Any?>
        else if (value is String)
            name = value
        else if (value is URI)
            uri = value
        else if (value is Configuration)
            config = value
    }

    if (constants == null && variables == null)
        constants = getNamespace("resql")?.toMap()
    if (name == null)
        name = randomName()
    if (uri == null)
        uri = File(System.getProperty("user.dir")).toURI()
    val cx = Context(name!!, uri!!.toString(), parent, constants ?: emptyMap(), variables ?: mutableMapOf(), config)
    CURRENT_CONTEXT.set(cx)
    return cx
}

class Context(
    val name: String,
    override val uri: String,
    private val parent: Context?,
    private val constants: Map<String,Any?>,
    private val variables: MutableMap<String,Any?> = mutableMapOf(),
    private var config: Configuration?
): Namespace {
    override val prefix: String = ""
    override val readOnly: Boolean = false
    var requestUri: String? = null
    init { CONTEXT_MAP[name] = this }

    fun childContext(childName: String?, constants: Map<String,Any?>, uri: String = this.uri): Context {
        if (CURRENT_CONTEXT.get() != this)
            throw RuntimeException("This context is not the current thread context")
        val cx = Context(childName ?: name+"/"+randomName(), uri, this, constants, mutableMapOf(), null)
        CURRENT_CONTEXT.set(parent)
        return cx
    }

    fun close(destroy: Boolean) {
        // TODO: close any context by removing it from the active context map
        if (CURRENT_CONTEXT.get() != this)
            throw RuntimeException("This context is not the current thread context")
        if (destroy) {
            CONTEXT_MAP.remove(name)
            CURRENT_CONTEXT.set(parent)
        }
        else
            CURRENT_CONTEXT.set(null)
    }

    override fun hasName(key: String): Boolean {
        return constants.containsKey(key) || variables.containsKey(key) || parent?.hasName(key) ?: false
    }

    override val names: List<String> get() {
        val keys = mutableSetOf<String>()
        keys.addAll(constants.keys)
        keys.addAll(variables.keys)
        if (parent != null)
            keys.addAll(parent.names)
        return keys.sorted()
    }

    override fun value(name: String): Any? {
        if (this.name.isBlank())
            return this
        if (constants.containsKey(name))
            return constants[name]
        if (variables.containsKey(name))
            return variables[name]
        return parent?.value(name)
    }

    override fun setValue(key: String, value: Any?): Boolean {
        if (constants.containsKey(key))
            return false
        var p = parent
        while (p != null) {
            if (p.constants.containsKey(key))
                return false
            p = p.parent
        }
        variables[key] = value
        return true
    }

    fun parentFolder(): String {
        if (requestUri == null)
            return parent?.parentFolder() ?: "/"
        var path = URI(requestUri!!).path
        if (!path.endsWith("/")) {
            val index = path.lastIndexOf("/")
            if (index > 0)
                path = path.substring(0, index + 1)
            else
                path = "/"
        }
        return path
    }

    fun configuration(): Configuration {
        if (config != null)
            return config!!
        if (parent != null)
            return parent.configuration()
        config = Configuration()
        return config!!
    }

    override fun toString(): String {
        return uri
    }
}

private val CURRENT_CONTEXT = ThreadLocal<Context>()
private val CONTEXT_MAP = mutableMapOf<String,Context>()

private fun randomName(): String {
    return UUID.randomUUID().toString()
}
