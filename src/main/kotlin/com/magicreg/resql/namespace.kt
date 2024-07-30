package com.magicreg.resql

import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.net.URI

fun getNamespace(prefix: String): Namespace? {
    return NS_PREFIX_MAP[prefix] ?: NS_URI_MAP[prefix]
}

fun addNamespace(ns: Namespace): Boolean {
    if (NS_PREFIX_MAP.containsKey(ns.prefix) || NS_URI_MAP.containsKey(ns.uri))
        return false
    NS_PREFIX_MAP[ns.prefix] = ns
    NS_URI_MAP[ns.toString()] = ns
    return true
}

class MemoryNamespace(
    override val prefix: String,
    override val uri: String,
    override val readOnly: Boolean
): Namespace {
    private val mapping = mutableMapOf<String,Any?>()

    override fun toString(): String {
        return uri
    }

    override val names: List<String> get() { return mapping.keys.sorted() }

    override fun hasName(name: String): Boolean {
        return mapping.containsKey(name)
    }

    override fun value(name: String): Any? {
        return if (name.isBlank()) this else mapping[name]
    }

    override fun setValue(name: String, value: Any?): Boolean {
        if (!readOnly && name.isNotBlank()) {
            if (value == null) {
                if (mapping.containsKey(name)) {
                    mapping.remove(name)
                    return true
                }
                return false
            }
            mapping[name] = value
            return true
        }
        return false
    }

    fun populate(map: Map<String,Any?>): Namespace {
        if (!readOnly || mapping.isEmpty())
            mapping.putAll(map)
        return this
    }
}

class HttpNamespace(
    override val uri: String,
    override val prefix: String = "",
    override val readOnly: Boolean = false
): Namespace {
    override val isEmpty: Boolean get() { return false }
    override val names: List<String> = emptyList()

    override fun hasName(name: String): Boolean {
        return value(name) != null
    }

    override fun value(name: String): Any? {
        val sep = if (uri.endsWith("/") || name.startsWith("/")) "" else "/"
        return URI("$uri$sep$name").resolve()
    }

    override fun setValue(name: String, value: Any?): Boolean {
        if (readOnly)
            return false
        val sep = if (uri.endsWith("/") || name.startsWith("/")) "" else "/"
        val url = URI("$uri$sep$name")
        return toBoolean(if (value == null) url.delete() else toBoolean(url.put(value)))
    }

    override fun apply(method: UriMethod, path: List<String>, query: Query, value: Any?): Response {
        val sep = if (uri.endsWith("/")) "" else "/"
        val url = "$uri$sep${path.joinToString("/")}${queryString(query)}"
        return httpRequest(method, URI(url), emptyMap(), value)
    }

    override fun toString(): String {
        return uri
    }
}

class FolderNamespace(
    private val folder: File,
    override val prefix: String = ""
): Namespace {
    override val uri: String = folder.toURI().toString()
    override val readOnly: Boolean = !folder.canWrite()
    override val names: List<String> get() { return folder.list().toList() }

    override fun hasName(name: String): Boolean {
        return File("$folder/$name").exists()
    }

    override fun value(name: String): Any? {
        val file = File("$folder/$name")
        return if (file.isDirectory)
            FolderNamespace(file)
        else if (file.exists())
            file.toURI().resolve()
        else
            null
    }

    override fun setValue(name: String, value: Any?): Boolean {
        if (readOnly)
            return false
        val file = File("$folder/$name")
        if (file.isDirectory)
            return false
        val url = file.toURI()
        return toBoolean(if (value == null) url.delete() else toBoolean(url.put(value)))
    }

    override fun apply(method: UriMethod, path: List<String>, query: Query, value: Any?): Response {
        val sep = if (uri.endsWith("/")) "" else "/"
        val file = File("$uri$sep${path.joinToString("/")}")
        return if (file.isDirectory)
            Response(file.list())
        else if (file.exists()) {
            when (method) { // TODO: take query into account
                UriMethod.GET -> Response(FileInputStream(file), file.toURI().contentType())
                UriMethod.POST, UriMethod.PUT -> {
                    val result = resolveUri(file.toURI(), method, emptyMap(), value)
                    if (result == null)
                        Response(StatusCode.Forbidden)
                    else
                        Response(result, file.toURI().contentType())
                }
                UriMethod.DELETE -> Response(file.delete())
            }
        }
        else if (method == UriMethod.PUT && file.parentFile.exists()) {
            val buffer = ByteArrayOutputStream()
            getFormat(file.toURI().contentType() ?: "text/plain")!!.encode(value, buffer, defaultCharset())
            file.writeBytes(buffer.toByteArray())
            Response(true)
        }
        else
            Response(StatusCode.NotFound)
        // TODO: filter result with queryString(query)
    }

    override fun toString(): String {
        return uri
    }
}

// TODO: ArchiveNamespace class that can interface zip and tgz

private val NS_PREFIX_MAP = mutableMapOf<String,Namespace>()
private val NS_URI_MAP = mutableMapOf<String,Namespace>()

private fun queryString(query: Query): String {
    val it = query.filter.iterator()
    while (it.hasNext()) {
        val map = it.next()
        if (map.isEmpty())
            continue
        return "?"+ getFormat("form")!!.encodeText(map)
    }
    return ""
}