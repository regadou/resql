package com.magicreg.resql

import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.net.URI

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

    override fun getValue(name: String): Any? {
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
    override val keepUrlEncoding = true
    override val isEmpty: Boolean get() { return false }
    override val names: List<String> = emptyList()

    override fun hasName(name: String): Boolean {
        return getValue(name) != null
    }

    override fun getValue(name: String): Any? {
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

    override fun execute(method: RestMethod, path: List<String>, query: Query, value: Any?): Response {
        val sep = if (uri.endsWith("/")) "" else "/"
        val url = "$uri$sep${path.joinToString("/")}${queryString(query)}"
        val headers = if (method == RestMethod.GET && value is String) mapOf("accept" to value) else emptyMap()
        return httpRequest(method, URI(url), headers, value)
    }

    override fun toString(): String {
        return uri
    }
}

class FolderNamespace(
    private val folder: File,
    override val prefix: String = ""
): Namespace {
    override val uri: String = if (folder.isDirectory) folder.toURI().toString() else throw RuntimeException("$folder is not a directory")
    override val readOnly: Boolean = !folder.canWrite()
    override val names: List<String> get() { return folder.list().toList() }

    override fun hasName(name: String): Boolean {
        return File("$folder/$name").exists()
    }

    override fun getValue(name: String): Any? {
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

    override fun execute(method: RestMethod, path: List<String>, query: Query, value: Any?): Response {
        val file = File("$folder/${path.joinToString("/")}")
        return if (file.exists()) {
            when (method) { // TODO: take query into account
                RestMethod.GET -> {
                    if (file.isDirectory) {
                        val index = getIndexFile(file, value)
                        if (index == null)
                            Response(file.list().map { "$file/$it".substring(folder.toString().length + 1) })
                        else
                            Response(FileInputStream(index), index.toURI().contentType())
                    }
                    else
                        Response(FileInputStream(file), file.toURI().contentType())
                }
                RestMethod.POST, RestMethod.PUT -> {
                    if (file.isDirectory)
                        Response(StatusCode.MethodNotAllowed)
                    // TODO: we could create a new file with POST (give a random name and select the extension from the content-type)
                    else {
                        val result = resolveUri(file.toURI(), method, emptyMap(), value)
                        if (result == null)
                            Response(StatusCode.Forbidden)
                        else if (result is Response)
                            return result
                        else
                            Response(result, file.toURI().contentType())
                    }
                }
                RestMethod.DELETE -> Response(file.delete())
            }
        }
        else if (method == RestMethod.PUT && file.parentFile.exists()) {
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

class ContextWrapperNamespace(override val prefix: String): Namespace {
    override val uri: String get() { return if (contextInitialized()) getContext().uri else "" }
    override val readOnly: Boolean get() { return if (contextInitialized()) getContext().readOnly else false }
    override val names: List<String>  get() { return if (contextInitialized()) getContext().names else emptyList() }

    override fun hasName(name: String): Boolean {
        return if (contextInitialized()) getContext().hasName(name) else false
    }

    override fun getValue(name: String): Any? {
        return if (contextInitialized()) getContext().getValue(name) else null
    }

    override fun setValue(name: String, value: Any?): Boolean {
        return if (contextInitialized()) getContext().setValue(name, value) else false
    }

    override fun execute(method: RestMethod, path: List<String>, query: Query, value: Any?): Response {
        return if (contextInitialized()) getContext().execute(method, path, query, value) else Response(StatusCode.NotFound)
    }
}

// TODO: ArchiveNamespace class that can interface zip and tgz

private fun queryString(query: Query): String {
    val it = query.filter.iterator()
    while (it.hasNext()) {
        val map = it.next()
        if (map.isEmpty())
            continue
        return "?"+getFormat("form")!!.encodeText(map)
    }
    return ""
}

private fun getIndexFile(folder: File, type: Any?): File? {
    if (type != "text/html")
        return null
    for (file in folder.list()) {
        if (file == "index.html" || file == "index.htm")
            return File("$folder/$file")
    }
    return null
}
