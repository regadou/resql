package com.magicreg.resql

import org.apache.commons.beanutils.BeanMap
import java.io.File
import java.net.URI

fun main(args: Array<String>) {
    if (args.isEmpty())
        showHelp()
    else {
        val map = mutableMapOf<String, Any?>()
        for (arg in args) {
            if (!setConfigData(map, arg))
                return
        }
        startApp(map)
    }
}

private fun showHelp(message: String? = null, returnValue: Boolean = false): Boolean {
    val params = BeanMap(Configuration()).entries.filter{it.key!="class"}.map{ "  - ${it.key}: ${getFormat("json")!!.encodeText(it.value).trim()}" }.sorted()
    if (message != null)
        println(message)
    println("Usage: resql <arg> [<arg> ...]")
    println("  each arg can be a data filename or actual data content")
    println("  supported formats are json, yaml, csv or urlencoded form")
    println("  list of configuration parameters with their default values:")
    println(params.joinToString("\n"))
    return returnValue
}

private fun setConfigData(configData: MutableMap<String, Any?>, txt: String): Boolean {
    val uri = txt.toUri() ?: existingFile(txt) ?: txt.detectFormat(true)?.toUri() ?: return showHelp("Invalid configuration uri: $txt")
    val value = uri.get().resolve()
    if (value is Exception)
        throw value
    for (entry in toMap(value).entries)
        configData[entry.key.toString()] = entry.value
    return true
}

private fun startApp(configData: Map<String, Any?>) {
    val config = Configuration(
        host = configData["host"]?.toString() ?: "localhost",
        port = configData["port"]?.toString()?.toIntOrNull() ?: 0,
        routes = loadNamespaces(configData["routes"]),
        maxPageSize = configData["maxPageSize"]?.toString()?.toIntOrNull() ?: 100,
        scripting = toBoolean(configData["scripting"]),
        home = configData["home"]?.toString(),
        style = configData["style"]?.toString()
    )
    println("configuration = "+getFormat("json")!!.encodeText(config))
    startServer(config)
}

private fun loadNamespaces(value: Any?): Map<String, Namespace> {
    val src: Map<String,Any?> = (
        if (value == null)
            emptyMap()
        else if (value is Map<*,*>)
            value as Map<String,Any?>
        else
            throw RuntimeException("Invalid map value: $value")
    )
    val dst = mutableMapOf<String, Namespace>()
    for (key in src.keys) {
        val route = src[key]?.toString()
        val uri = if (route.isNullOrBlank()) URI("") else route.toUri() ?: throw RuntimeException("Invalid uri value: ${src[key]}")
        dst[key] = loadNamespace(key, uri)
    }
    return dst
}

private fun loadNamespace(prefix: String, uri: URI): Namespace {
    if (uri.scheme == null) {
        return if (uri.path.isNullOrBlank())
            ContextWrapperNamespace(prefix)
        else
            loadNamespace(prefix, File(uri.path).canonicalFile.toURI())
    }
    return when (uri.scheme) {
        "http", "https" -> {
            val value = uri.resolve()
            if (value is Map<*,*>)
                MemoryNamespace(prefix, uri.toString(), true).populate(value as Map<String,Any?>)
            else
                HttpNamespace(uri.toString(), prefix)
            // TODO: detect if it is an OpenAPI file and manage accordingly
        }
        "file" -> {
            val file = File(uri.path)
            if (file.isDirectory)
                FolderNamespace(file, prefix)
            else {
                val value = uri.resolve()
                // TODO: check if it is a zip or tgz archive to be managed like a folder
                MemoryNamespace(prefix, uri.toString(), true).populate(toMap(value) as Map<String, Any?>)
            }
        }
        "jdbc" -> Database(uri.toString(), prefix)
        "data" -> MemoryNamespace(prefix, uri.toString().split(",")[0].split(";")[0], true).populate(toMap(uri.resolve()) as Map<String, Any?>)
        // TODO: support sftp uri scheme as if it was a mounted remote file
        else -> throw RuntimeException("Uri scheme not supported for namespaces: ${uri.scheme}")
    }
}

fun existingFile(txt: String): URI? {
    val file = File(txt)
    return if (!file.exists() || file.isDirectory) null else file.toURI()
}