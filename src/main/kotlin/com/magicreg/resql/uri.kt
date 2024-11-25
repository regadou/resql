package com.magicreg.resql

import eu.maxschuster.dataurl.DataUrl
import eu.maxschuster.dataurl.DataUrlSerializer
import org.apache.commons.beanutils.BeanMap
import java.io.*
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.*

fun isValidUriScheme(scheme: String): Boolean {
    if (URI_SCHEMES.contains(scheme))
        return true
    if (!contextInitialized())
        return false
    return getContext().namespace(scheme) != null
}

fun isReadOnlyUri(uri: URI): Boolean {
    val url = resolveRelativeTemplate(uri)
    if (url.scheme == "data" && url.toString().indexOf(",") < 0)
        return false
    return getContext().namespace(url.scheme)?.readOnly ?: arrayOf("data", "geo").contains(url.scheme)
}

fun resolveUri(uri: URI, method: RestMethod, headers: Map<String,String>, body: Any?): Any? {
    val url = resolveRelativeTemplate(uri)
    if (url.scheme == "http" || url.scheme == "https")
        return httpRequest(method, url, headers)
    if (contextInitialized()) {
        val ns = getContext().namespace(url.scheme)
        if (ns != null) {
            val path = uri.path?.split("/")?.filter { it.isNotBlank() } ?: emptyList()
            return ns.execute(method, path, Query(uri.query), body)
        }
    }

    return when (method) {
        RestMethod.GET -> when (url.scheme) {
            "file" -> getFile(File(url.path))
            "data" -> decodeData(url.toString())
            "geo" -> GeoLocation(url)
            "jdbc" -> Database(url.toString())
            "sftp", "mailto", "sip" -> Response(StatusCode.NotImplemented)
            else -> {
                if (URI_SCHEMES.contains(url.scheme))
                    Response(StatusCode.InternalServerError)
                else
                    Response(StatusCode.BadRequest)
            }
        }

        RestMethod.POST -> when (url.scheme) {
            "file" -> postFile(File(url.path), body, headers)
            "data" -> encodeData(url.toString(), body)
            "geo", "jdbc", "sftp", "mailto", "sip" -> Response(StatusCode.NotImplemented)
            else -> {
                if (URI_SCHEMES.contains(url.scheme))
                    Response(StatusCode.InternalServerError)
                else
                    Response(StatusCode.BadRequest)
            }
        }

        RestMethod.PUT -> when (url.scheme) {
            "file" -> putFile(File(url.path), body, headers)
            "data" -> encodeData(url.toString(), body)
            "geo", "jdbc", "sftp", "mailto", "sip" -> Response(StatusCode.NotImplemented)
            else -> {
                if (URI_SCHEMES.contains(url.scheme))
                    Response(StatusCode.InternalServerError)
                else if (contextInitialized() && (url.path.isNullOrBlank() || url.path == "/"))
                    createNamespace(url.scheme, body.resolve())
                else
                    Response(StatusCode.BadRequest)
            }
        }

        RestMethod.DELETE -> when (url.scheme) {
            "file" -> deleteFile(File(url.path))
            "data", "geo", "jdbc", "sftp", "mailto", "sip" -> Response(StatusCode.NotImplemented)
            else -> {
                if (URI_SCHEMES.contains(url.scheme))
                    Response(StatusCode.InternalServerError)
                else
                    Response(StatusCode.BadRequest)
            }
        }
    }
}

fun httpRequest(method: RestMethod, uri: URI, headers: Map<String,String>, value: Any? = null): Response {
    val requestBuilder = HttpRequest.newBuilder().uri(uri)
    for (name in headers.keys)
        requestBuilder.setHeader(name, headers[name])
    val request = when (method) {
        RestMethod.GET -> requestBuilder.GET()
        RestMethod.PUT-> requestBuilder.PUT(HttpRequest.BodyPublishers.ofInputStream(prepareData(value, requestBuilder, headers[CONTENT_TYPE_HEADER])))
        RestMethod.POST -> requestBuilder.POST(HttpRequest.BodyPublishers.ofInputStream(prepareData(value, requestBuilder, headers[CONTENT_TYPE_HEADER])))
        RestMethod.DELETE -> requestBuilder.DELETE()
    }
    val response = HttpClient.newBuilder()
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build().send(request.build(), HttpResponse.BodyHandlers.ofInputStream())
    val statusCode = response.statusCode()
    if (statusCode >= 400)
        return Response(getStatusCode(response.statusCode()))
    val parts = response.headers().firstValue("content-type").orElse(TEXT_FORMAT).split(";")
    val charset = parts.firstOrNull { it.contains("charset=")}?.split("=")?.get(1)?.trim() ?: defaultCharset()
    return Response(response.body(), parts[0].trim(), charset, getStatusCode(statusCode))
}

private const val TEXT_FORMAT = "text/plain"
private const val DATA_FORMAT = "application/json"
private const val CONTENT_TYPE_HEADER = "content-type"
private val URI_SCHEMES = "http,https,file,data,geo,jdbc,sftp,mailto,sip".split(",")
private val DATA_SERIALIZER = DataUrlSerializer()

private fun resolveRelativeTemplate(tpl: URI): URI {
    // TODO: find out template sections and resolve variables
    val uri = populateTemplateValues(tpl)
    if (uri.scheme != null)
        return uri
    if (!contextInitialized())
        throw RuntimeException("Context is not initialized for relative uris, please use an absolute uri with a specified scheme")
    val cx = getContext()
    var path = uri.toString()
    if (path.startsWith("/"))
        return URI("${cx.uri}$path")
    val fullPath = File(cx.parentFolder(), path)
    return URI("${cx.uri}$fullPath")
}

private fun populateTemplateValues(uri: URI): URI {
    val parts = uri.toString().split("{")
    if (parts.size == 1)
        return uri
    val values = mutableListOf<String>()
    for (part in parts) {
        if (values.isEmpty())
            values.add(part)
        else {
            val end = part.indexOf("}")
            if (end > 0) {
                val first = part.substring(0, end).toExpression().resolve()
                val last = part.substring(end+1)
                values.add(first.toText()+last)
            }
            else
                values.add(part.substring(end+1))
        }
    }
    return URI(values.joinToString(""))
}

private fun prepareData(value: Any?, requestBuilder: HttpRequest.Builder, headerType: String?): () -> InputStream {
    val type = headerType ?: DATA_FORMAT
    requestBuilder.setHeader(CONTENT_TYPE_HEADER, type)
    val output = ByteArrayOutputStream()
    encode(value, type, output)
    return { ByteArrayInputStream(output.toByteArray()) }
}

private fun getStatusCode(code: Int): StatusCode {
    for (status in StatusCode.entries) {
        if (status.code == code)
            return status
    }
    return when (code/100) {
        2 -> StatusCode.OK
        3 -> StatusCode.MovedPermanently
        4 -> StatusCode.BadRequest
        5 -> StatusCode.InternalServerError
        else -> StatusCode.BadGateway
    }
}

private fun getFile(file: File): Any? {
    return if (file.isDirectory)
        file.listFiles().map { it.toURI() }
    else if (file.exists())
        decode(detectFileType(file.toString()), FileInputStream(file))
    else
        null
}

private fun putFile(file: File, value: Any?, headers: Map<String,String>): Any? {
    if (canSaveFile(file)) {
        val type = headers["content-type"] ?: detectFileType(file.toString())
        encode(value, type, FileOutputStream(file))
        return file.toURI()
    }
    else
        return null
}

private fun postFile(file: File, value: Any?, headers: Map<String,String>): Any? {
    if (file.isDirectory) {
        val type = headers["content-type"] ?: if (value is CharSequence || value is Number || value is Boolean) TEXT_FORMAT else DATA_FORMAT
        val name = Math.random().toString().split(".")[1] + "." + getMimetypeExtensions(type)[0]
        val newFile = File("$file/$name")
        encode(value, detectFileType(file.toString()), FileOutputStream(newFile))
        return newFile.toURI()
    }
    return putFile(file, value, headers)
}

private fun deleteFile(file: File): Boolean {
    if (!file.exists())
        return false
    if (file.isDirectory) {
        for (f in file.listFiles())
            deleteFile(f)
    }
    return file.delete()
}

private fun canSaveFile(file: File): Boolean {
    if (file.isDirectory)
        return false
    if (file.exists())
        return true
    if (file.parentFile.exists())
        return true
    return file.parentFile.mkdirs()
}

private fun decodeData(uri: String): Any? {
    val ending = if (uri.indexOf(",") < 0) "," else ""
    val uridata = DATA_SERIALIZER.unserialize(uri+ending)
    val charset = if (uridata.encoding.name == "URL") defaultCharset() else uridata.encoding.name
    return decode(uridata.mimeType, ByteArrayInputStream(uridata.data), charset)
}

private fun encodeData(uri: String, value: Any?): Any? {
    val txt = if (uri.contains(",")) uri else "$uri,"
    if (txt == "data:," && value.isText())
        return value.toText().detectFormat(true) ?: throw RuntimeException("Cannot detect format from string:\n$value")
    val uridata = DATA_SERIALIZER.unserialize(txt)
    val output = ByteArrayOutputStream()
    encode(value, uridata.mimeType, output, defaultCharset())
    val outdata = DataUrl(output.toByteArray(), uridata.encoding, uridata.mimeType)
    return DATA_SERIALIZER.serialize(outdata).toUri().toString()
}

private fun decode(type: String?, input: InputStream, charset: String = defaultCharset()): Any? {
    val mimetype = type ?: TEXT_FORMAT
    val format = (getFormat(mimetype) ?: getFormat(TEXT_FORMAT))!!
    val value = format.decode(input, charset)
    input.close()
    return value
}

private fun encode(value: Any?, type: String?, output: OutputStream, charset: String = defaultCharset()) {
    val mimetype = type ?: TEXT_FORMAT
    val format = (getFormat(mimetype) ?: getFormat(TEXT_FORMAT))!!
    format.encode(value.resolve(), output, charset)
    output.flush()
    output.close()
}

private fun createNamespace(prefix: String, data: Any?): Namespace {
    val ns = if (data is Database)
        data.copyWithPrefix(prefix)
    else if (data.isMappable())
        MemoryNamespace(prefix, randomUri(), false).populate(data.toMap()!!.mapKeys { it.key.toText() })
    else if (data.isIterable()) {
        val map = mutableMapOf<String,Any?>()
        val iterator = data.toIterator()!!
        while (iterator.hasNext()) {
            val submap = iterator.next().toMap()
            if (submap != null)
                map.putAll(submap.mapKeys { it.key.toText() })
        }
        MemoryNamespace(prefix, randomUri(), false).populate(map)
    }
    else if (data == null)
        MemoryNamespace(prefix, randomUri(), false)
    else
        MemoryNamespace(prefix, randomUri(), false).populate(BeanMap(data).mapKeys { it.key.toText() })
    if (!getContext().addNamespace(ns))
        throw RuntimeException("Error adding namespace with prefix $prefix and uri ${ns.uri}")
    return ns
}

private fun randomUri(): String {
    return "http://localhost/"+ UUID.randomUUID()
}