package com.magicreg.resql

import eu.maxschuster.dataurl.DataUrl
import eu.maxschuster.dataurl.DataUrlSerializer
import java.io.*
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

fun isBuiltinUriScheme(scheme: String): Boolean {
    return URI_SCHEMES.contains(scheme)
}

fun resolveRelativeTemplate(uri: URI): URI {
    // TODO: find out template sections and resolve variables
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

fun resolveUri(uri: URI, method: UriMethod, headers: Map<String,String>, body: Any?): Any? {
    val url = resolveRelativeTemplate(uri)
    if (url.scheme == "http" || url.scheme == "https") {
        val response = httpRequest(method, url, headers)
        if (response.data is InputStream)
            return decode(response.type, response.data, response.charset)
        if (response.status.code >= 400)
            return response
        return response.data
    }
    return when (method) {
        UriMethod.GET -> when (url.scheme) {
            "file" -> getFile(File(url.path))
            "data" -> decodeData(url.toString())
            "jdbc" -> try { Database(url.toString()) } catch (e: Exception) {e}
            else -> {
                val ns = getNamespace(url.scheme)
                if (ns != null)
                    ns.value(trimUriScheme(url))
                else
                    RuntimeException("Unsupported uri scheme for GET: ${url.scheme}")
            }
        }

        UriMethod.POST -> when (url.scheme) {
            "file" -> postFile(File(url.path), body, headers)
            "data" -> encodeData(url.toString(), body)
            else -> getNamespace(url.scheme)?.setValue(trimUriScheme(url), body)
                ?: RuntimeException("Unsupported uri scheme for POST: ${url.scheme}")
        }

        UriMethod.PUT -> when (url.scheme) {
            "file" -> putFile(File(url.path), body, headers)
            "data" -> encodeData(url.toString(), body)
            else -> getNamespace(url.scheme)?.setValue(trimUriScheme(url), body)
                ?: RuntimeException("Unsupported uri scheme for PUT: ${url.scheme}")
        }

        UriMethod.DELETE -> when (url.scheme) {
            "file" -> deleteFile(File(url.path))
            else -> getNamespace(url.scheme)?.setValue(trimUriScheme(url), null)
                ?: RuntimeException("Unsupported uri scheme for DELETE: ${url.scheme}")
        }
    }
}

fun httpRequest(method: UriMethod, uri: URI, headers: Map<String,String>, value: Any? = null): Response {
    val requestBuilder = HttpRequest.newBuilder().uri(uri)
    for (name in headers.keys)
        requestBuilder.setHeader(name, headers[name])
    val request = when (method) {
        UriMethod.GET -> requestBuilder.GET()
        UriMethod.PUT-> requestBuilder.PUT(HttpRequest.BodyPublishers.ofInputStream(prepareData(value, requestBuilder, headers[CONTENT_TYPE_HEADER])))
        UriMethod.POST -> requestBuilder.POST(HttpRequest.BodyPublishers.ofInputStream(prepareData(value, requestBuilder, headers[CONTENT_TYPE_HEADER])))
        UriMethod.DELETE -> requestBuilder.DELETE()
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
    return try { decode(uridata.mimeType, ByteArrayInputStream(uridata.data), charset) } catch (e: Exception) { e }
}

private fun encodeData(uri: String, value: Any?): Any? {
    val txt = if (uri.contains(",")) uri else "$uri,"
    if (txt == "data:," && value.isText())
        return value.toText().detectFormat(true) ?: throw RuntimeException("Cannot detect format for string:\n$value")
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
    format.encode(value, output, charset)
    output.flush()
    output.close()
}

private fun trimUriScheme(uri: URI): String {
    val txt = uri.toString()
    return if (uri.scheme == null) txt else txt.substring(uri.scheme.length+1)
}