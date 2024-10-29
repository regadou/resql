package com.magicreg.resql

import io.ktor.server.application.*
import io.ktor.http.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.*
import io.ktor.server.sessions.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.*
import java.net.URI
import java.time.LocalDateTime
import java.time.ZoneOffset

fun startServer(config: Configuration) {
    if (config.port == null || config.port < 1)
        throw RuntimeException("Invalid port number: ${config.port}")
    val topContext = getContext(config)
    embeddedServer(Netty, host = config.host, port = config.port) {
        install(Sessions) {cookie<WebSession>("WEB_SESSION", storage = SessionStorageMemory())}
        routing { route("/{path...}") {handle{requestProcessing(topContext, call)}}}
        println("Server listening at http://${config.host}:${config.port}/ ...")
    }.start(true)
}

private const val DEBUG = true
private val METHODS_WITH_BODY = arrayOf(UriMethod.POST, UriMethod.PUT)
private val DEFAULT_FORMAT = getFormat("application/json")!!
private val HTTP_STATUS_CACHE = mutableMapOf<StatusCode,HttpStatusCode>()

private suspend fun requestProcessing(topContext: Context, call: ApplicationCall) {
    val headers = call.request.headers.toMap()!!.mapKeys {it.key.toString().normalize()}
    val cx = getSessionContext(call, topContext, headers)
    val config = cx.configuration()
    try {
        val method = UriMethod.valueOf(call.request.httpMethod.value.uppercase())
        val path = call.request.path()
        val query = Query(call.request.queryParameters.toMap()!!.mapKeys{it.toString()})
        val inputStream = if (METHODS_WITH_BODY.contains(method)) call.receiveStream() else null
        logRequest(method, path, call.request.origin.remoteAddress, headers["content-length"])
        var parts = path.split("/").filter { it.isNotBlank() }
        if (parts.isNotEmpty()) {
            val prefix = parts[0]
            val ns = config.routes[prefix] ?: return call.respondText(
                "$path not found\n",
                ContentType.Text.Plain,
                httpStatus(StatusCode.NotFound)
            )
            if (method != UriMethod.GET) {
                if (ns.readOnly)
                    return call.respondText(
                        "$method not allowed on $path\n",
                        ContentType.Text.Plain,
                        httpStatus(StatusCode.MethodNotAllowed)
                    )
                if (method == UriMethod.POST && scriptingConditionDone(config, inputStream!!, headers, call))
                    return
            }
            if (!ns.keepUrlEncoding)
                parts = parts.map { it.urlDecode() }
            if (method == UriMethod.GET && query.page.size > config.maxPageSize)
                query.addQueryPart(".page", mapOf("size" to config.maxPageSize))
            val response = ns.apply(method, parts.subList(1, parts.size), query, requestBody(headers, inputStream))
            val httpStatus = httpStatus(response.status)
            if (response.status.code >= 400) {
                val typeParts = (response.type ?: responseFormat(headers).mimetype).split("/")
                call.respondText(
                    "$path ${response.data ?: response.status.message}\n",
                    ContentType.Text.Plain,
                    httpStatus
                )
            }
            else if (response.data is InputStream) {
                val typeParts = (response.type ?: responseFormat(headers).mimetype).split("/")
                call.respondOutputStream(
                    ContentType(typeParts[0], typeParts[1]),
                    httpStatus
                ) { copyStream(response.data, this, true) }
            }
            else
                sendValue("/$prefix/", "", response, headers, call)
        }
        else if (method == UriMethod.GET)
            sendValue("/", "/", Response(config.routes.keys), headers, call)
        else if (method == UriMethod.POST && scriptingConditionDone(config, inputStream!!, headers, call))
            return
        else
            call.respondText("$method not allowed on $path\n", ContentType.Text.Plain, httpStatus(StatusCode.MethodNotAllowed))
    }
    catch (e: Exception) {
        val stacktrace = getStacktrace(e)
        System.err.println(stacktrace)
        val msg = if (!DEBUG) e.toString() else "<html><body><h2 style='text-align:center'>System error</h2><pre>\n$stacktrace\n</pre></body></html>"
        call.respondText(msg+"\n", ContentType.Text.Html, httpStatus(StatusCode.InternalServerError))
    }
    cx.close(false)
}

private suspend fun scriptingConditionDone(config: Configuration, inputStream: InputStream, headers: Map<String,Any?>, call: ApplicationCall): Boolean {
    val type = headers["content-type"].toString()
    val format = getFormat(type)
    if (format == null) {
        call.respondText("Content type not supported: $type", ContentType.Text.Plain, httpStatus(StatusCode.BadRequest))
        return true
    }
    else if (!format.scripting)
        return false
    else if (config.scripting) {
        val result = format.decode(inputStream, defaultCharset()) ?: ""
        sendValue("", "", Response(result), headers, call)
        return true
    }
    else {
        call.respondText("Scripting content type not allowed on this server", ContentType.Text.Plain, httpStatus(StatusCode.Unauthorized))
        return true
    }
}

private suspend fun sendValue(prefix: String, suffix: String, response: Response, headers: Map<String,Any?>, call: ApplicationCall) {
    val format = getFormat(response.type ?: "") ?: responseFormat(headers)
    var typeParts = format.mimetype.split("/")
    val data = if (typeParts[1] == "html" && response.data is Collection<*>)
        response.data.sortedBy{it.toString().lowercase()}.joinToString("<br>\n") { printHtmlLink(it, prefix, suffix, headers) }
    else
        format.encodeText(response.data, response.charset)
    call.respondText(data, ContentType(typeParts[0], typeParts[1]), httpStatus(response.status))
}

private fun httpStatus(code: StatusCode): HttpStatusCode {
    var status = HTTP_STATUS_CACHE[code]
    if (status == null) {
        status = HttpStatusCode(code.code, code.message)
        HTTP_STATUS_CACHE[code] = status
    }
    return status
}

private fun responseFormat(headers: Map<String,Any?>): Format {
    val accept = headers["accept"]?.toString() ?: return DEFAULT_FORMAT
    for (level in accept.split(";")) {
        for (part in level.split(",")) {
            val type = part.trim()
            if (type.isEmpty() || type.startsWith("q="))
                continue
            val format = getFormat(type)
            if (format != null && format.supported && !format.mimetype.contains("kotlin"))
                return format
        }
    }
    return DEFAULT_FORMAT
}

private suspend fun requestBody(headers: Map<String,Any?>, inputStream: InputStream?): Any? {
    if (inputStream == null) {
        val formats = extractHeader(headers, "accept")
        return if (formats.isEmpty()) null else formats[0]
    }
    val parts = (headers["content-type"]?.toString() ?: "text/plain").split(";")
    val mimetype = parts[0].trim()
    val charset = parts.firstOrNull { it.contains("charset=")}?.split("=")?.get(1)?.trim() ?: defaultCharset()
    val format = getFormat(mimetype) ?: throw RuntimeException("Unknown mimetype: $mimetype")
    return withContext(Dispatchers.IO) { format.decode(inputStream, charset) }
}

private fun logRequest(method: UriMethod, uri: String, remote: String, size: Any?) {
    val bytes = if (size == null) "" else " received $size bytes"
    println(printTime(System.currentTimeMillis())+" "+remote+" "+method.name+" "+uri+bytes)
}

private fun getSessionContext(call: ApplicationCall, parent: Context, headers: Map<String,Any?>): Context {
    val session : WebSession? = call.sessions.get<WebSession>()
    var local = call.request.local
    val baseuri = URI("${local.scheme}://${local.serverHost}:${local.serverPort}")
    val cx = findContext(session?.id, true) ?: getContext(parent, baseuri)
    if (session == null)
        call.sessions.set(WebSession(cx.name))
    cx.requestUri = local.uri.split("?")[0]
    cx.setValue("languages", extractHeader(headers, "accept-language"))
    return cx
}

private fun getStacktrace(e: Exception): String {
    val output = ByteArrayOutputStream()
    val pw = PrintWriter(output)
    e.printStackTrace(pw)
    pw.flush()
    return output.toString()
}

private fun printTime(millis: Long): String {
    val zone = ZoneOffset.systemDefault()
    val offSet = zone.rules.getOffset(LocalDateTime.now())
    val t = LocalDateTime.ofEpochSecond(millis/1000, (millis%1000).toInt()*1000, offSet)
    return t.toString().split(".")[0].split("T").joinToString(" ")
}

private fun printHtmlLink(data: Any?, prefix: String = "", suffix: String = "", headers: Map<String,Any?> = emptyMap()): String {
    var link = ""
    var name = ""
    if (data.isText() || data is Number || data is Boolean) {
        val parts = data.toText().split("/").filter { it.isNotBlank() }
        name = if (parts.isEmpty()) "" else parts[parts.size-1]
        link = parts.map { it.urlEncode() }.joinToString("/")
    }
    else {
        val map = data.toMap() ?: mapOf("value" to data)
        name = map.label()
        link = (map["id"]?.toString() ?: name).urlEncode()
    }
    return "<a href='$prefix$link$suffix'>$name</a>"
}

private suspend fun copyStream(input: InputStream, output: OutputStream, closeInput: Boolean) {
    withContext(Dispatchers.IO) {
        input.copyTo(output)
        if (closeInput)
            input.close()
    }
}

private fun extractHeader(headers: Map<String,Any?>, name: String): List<String> {
    val value = headers[name]?.toString() ?: return emptyList()
    return value.replace(";", ",")
                .split(",")
                .filter {it.indexOf("q=")<0 && it.indexOf("*/*")<0}
                .map {it.split("-")[0]}
}