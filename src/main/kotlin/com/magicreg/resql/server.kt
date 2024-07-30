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
import io.ktor.util.StringValues
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.*
import java.net.URI
import java.time.LocalDateTime
import java.time.ZoneOffset

fun startServer(config: Configuration) {
    if (config.port == null || config.port < 1)
        throw RuntimeException("Invalid port number: ${config.port}")
    val topContext = getContext()
    embeddedServer(Netty, host = config.host, port = config.port) {
        install(Sessions) {cookie<WebSession>("WEB_SESSION", storage = SessionStorageMemory())}
        routing {
            route("/{path...}") {
                handle {
                    val cx = getSessionContext(call, topContext)
                    try { requestProcessing(config, call) }
                    finally { cx.close(false) }
                }
            }
        }
        println("Server listening at http://${config.host}:${config.port}/ ...")
    }.start(true)
}

private val METHODS_WITH_BODY = arrayOf(UriMethod.POST, UriMethod.PUT)
private val DEFAULT_FORMAT = getFormat("application/json")!!
private data class WebSession(val id: String)

private suspend fun requestProcessing(config: Configuration, call: ApplicationCall) {
    val method = UriMethod.valueOf(call.request.httpMethod.value.uppercase())
    val path = call.request.path()
    val query = Query(getMap(call.request.queryParameters, false))
    val headers = getMap(call.request.headers, true)
    val inputStream = if (METHODS_WITH_BODY.contains(method)) call.receiveStream() else null
    logRequest(method, path, call.request.origin.remoteAddress, headers["content-length"])
    val parts = path.split("/").filter{it.isNotBlank()}
    if (parts.isNotEmpty()) {
        val prefix = parts[0]
        val ns = config.routes[prefix] ?: return call.respondText("$path not found\n", ContentType.Text.Plain, HttpStatusCode(StatusCode.NotFound.code, StatusCode.NotFound.message))
        if (ns.readOnly && method != UriMethod.GET)
            return call.respondText("$method not allowed on $path\n", ContentType.Text.Plain, HttpStatusCode(StatusCode.MethodNotAllowed.code, StatusCode.MethodNotAllowed.message))
        if (method == UriMethod.GET && query.page.size > config.maxPageSize)
            query.addQueryPart(".page", mapOf("size" to config.maxPageSize))
        val response = ns.apply(method, parts.subList(1, parts.size), query, requestBody(headers, inputStream))
        val httpStatus = HttpStatusCode(response.status.code, response.status.message)
        if (response.data is InputStream) {
            val typeParts = (response.type ?: responseFormat(headers).mimetype).split("/")
            call.respondOutputStream(ContentType(typeParts[0], typeParts[1]), httpStatus) { copyStream(response.data,this, true) }
        }
        else if (response.status.code >= 400) {
            val typeParts = (response.type ?: responseFormat(headers).mimetype).split("/")
            call.respondText("$path ${response.data ?: response.status.message}\n", ContentType.Text.Plain, httpStatus)
        }
        else
            sendValue("$prefix/", "", response, headers, call)
    }
    else if (method == UriMethod.GET)
        sendValue("", "/", Response(config.routes.keys), headers, call)
    else
        call.respondText("$method not allowed on $path\n", ContentType.Text.Plain, HttpStatusCode(StatusCode.MethodNotAllowed.code, StatusCode.MethodNotAllowed.message))
}

private suspend fun sendValue(prefix: String, suffix: String, response: Response, headers: Map<String,Any?>, call: ApplicationCall) {
    val format = getFormat(response.type ?: "") ?: responseFormat(headers)
    var typeParts = format.mimetype.split("/")
    val data = if (typeParts[1] == "html" && response.data is Collection<*>)
        response.data.joinToString("<br>\n") { "<a href='$prefix$it$suffix'>$it</a>" }
    else
        format.encodeText(response.data, response.charset)
    call.respondText(data, ContentType(typeParts[0], typeParts[1]), HttpStatusCode(response.status.code, response.status.message))
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
    if (inputStream == null)
        return null
    val parts = (headers["content-type"]?.toString() ?: "text/plain").split(";")
    val mimetype = parts[0].trim()
    val charset = parts.firstOrNull { it.contains("charset=")}?.split("=")?.get(1)?.trim() ?: defaultCharset()
    val format = getFormat(mimetype) ?: throw RuntimeException("Unknown mimetype: $mimetype")
    return withContext(Dispatchers.IO) { format.decode(inputStream, charset) }
}

private fun getMap(values: StringValues, normalize: Boolean): Map<String,Any?> {
    val map = mutableMapOf<String,Any?>()
    values.forEach { k, list ->
        val key = if (normalize) k.normalize() else k
        map[key] =  list.map{it.parse()}.simplify()
    }
    return map
}

private fun logRequest(method: UriMethod, uri: String, remote: String, size: Any?) {
    val bytes = if (size == null) "" else " received $size bytes"
    println(printTime(System.currentTimeMillis())+" "+remote+" "+method+" "+uri+bytes)
}

private fun getSessionContext(call: ApplicationCall, parent: Context): Context {
    val session : WebSession? = call.sessions.get<WebSession>()
    var local = call.request.local
    val baseuri = URI("${local.scheme}://${local.serverHost}:${local.serverPort}")
    val cx = findContext(session?.id, true) ?: getContext(parent, baseuri)
    if (session == null)
        call.sessions.set(WebSession(cx.name))
    cx.requestUri = local.uri.split("?")[0]
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

private suspend fun copyStream(input: InputStream, output: OutputStream, closeInput: Boolean) {
    withContext(Dispatchers.IO) {
        input.copyTo(output)
        if (closeInput)
            input.close()
    }
}
