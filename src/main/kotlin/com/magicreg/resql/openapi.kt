package com.magicreg.resql

import io.swagger.v3.oas.models.media.Schema
import io.swagger.v3.parser.OpenAPIV3Parser
import io.swagger.v3.parser.core.models.SwaggerParseResult
import java.net.URI

data class Definition(val word: String) {
    val key: String = word.lowercase()
    val schemaMap: MutableMap<String,Schema<Any>> = mutableMapOf()
}

fun createDictionary(apis: List<URI>): Map<String,Definition> {
    val dictio = mutableMapOf<String,Definition>()
    for (uri in apis) {
        val result: SwaggerParseResult = OpenAPIV3Parser().readLocation(uri.toString(), null, null)
        if (result == null)
            throw RuntimeException("$uri parsing result is null")
        else if (result.openAPI == null) {
            val message = result.messages?.joinToString("\n  ") ?: "Unknown parsing error"
            throw RuntimeException("$uri has parsing errors\n  $message")
        }
        else if (result.openAPI.components == null) {
            val message = result.messages?.joinToString("\n  ") ?: ""
            throw RuntimeException("$uri components is null\n  $message")
        }
        else {
            val schemas: Map<String,Schema<Any>> = result.openAPI.components.schemas ?: emptyMap()
            val source = uri.toString().split("/").lastOrNull { it.isNotBlank() }?.split(".")?.get(0) ?: uri.toString()
            for (key in schemas.keys)
                addSchema(source, dictio, key, schemas[key]!!)
        }
    }
    return dictio
}

fun addSchema(source: String, dictio: MutableMap<String,Definition>, word: String, schema: Schema<Any>) {
    val key = word.lowercase()
    var entry = dictio[key]
    if (entry == null) {
        entry = Definition(word)
        dictio[key] = entry
    }
    entry.schemaMap[source] = schema
}
