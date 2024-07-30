package com.magicreg.resql

import javax.script.Bindings
import javax.script.ScriptEngine
import javax.script.ScriptEngineManager
import kotlin.script.experimental.jsr223.KotlinJsr223DefaultScriptEngineFactory

fun getScriptEngines(): List<String> {
    val engines = mutableListOf<String>()
    for (factory in MANAGER.engineFactories) {
        val name = factory.engineName
        val lang = factory.languageName
        engines.add(if (name == lang) name else "$name($lang)")
    }
    return engines
}

fun getKotlinParser(): ScriptEngineParser {
    return KOTLIN_PARSER
}

class ScriptEngineParser(private val id: String) {
    private var inited = false
    val engine = getScriptEngine(id)
    val bindings = ContextBindings(getContext())

    fun checkInit(bindings: Bindings) {
        if (inited)
            return
        inited = true
        val text = initCodeMap[engine.factory.engineName]!!
        engine.eval(text, bindings)
    }

    fun execute(text: String): Any? {
        checkInit(bindings)
        return engine.eval(text, bindings)
    }

    override fun toString(): String {
        return "ScriptEngineParser(${engine.factory.engineName})"
    }
}

class ContextBindings(private val cx: Context): Bindings, AbstractMap<String,Any?>() {
    override fun clear() {
        for (key in cx.names)
            cx.setValue(key, null)
    }

    override fun put(key: String, value: Any?): Any? {
        val old = cx.value(key)
        cx.setValue(key, value)
        return old
    }

    override fun putAll(from: Map<out String, Any?>) {
        for (entry in from.entries)
            put(entry.key, entry.value)
    }

    override fun remove(key: String): Any? {
        val old = cx.value(key)
        cx.setValue(key, null)
        return old
    }

    override val entries: MutableSet<MutableMap.MutableEntry<String, Any?>> get() {
        return cx.names.map { MapEntry(it, cx.value(it)) }.toMutableSet()
    }

    override val keys: MutableSet<String> get() {
        return cx.names.toMutableSet()
    }

    override val values: MutableCollection<Any?> get() {
        return cx.names.map { cx.value(it) }.toMutableList()
    }

}

private val MANAGER = ScriptEngineManager()
private val KOTLIN_NAMES = "kotlin,kts,kt,text/x-kotlin".split(",")
private val KOTLIN_FACTORY = KotlinJsr223DefaultScriptEngineFactory()
private val KOTLIN_PARSER = ScriptEngineParser("kotlin")
private val initCodeMap = mapOf(
    "kotlin" to "import com.magicreg.resql.*; import java.io.*; import java.net.*;"
)

private fun getScriptEngine(name: String): ScriptEngine {
    if (KOTLIN_NAMES.contains(name))
        return KOTLIN_FACTORY.scriptEngine
    return MANAGER.getEngineByName(name) ?: MANAGER.getEngineByExtension(name) ?: MANAGER.getEngineByMimeType(name)
                                         ?: throw RuntimeException("Unknown script engine: $name")
}
