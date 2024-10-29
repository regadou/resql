package com.magicreg.resql

fun parseText(txt: String): Expression {
    return parseExpression(ParseStatus(txt))
}

fun compileExpression(tokens: List<Any?>): Expression {
    if (tokens.isEmpty())
        return Expression()
    if (tokens.size == 1)
        return Expression(null, tokens)
    var function: Function? = null
    var args = mutableListOf<Any?>()
    for (token in tokens) {
        if (token !is Function)
            args.add(token)
        else if (function == null)
            function = token
        else { // TODO: check for operator precedence
            val exp = Expression(function, args)
            function = token
            args = mutableListOf(exp)
        }
    }
    return Expression(function, args)
}

private fun parseExpression(status: ParseStatus): Expression {
    val end = status.end
    val expressions = mutableListOf<Expression>()
    var tokens = mutableListOf<Any?>()
    var token: String? = null
    var quote: Char? = null

    fun addToken(addExpression: Boolean) {
        if (token != null) {
            tokens.add(parseToken(token!!))
            token = null
        }
        if (addExpression && tokens.isNotEmpty()) {
            expressions.add(compileExpression(tokens))
            tokens = mutableListOf()
        }
    }

    while (status.pos < status.length) {
        val c = status.chars[status.pos]
        if (quote != null) {
            if (c == quote) {
                tokens.add(token)
                token = null
                quote = null
            }
            else
                token += "$c"
        }
        else if (c in QUOTE_CHARS) {
            addToken(false)
            quote = c
            token = ""
        }
        else if (c == end) {
            status.end = null
            break
        }
        else if (c in GROUP_CHARS) {
            val index = GROUP_CHARS.indexOf(c)
            if (index%2 == 0) {
                addToken(false)
                status.pos++
                status.end = GROUP_CHARS[index+1]
                tokens.add(parseExpression(status))
            }
            else
                throw RuntimeException("Encountered not expected closing bracket at line ${status.line}: $c")
        }
        else if (c == '\n') {
            addToken(true)
            status.line++
        }
        else if (c <= ' ' || c in BLANK_NON_ASCII)
            addToken(false)
        else if (token == null)
            token = "$c"
        else
            token += c
        status.pos++
    }

    if (quote != null)
        throw RuntimeException("Missing closing quote at line ${status.line}: $quote")
    if (status.end != null)
        throw RuntimeException("Missing closing bracket at line ${status.line}: ${status.line}")
    addToken(true)
    return if (expressions.isEmpty())
        compileExpression(tokens)
    else if (expressions.size == 1)
        expressions[0]
    else
        Expression(MiscOperator.EXECUTE, expressions)
}

private class ParseStatus(txt: String) {
    val chars = txt.toCharArray()
    var pos: Int = 0
    val length = txt.length
    var end: Char? = null
    var line: Int = 1
}

private val BLANK_NON_ASCII = '\u007F'..'\u00AD'
private val QUOTE_CHARS = arrayOf('"', '\'', '`')
private val GROUP_CHARS = arrayOf('(', ')', '[', ']', '{', '}')

private fun parseToken(token: String): Any? {
    val cx = getContext()
    if (cx.hasName(token))
        return cx.property(token)
    return getFunction(token) ?: token.keyword()
}
