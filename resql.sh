#!/bin/bash

COOKIE_FILE="/home/$USER/.resql-cookies"
if [ -z "$RESQL_SERVER" ]; then
    RESQL_SERVER="$1"
    shift
fi

execute() {
    if ! [ -f "$COOKIE_FILE" ]; then
        touch "$COOKIE_FILE"
    fi 
    curl -H "content-type: text/x-resql" -b "$COOKIE_FILE" -X POST "$RESQL_SERVER" -d "$1"
}

if [ -z "$RESQL_SERVER" ]; then
    echo ``'usage: resql <server url> <files> | <expression>'
    echo 'server url can be omitted if environment variable RESQL_SERVER is not empty'
    echo 'if neither files nor expression is given, a REPL session will be started'
elif [ -z "$1" ]; then
    while [ -z '' ]; do
        echo ''
        read -p '? ' line
        if [ "$line" = "exit" ] || [ "$line" = "quit" ]; then
            break
        elif [ -n "$line" ]; then
            execute "$line"
        fi
    done
elif [ -f "$1" ]; then
    for f in $@; do
        if [ -f "$f" ]; then
            execute "$(cat "$f")"
        else
            echo "Invalid file: $f"
        fi
    done
else
    execute "$@"
fi

