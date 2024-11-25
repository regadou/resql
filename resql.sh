#!/bin/bash

ACCEPT_LANG=$(echo $LANG|cut -d _ -f 1|cut -d . -f 1)
if [ -z "$RESQL_SERVER" ]; then
    RESQL_SERVER="$1"
    shift
fi

execute() {
    if [ -z "$RESQL_COOKIES" ]; then
        RESQL_COOKIES="/home/$USER/resql-cookies-$RANDOM.txt"
        action="-c"
    else
        action="-b"
    fi 
    curl -H "content-type: text/x-resql" -H "accept: application/json" -H "accept-language: $ACCEPT_LANG" $action "$RESQL_COOKIES" -X POST "$RESQL_SERVER" -d "$1"
}

if [ -z "$RESQL_SERVER" ]; then
    echo ``'usage: resql <server url> <files> | <expression>'
    echo 'server url can be omitted if environment variable RESQL_SERVER is not empty'
    echo 'if neither files nor expression is given, a REPL session will be started'
elif [ -z "$1" ]; then
    while [ -z '' ]; do
        echo ''
        read -ep '? ' line
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

