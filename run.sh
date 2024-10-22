#!/bin/sh

version=0.1-SNAPSHOT
folder=build/resql-$version
if [ "$1" = clean ]; then
    gradle clean || exit
    shift
fi
if [ ! -d $folder ]; then
    gradle distZip || exit
    unzip build/dis*/* -d build || exit
fi
if [ "$1" = debug ]; then
    export JAVA_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"
    shift
fi
build/resql*/bin/resql "$@"

