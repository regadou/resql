#!/bin/sh

gradle clean || exit
gradle distZip || exit
gradle publishToMavenLocal || exit

