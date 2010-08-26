#!/bin/bash

cd `dirname $0`;


HEDWIGBASE=../../../../..

HEDWIGJAR=`ls $HEDWIGBASE/server/target/server-*-with-dependencies.jar`
if [ ! $? -eq 0 ]; then
    echo "\n\nCould not find server-VERSION-with-dependencies.jar. \nYou need to build the java part of hedwig. \nRun mvn package in the toplevel hedwig directory.\n\n"
    exit 1;
fi

HEDWIGSERVERTESTS=$HEDWIGBASE/server/target/test-classes/
if [ ! -e $HEDWIGSERVERTESTS ]; then
    echo "\n\nThe hedwig java server tests need to be build.\b\b"
    exit 1;
fi

export CP=$HEDWIGJAR:$HEDWIGSERVERTESTS

case "$1" in
    start)
	if [ -e server-control.pid ]; then
	    kill -9 `cat server-control.pid`
	    rm server-control.pid
	fi
	java -cp $CP  -Dlog4j.configuration=log4j.properties org.apache.hedwig.ServerControlDaemon  <&-  1> servercontrol.out  2>&1  &
	echo $! > server-control.pid
	;;
    stop)
	kill -9 `cat server-control.pid`
	rm server-control.pid
	exit 0
	;;
    *)
	echo "Usage: run-server-control.sh [start|stop]"
	exit 1
	;;
esac
