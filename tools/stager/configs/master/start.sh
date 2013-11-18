#!/bin/bash

java -Xloggc:./gc.log -XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:+PrintGCDateStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./master.hprof -XX:MaxPermSize=256m -Xmx1G -cp .:master.jar master.main 2>&1 > /dev/null &
