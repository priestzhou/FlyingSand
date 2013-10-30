pw=`pwd`
jhome=$pw/jre1.7.0_40
JAVA_HOME=$jhome $jhome/bin/java -d64 -server -Xloggc:$pw/monitor_gc.log -XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:+PrintGCDateStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$pw/monitor.hprof -XX:MaxPermSize=128m -Xmx512M -cp .:monitor.jar monitor.main -webport 8082 2>&1 >> $pw/monitor.log & 
