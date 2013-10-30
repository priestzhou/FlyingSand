pw=`pwd`
jhome=$pw/jre1.7.0_40
JAVA_HOME=$jhome $jhome/bin/java -d64 -server -Xloggc:$pw/agent_gc.log -XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:+PrintGCDateStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$pw/agent.hprof -XX:MaxPermSize=128m -Xmx512M -cp .:agent.jar agent.main -webport 8081 2>&1 >>agent.log &
