pw=`pwd`
jpath=$pw/jre1.7.0_40/bin/java
JAVA_HOME=$jpath nohup $jpath -cp .:agent.jar agent.main -webport 8081 2>&1 >>agent.log & 
