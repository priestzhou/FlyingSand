pw=`pwd`
jpath=$pw/jre1.7.0_40/bin/java
JAVA_HOME=$jpath nohup $jpath -cp .:monitor.jar monitor.main -webport 8082 2>&1 >> monitor.log & 
