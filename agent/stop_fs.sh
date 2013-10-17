ps -ef |grep -v grep |grep "agent.main" | awk '{print $2}' | xargs  kill -9 
ps -ef |grep -v grep |grep " monitor.main" | awk '{print $2}' | xargs  kill -9 
