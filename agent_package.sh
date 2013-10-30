scons -j3 build/agent.jar build/monitor.jar
rm fs_agent.tar.gz
cp build/agent.jar build/monitor.jar agent/
tar -czf  fs_agent.tar.gz agent
