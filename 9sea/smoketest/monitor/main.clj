(ns smoketest.monitor.main
    (:require
        [monitor.tools :as mt]
    )
    (:use 
        testing.core
    )
)

(def atag "agent.main")
(def abash " nohup java -cp .:agent.jar agent.main 2>&1 >>agent.log & " )

(def mtag "\" monitor.main\"")
(def mbash " nohup java -cp  .:monitor.jar monitor.main 2>&1 >>monitor.log & " )

(suite "monitor and agent start each other"
    (:fact agent_start_monitor
        (do
            (println (mt/check-process mtag))
            (println (nil? (mt/check-process mtag)))               
            (assert
                (nil? (mt/check-process mtag))
            )
            (assert
                (nil? (mt/check-process atag))
            )            
            (mt/restart-process mbash)
            (assert
                (not (nil? (mt/check-process mtag)))
            )
            (Thread/sleep 40000)
            (println (mt/check-process atag))
            (println (nil? (mt/check-process atag)))            
            (assert
                (not (nil? (mt/check-process atag)))
            )            
            (println "test end")
        )
        :is
        nil
    )
)

(suite "monitor and agent start each other"
    (:fact monitor_start_agent
        (do
            (assert
                (nil? (mt/check-process mtag))
            )            
            (assert
                (nil? (mt/check-process atag))
            )
            (mt/restart-process mbash)
            (assert
                (not (nil? (mt/check-process mtag)))
            )
            (Thread/sleep 15000)
            (assert
                (not (nil? (mt/check-process atag)))
            )            
            (println "test end")
        )
        :is
        nil
    )
)