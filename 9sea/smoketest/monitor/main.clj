(ns smoketest.monitor.main
    (:require
        [monitor.tools :as mt]
    )
    (:use 
        testing.core
    )
)

(def atag "agent.main")
(def abash "./start_agent.sh" )

(def mtag "\" monitor.main\"")
(def mbash "./start_monitor.sh" )


(defn- killone [tag]
    (let [
            cmd  (into-array 
                    ["/bin/sh" "-c" 
                        (str 
                            "ps -ef |grep -v grep |grep " 
                            tag 
                            "| awk '{print $2}' | xargs  kill -9 "
                        )
                    ]
                )
            run (Runtime/getRuntime)
            p (.exec run cmd)

        ]
        p
    )
)

(defn- kill_all []
    (killone atag)
    (killone mtag)
)



(suite "monitor and agent start each other"
    (:fact agent_start_monitor
        (do
            (kill_all)   
            (Thread/sleep 2000)
            (assert
                (nil? (mt/check-process mtag))
            )
            (assert
                (nil? (mt/check-process atag))
            )            
            (mt/restart-process abash)
            (assert
                (not (nil? (mt/check-process atag)))
            )
            (Thread/sleep 20000)          
            (assert
                (not (nil? (mt/check-process mtag)))
            )            
            (println "test end")
        )
        :is
        nil
    )
    (:fact monitor_start_agent
        (do
            (kill_all)
            (Thread/sleep 2000)          
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
            (Thread/sleep 20000)
            (assert
                (not (nil? (mt/check-process atag)))
            )            
            (println "test end")
        )
        :is
        nil
    ) 
    (:fact restart_muti_times
        (do
            (kill_all)
            (Thread/sleep 2000)          
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
            (Thread/sleep 20000)
            (assert
                (not (nil? (mt/check-process atag)))
            )
            (killone mtag)
            (Thread/sleep 2000)          
            (assert
                (nil? (mt/check-process mtag))
            )
            (Thread/sleep 20000)
            (assert
                (not (nil? (mt/check-process mtag)))
            )
            (killone atag)
            (Thread/sleep 2000)          
            (assert
                (nil? (mt/check-process atag))
            )
            (Thread/sleep 20000)
            (assert
                (not (nil? (mt/check-process atag)))
            )
            (killone mtag)
            (Thread/sleep 2000)          
            (assert
                (nil? (mt/check-process mtag))
            )
            (Thread/sleep 20000)
            (assert
                (not (nil? (mt/check-process mtag)))
            )
            (killone atag)
            (Thread/sleep 2000)          
            (assert
                (nil? (mt/check-process atag))
            )
            (Thread/sleep 20000)
            (assert
                (not (nil? (mt/check-process atag)))
            )            
            (println "test end")
        )
        :is
        nil
    )       
)
