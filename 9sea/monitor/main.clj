(ns monitor.main
    (:require
        [monitor.tools :as tool]
    )    
    (:gen-class)
)

(defn now [] (new java.util.Date))

(defn -main []
    (println "start monitor" (now))
    (tool/check 
        "agent.main" 
        " nohup java -cp .:agent.jar agent.main 2>&1 >>agent.log & " 
        5000
    )   
)

