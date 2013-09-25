(ns monitor.main
    (:require
        [monitor.tools :as tool]
    )    
    (:gen-class)
)

(defn -main []
    (println "start monitor")
    (tool/check 
        "agent.main" 
        " nohup java -cp .:agent.jar agent.main 2>&1 >>agent.log & " 
        5000
    )   
)

