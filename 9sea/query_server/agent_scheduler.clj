(ns query-server.agent-scheduler
    (:require 
        [query-server.agent-driver :as ad]
    )
    (:gen-class)
)

(defn- new-agent-run [agentMap]
    (println "new-agent-run")
    (try
        (ad/new-agent 
            (:id agentMap) 
            (:agentname agentMap) 
            (:agenturl agentMap)
            (:accountid agentMap)
        )
        (ad/get-agent-data 
            (:id agentMap)
            (:agenturl agentMap)
            "both"
        )
        (ad/chage-agent-stat
            (:id agentMap)
            "normal"
        )
        (catch Exception e 
            (println e)
        )
    )
)

(defn new-agent-check []
    (println "new-agent-check")
    (try 
        (let [agent-list (ad/get-agent-list "new")
            _ (println agent-list)
            ]
            (doall (map new-agent-run agent-list))
        )
        (catch Exception e 
            (println e)
            (println (.printStackTrace e))
        )
    )
    
    (Thread/sleep 600000)
    (recur)
)

(defn inc-data-check []
    (println "inc-data-check")
    (let [agent-list (ad/get-agent-list "normal")]
        (map  
            #(ad/get-agent-data 
                (:id %)
                (:AgentUrl %)
                "inc"
            )
            agent-list
        )
    )
    (Thread/sleep 300000)
    (recur)
)

(defn all-data-check []
    (println "all-data-check")
    (let [agent-list (ad/get-agent-list "normal")]
        (map  
            #(ad/get-agent-data 
                (:id %)
                (:AgentUrl %)
                "all"
            )
            agent-list
        )
    )
    (Thread/sleep 86400000)
    (recur)
)