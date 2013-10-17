(ns query-server.agent-scheduler
    (:require 
        [query-server.agent-driver :as ad]
    )
    (:gen-class)
)

(defn- new-agent-run [agentMap]
    (ad/new-agent 
        (:id agentMap) 
        (:AgentName agentMap) 
        (:AgentUrl agentMap)
        (:AccountID agentMap)
    )
    (ad/get-agent-data 
        (:id agentMap)
        (:AgentUrl agentMap)
        "both"
    )
    (ad/chage-agent-stat
        (:id agentMap)
        "normal"
    )
)

(defn new-agent-check []
    (let [agent-list (ad/get-agent-list "new")]
        (map new-agent-run agent-list)
    )
    (Thread/sleep 600000)
    (recur)
)

(defn inc-data-check []
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