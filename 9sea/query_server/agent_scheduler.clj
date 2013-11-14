(ns query-server.agent-scheduler
    (:require 
        [query-server.agent-driver :as ad]
        [query-server.config :as config]
    )
    (:use
        [utilities.core :only (except->str)]
        [logging.core :only [defloggers]]
    )    
    (:gen-class)
)

(defloggers debug info warn error)

(defn- new-agent-run [agentMap]
    (info "new-agent-run" )
    (try
        (ad/new-agent 
            (:id agentMap) 
            (:agentname agentMap) 
            (:agenturl agentMap)
            (:accountid agentMap)
        )
        (ad/get-agent-data 
            (:id agentMap)
            (:agentname agentMap) 
            (:agenturl agentMap)
            "both"
        )
        (ad/change-agent-stat
            (:id agentMap)
            "normal"
        )
        (catch Exception e 
            (error "add new agent fail " :except (except->str e))
        )
    )
)

(defn new-agent-check []
    (info "new-agent-check")
    (try 
        (let [agent-list (ad/get-agent-list "new")
            
            ]
            (doall (map new-agent-run agent-list))
        )
        (catch Exception e 
            (error " new agent check fail " :except (except->str e))
        )
    )
    
    (Thread/sleep (config/get-key :new-agent-check-interval))
    (recur)
)

(defn inc-data-check []
    (info "inc-data-check")
    (try
        (let [agent-list (ad/get-agent-list "normal")]
            (doall (map  
                #(ad/get-agent-data 
                    (:id %)
                    (:agentname %) 
                    (:agenturl %)
                    "inc"
                )
                agent-list
            ))
        )
        (catch Exception e 
            (error " inc-data-check fail " :except (except->str e))
        )        
    )

    (Thread/sleep (config/get-key :inc-data-check-interval))
    (recur)
)

(defn all-data-check []
    (info "all-data-check")
    (try 
        (let [agent-list (ad/get-agent-list "normal")]
            (doall (map  
                #(ad/get-agent-data 
                    (:id %)
                    (:agentname %) 
                    (:agenturl %)
                    "all"
                )
                agent-list
            ))
        )
        (catch Exception e 
            (error " all-data-check fail " :except (except->str e))
        )
    )
    (Thread/sleep (config/get-key :all-data-check-interval))
    (recur)
)

(defn mismatch-agent-check []
    (try 
        (let [agent-list (ad/get-agent-list "mismatch")]
            (doall (map  
                #(ad/check-mismatch-agent
                    (:id %)
                    (:appname %) 
                    (:appversion %)
                    (:agentname %) 
                    (:agenturl %)
                    (:accountid %)                    
                )
                agent-list
            ))
        )
        (catch Exception e 
            (error " mismatch-agent-check fail " :except (except->str e))
        )          
    )
    (Thread/sleep (config/get-key :mismatch-agent-check-interval))
    (recur)
)

(defn start_agent_future []
    (debug "start_agent_future" :key (config/get-key :agent-start-flag))
    (when (config/get-key :agent-start-flag)    
        (future 
            (new-agent-check)
        )
        (future 
            (inc-data-check)
        )
        (future
            (do
                (Thread/sleep 60000)
                (all-data-check)
            )
        )
        (future
            (mismatch-agent-check)
        ) 
    )
)