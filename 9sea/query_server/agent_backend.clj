(ns query-server.agent-backend
(:require 
  [clojure.java.jdbc :as jdbc]
  [korma.core :as orm ]
  [query-server.config :as config]
  [clojure.java.io :as io]
  [query-server.core :as shark]
  [clojure.data.json :as json]
  [clj-time.core :as time]
  [query-server.mysql-connector :as mysql]
  [query-server.hive-adapt :as hive]
  )
(:use 
  [korma.db]
  [korma.config]
  [logging.core :only [defloggers]]
  [clj-time.coerce]
  [clojure.set]
  [clj-time.format]
  )
;(:import [org.apache.hadoop.hive.ql.parse ParseDriver])
)

(def agent-status ["nil" "no-sync" "running" "stopped" "abandoned"])

(defloggers debug info warn error)

(defn check-agent-name?
   [agent-name account-id]
 (let [res (orm/select mysql/TblAgent (orm/fields :id) (orm/where {:AgentName agent-name :AccountID account-id}))]
   (if (empty? res) 
    (do
    	(info "no duplicate agent-name found")
         nil
    )
     (-> res (first) (:id)))
 )
)

(defn check-agent-url?
   [agent-url account-id]
 (let [res (orm/select mysql/TblAgent (orm/fields :id) (orm/where {:AgentUrl agent-url :AccountID account-id}))]
   (if (empty? res) 
    (do
    	(info "no duplicate agent-url found")
         nil
    )
     (-> res (first) (:id)))
 )
)


(defn select-agent
  [account-id]
  (let [rs (orm/exec-raw (mysql/get-korma-db) ["select id,AgentState as status,AgentUrl as url,
                                               date_format(LastSyncTime,'%Y-%m-%d %H:%i:%s') as synctime,
                                               AgentName as name from TblAgent where AccountID=?" 
                                               [account-id]] :results)
       ]
  (for [
        x rs
        :let [i (:status x)
              s_time (if (nil? (:synctime x))
                       -1
                       (to-long 
                       (parse (formatter "yyyy-MM-dd H:mm:ss") (:synctime x)))
                     )
                      
              _ (info "recent-sync time:" s_time)
              ]
        ]
        (if (= -1 s_time)
          (assoc x :status (get agent-status i) )

          (assoc x :status (get agent-status i) :recent-sync s_time)
        )
  ))
)

(defn delete-agent
  [agent-id]
  ; set the status to abandon
  (orm/update mysql/TblAgent (orm/set-fields {:AgentState 4}) (orm/where {:id agent-id}))
)


(defn add-agent
  [agent-name url account-id]
  (debug (format "add new agent: agent-name:%s agent-url:%s account-id:%s" agent-name url account-id))

  (orm/insert mysql/TblAgent 
              (orm/values [{:AgentName agent-name :AgentUrl url :AccountID account-id :AgentState 1}])
  )
)

(defn edit-agent
  [agent-id agent-name url]
  (orm/update mysql/TblAgent
              (orm/set-fields {:AgentName agent-name :AgentUrl url})
              (orm/where {:id agent-id})
  )
)

(defn no-sync-check?
  [agent-id]
  (let [rs (orm/select mysql/TblAgent
              (orm/where {:id agent-id :AgentState 1})
           )]
    (if (empty? rs)
      false
      true
    )
  )
)


(defn check-agent-id?
  [agent-id]
  (orm/dry-run (orm/select mysql/TblAgent (orm/where {:id agent-id})))
 (let [res (orm/select mysql/TblAgent (orm/where {:id agent-id}))
       raw-query-str (str "select * from TblAgent where id=" agent-id)
;       rs (orm/exec-raw (mysql/get-korma-db) raw-query-str)
       ]
 ;      (orm/dry-run (orm/select mysql/TblAgent (orm/fields :id) (orm/where {:id agent-id})))
   (debug (format "check-agent-id? agent-id:%d raw-string:%s" agent-id raw-query-str))
   (if (empty? res) 
    (do
    	(debug "no agent found")
         nil
    )
     (-> res (first) (:id)))
 )
)

