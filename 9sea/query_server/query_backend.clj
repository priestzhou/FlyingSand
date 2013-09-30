(ns query-server.query-backend
(:require 
  [clojure.java.jdbc :as sql]
  [korma.core :as orm ]
  [query-server.conf :as conf]
  [clojure.data.json :as json]
  [clojure.java.io :as io]
  [query-server.core :as shark]
  [clojure.data.json :as json]
  )
(:use 
  [korma.db]
  )
;(:import [org.apache.hadoop.hive.ql.parse ParseDriver])
;; (:use [logging.core :only [deffloggers]])
)

(def my-db
  {:classname "com.mysql.jdbc.Driver"
   :subprotocol "mysql"
   :subname "192.168.1.101:3306/meta"
   :user "root"
   :password "fs123"})

(defdb korma-db my-db)

(orm/defentity application
    (orm/pk :application_id)
    (orm/database korma-db)
)

(orm/defentity metastore
  (orm/pk :application_version_id)
  (orm/database korma-db)
  )

(orm/defentity history_query
  (orm/pk :query_id)
  (orm/database korma-db)
  )


  
(defn save-query
  [query-str start-time]
  (orm/insert history_query
              (orm/values [{:query_str query-str :finish_time start-time}]))
  )
  
(defn get-application
  [account-id app-name]
  (let [app (orm/select application
          (orm/where {:acount_id account-id :application_name app-name}))]
    (if (empty? app)
      (println "application not found!"
      nil)
      (-> app (first) )
        )
      )
    )

(defn get-hive-table
  [app-id app-version db-name]
  )
  


(defn get-query-result
  [query-id]
  (let [result (shark/get-result query-id)]
   println result
    (-> result)
))

(defn submit-query
  [account-id app-name app-version db-name query-str]
    ;if-let [app-id (if-let [app (get-application account-id app-name)] (:application_id))]
    ;TODO we should replace table name with hive-table-name, 
    ; it is included: parse hql to AST, replace TOK_TABNAME, then rewrite back to hql
    
    ;insert into history query
    (let [start-time (java.lang.System/currentTimeMillis)
          query-id (hash start-time)]
      (println query-id)
      (shark/submit-query query-id query-str)
      (-> query-id)
    )
  )
    
    
    
    
    
  
      
  

