(ns query-server.query-backend
(:require 
  [clojure.java.jdbc :as jdbc]
  [korma.core :as orm ]
  [query-server.conf :as conf]
  [clojure.java.io :as io]
  [query-server.core :as shark]
  [clojure.data.json :as json]
  [clj-time.core :as time]
  [query-server.mysql-connector :as mysql]
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

(defloggers debug info warn error)

(defn check-query-name
   [query-name]
 (let [res (orm/select mysql/TblSavedQuery (orm/fields :QueryId) (orm/where {:QueryName query-name}))]
   (if (empty? res) 
    (do
    	(info "no duplicate name found")
         nil
    )
     (-> res (first) (:QueryId)))
 )
)
 
(defn check-query-ownership
  [qid user-id]
  (let [res (orm/select mysql/TblSavedQuery (orm/fields :QueryId) 
            (orm/where {:QueryId qid :CreateUserId user-id}))
       ]
  (if (empty? res)
    nil
    (-> res (first) (:QueryId))
  ))
)

(defn delete-saved-query
  [qid]
  (orm/delete mysql/TblSavedQuery (orm/where {:QueryId qid}))
)

(defn update-query-status
   [query-id stat]
 ;   (sql/with-connection db
 ;     (sql/update-values :mysq/TblHistoryQuery ["QueryId=?" query-id] {:ExecutionStatus (status-convert stat)})
 ;   )
  ;   mysq/TblHistoryQuery
  (let [sql-str (str " update TblHistoryQuery set ExecutionStatus=" (mysql/status-convert stat)" where QueryId="query-id)]
   (orm/exec-raw mysql/korma-db sql-str)
  )
)

(defn update-history-query
  [q-id stats error url end-time duration]
  (let [sql-str (format "update TblHistoryQuery set ExecutionStatus=%d, Error=%s, Url=\"%s\", EndTime=\"%s\", Duration=%d where QueryId=%d"
                        (mysql/status-convert stats) 
                        error 
                        url
                        (unparse (formatters :date-hour-minute-second) (from-long end-time))
                        duration
                        q-id
                )] 
    (prn "update-history-query" sql-str)
    (orm/exec-raw mysql/korma-db sql-str)
  )
)

(defn select-saved-queries
  [user-id]
  (
   orm/select mysql/TblSavedQuery (orm/fields :QueryId :QueryName :AppName :AppVersion :DBName :QueryString)
    (orm/where {:CreateUserId user-id})
  )
)

(defn select-history-queries
  [user-id]
)


(defn save-query
  [query-name app-name app-ver db query-str submit-time user-id]
  (
   info (format "save query: name %s; string %s;submit-time %s; user-id %s" query-name query-str submit-time user-id)
   )
  	   (orm/insert mysql/TblSavedQuery
              (orm/values [{:QueryName query-name 
			    :AppName app-name
			    :AppVersion app-ver
			    :DBName db
			    :QueryString query-str 
			    :SubmitTime submit-time 
			    :CreateUserId user-id}]))
)

(defn log-query
;save the submited query into db
  [query-str user-id submit-time]
 (let [res (orm/insert mysql/TblHistoryQuery
             (orm/values [{:QueryString query-str
                           :SubmitUserId user-id
                           :SubmitTime submit-time
			   :ExecutionStatus (mysql/status-convert "submitted")}]
             ))]
   (let [q-id (re-seq #"[0-9]+" (str res))]
    (Integer/parseInt (first q-id))
   )
 )
)
  
(defn get-application
  [account-id]
  (let [app (orm/select mysql/TblApplication
          (orm/where {:AccountId account-id}))]
    (if (empty? app)
      (println "application not found!"
      nil)
      (-> app )
        )
      )
    )

(defn get-query-result
  [query-id]
  (let [result (shark/get-result query-id)]
    (-> result)
  )
)

(defn submit-query
  [user-id app-name app-version db-name query-str]
    ;if-let [app-id (if-let [app (get-application account-id app-name)] (:application_id))]
    ;TODO we should replace table name with hive-table-name, 
    ; it is included: parse hql to AST, replace TOK_TABNAME, then rewrite back to hql
    
    ;insert into history query
    (let [submit-time (System/currentTimeMillis)
          submit-date (unparse (formatters :date-hour-minute-second) (from-long submit-time))
         ; query-id (hash start-time)]
          query-id (log-query query-str user-id submit-date)
         ]
      (println (format "queryid: %s start-time: %s" query-id submit-date))
      (shark/submit-query query-id query-str update-history-query)
      (-> query-id)
    )
)

(defn get-applications
  [account-id]
  (let [ res (orm/select mysql/TblApplication (orm/fields :ApplicationName) (orm/where {:AccountId account-id}))]
   ( if(empty? res)
    nil
    (vals res)
  ))
)

(defn get-all-tables-in-app
  [app-name]
  (let [res (orm/select mysql/TblMetaStore (orm/where {:AppName app-name}))]
    (if(empty? res)
      nil
      res
      )
  )
 )

(defn get-hive-cols [tn]
    (let [mainSql (str "DESCRIBE " tn )
            res (shark/run-shark-query' "" mainSql)
        ]
      (prn "get-hive-cols" res)
      (map #(select-keys % [:col_name :data_type]) res)
    )
)

(defn transform-cols
  [raw-result]

  (let [
      ;  map-result (apply hash-map raw-result)
       res (map #(rename-keys % {:col_name :name :data_type :type}) raw-result)
    ;    r (rename-keys map-result {:col_name :name :data_type :type})
       ]
    res
  )
)

(defn get-table-schema
  [schema]
  (let [hive-table (get schema :hive_name)
        table-name (get schema :TableName)
        cols (transform-cols (get-hive-cols hive-table))]
    (prn "table column" cols)
    {
     :type "table"
     :name table-name
     :hive-name hive-table
     :children (into [] cols)
    }
  )
)

(defn add-children 
; return a vector of children based on group-set items
  [group-set raw-data]
  (if (empty? group-set)
    ; deal with table
    (do
    (vec (map #(-> % (get-table-schema)) (nth (vals raw-data) 0))) 
      )

  (let [group-key (first group-set)
        group-data (into {} (map #(group-by group-key %) (vals raw-data))) 
        res (transient [])
        ]
    (doseq [g group-data]
     (conj! res
    (assoc {:type "namespace" :name (first g)} :children (add-children (rest group-set) {(first g) (second g)})
            ))
      )
    (persistent! res)
 ))
)

(defn make-app-tree
  [app-name ver-db-tables]
  (debug "app-name: " app-name "ver-db-tables:" ver-db-tables)
  (let [group-vec [:AppVersion :DBName]
        app-tree {:type "namespace" :name app-name}
        group-data (group-by :AppName ver-db-tables)
        ]
   (let [children (add-children group-vec group-data) 
     app-tree2 (assoc app-tree :children children)]
     
   (info "app-tree" app-tree2)
    app-tree2
  ))
)

(defn select-meta
  [app-name]
  (let [res (orm/select mysql/TblMetaStore (orm/where {:AppName app-name}))]
    (prn "meta ret:" res)
    (if(empty? res)
      nil
      res
    )
))

(defn app-mapper
  [app]
  (let [app-name (get app :ApplicationName)
        tree (make-app-tree app-name (select-meta app-name))
        ]
    (debug "app-tree is " tree)
    tree
  )
)

(defn get-metastore-tree
  [account-id]
   (let [apps (get-application account-id)
         ]
     (if (nil? apps)
       nil
       ; get tree structure of all applications
      (do
      (prn "app is" apps)
     (let[ret (map app-mapper apps)]
       (prn "metastore-tree ret" ret)
       ret)
     ; (vector? apps)
    ;  (app-mapper (nth apps 0))
     )
   ))
)





