(ns query-server.query-backend
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
            (orm/where {:QueryId qid :CreatedUserId user-id}))
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
   (orm/exec-raw (mysql/get-korma-db) sql-str)
  )
)

(defn update-history-query
  [q-id stats error url end-time duration]
  (let [sql-str (format "update TblHistoryQuery set ExecutionStatus=%d, Error=%s, Url=\"%s\", EndTime=\"%s\", Duration=%d where QueryId=%d"
                        (mysql/status-convert stats) 
                        error 
                        url
                        (unparse (formatters :date-hour-minute-second)
                                   (from-long end-time))
                        duration
                        q-id
                )] 
    (prn "update-history-query" sql-str)
    (orm/exec-raw (mysql/get-korma-db) sql-str)
  )
)

(defn select-saved-queries
  [user-id]
  (
   orm/exec-raw (mysql/get-korma-db)
                 ["select QueryId as id,QueryName as name,AppName as app,AppVersion as version,
                  DBName as db,QueryString as query from TblSavedQuery where CreatedUserId=? 
                  order by SubmitTime desc" [user-id]] :results

  )
)

(defn select-history-queries
  [user-id]
  (let [rs (orm/exec-raw (mysql/get-korma-db) [
                          "select QueryId as id,QueryString as query,ExecutionStatus as status,
                          date_format(SubmitTime,'%Y-%m-%d %H:%i:%s') as submit_time,Url as url,
                          Duration as duration from TblHistoryQuery where SubmitUserId=? 
                          order by SubmitTime desc limit 20" [user-id]] :results)
       ]
       (for [
        x rs
        :let [_ (prn "select-history:" (:submit_time x)) i (:status x) 
              s_time (to-long (parse (formatter "yyyy-MM-dd H:mm:ss") (:submit_time x)))]
        ]
        (assoc x :status (get mysql/query-status i) :submit_time s_time)
       )
    )
)

(defn save-query
  [query-name app-name app-ver db query-str submit-time user-id]
  (info "save-query" :query-name query-name :submit-time submit-time :user-id user-id)
  	   (orm/insert mysql/TblSavedQuery
              (orm/values [{:QueryName query-name 
			    :AppName app-name
			    :AppVersion app-ver
			  ;  :DBName db
			    :QueryString query-str 
			    :SubmitTime submit-time 
			    :CreatedUserId user-id}]))
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
  (let [account-prefix (str account-id ".%")
        app (orm/select mysql/TblMetaStore
           (orm/fields [:AppName])
           (orm/modifier "DISTINCT")
          (orm/where {:NameSpace [like account-prefix]}))]
    (if (empty? app)
      (warn "application not found!")
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
  [context user-id query-str]
    ;if-let [app-id (if-let [app (get-application account-id app-name)] (:application_id))]
    ;TODO we should replace table name with hive-table-name, 
    ; it is included: parse hql to AST, replace TOK_TABNAME, then rewrite back to hql
    
    ;insert into history query
    (let [submit-time (System/currentTimeMillis)
          submit-date (unparse (formatters :date-hour-minute-second) 
                                 (from-long submit-time))
         ; query-id (hash start-time)]
          query-id (log-query query-str user-id submit-date)
         ]
      (debug "submit-query" :queryid query-id :start-time submit-date)
      (shark/submit-query context query-id query-str)
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
            res (hive/run-shark-query' "" mainSql)
        ]
      (prn "get-hive-cols" res)
      (map #(select-keys % [:col_name :data_type]) res)
    )
)

(defn add-children 
; return a vector of children based on group-set items
  [group-set raw-data]
  (prn "raw-data" raw-data)
  (if (empty? group-set)
    ; deal with table
    (do
     (vec (map #(-> % (hive/get-table-schema)) (nth (vals raw-data) 0))) 
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
;  (debug "make-app-tree" :app-name app-name :ver-db-tables ver-db-tables)
  (let [group-vec [:AppVersion]
        app-tree {:type "namespace" :name app-name}
        group-data (group-by :AppName ver-db-tables)
        ]
   (let [children (add-children group-vec group-data) 
     app-tree2 (assoc app-tree :children children)]
     
    app-tree2
  ))
)

(defn select-meta
  [app-name account-id]
  (let [account-prefix (str account-id ".%")
        res (orm/select mysql/TblMetaStore 
                        (orm/where {:AppName app-name :NameSpace [like account-prefix]}))]
    (prn "meta ret:" res)
    (if(empty? res)
      nil
      res
    )
  )
)

(defn app-mapper
  [app account-id]
  (let [app-name (get app :AppName)
        _ (prn "app-mapper: account-id is" account-id)
        tree (make-app-tree app-name (select-meta app-name account-id))
        ]
    ; (debug "app-tree" :tree tree)
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
        (let[ret (map (fn [i] (app-mapper i account-id)) apps)]
          (prn "metastore-tree ret" ret)
          ret
        )
      )
   ))
)





