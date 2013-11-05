(ns query-server.core
(:require
  [clojure.java.jdbc :as sql]
  [clojure.string :as str]
  [query-server.config :as config]
  [clojure.data.json :as json]
  [korma.core :as orm ]
  [clojure.java.io :as io]
  [query-server.mysql-connector :as mysql]
  [clj-time.core :as time]
  [parser.translator :as trans]
  [clojure.data.csv :as csv]
  [utilities.core :as util]
  [query-server.agent-driver :as ad]
  [query-server.hive-adapt :as hive]
  )
(:import [com.mchange.v2.c3p0 ComboPooledDataSource DataSources PooledDataSource]
         [java.io IOException]
         [java.io File]
         [java.net URI]
         [java.sql BatchUpdateException DriverManager
               PreparedStatement ResultSet SQLException Statement Types]
         [java.util Hashtable Map Properties]
         [javax.naming InitialContext Name]
         [javax.sql DataSource]
)


(:use
  [korma.db]
  [korma.config]
  [logging.core :only [defloggers]]
  [clj-time.coerce]
  [clojure.set]
  [clj-time.format]
  [clojure.java.shell :only [sh]]
  [query-server.agent-driver :only (sha1-hash)]
)
;; (:use [logging.core :only [deffloggers]])
)

(defloggers debug info warn error)

(def ^:private result-map (atom {}))
(def ^:private result-count-map (atom {}))

(defn pooled-spec
  "return pooled conn spec.
   Usage:
     (def pooled-db (pooled-spec db-spec))
     (with-connection pooled-db ...)"
  [{:keys [classname subprotocol subname user password] :as other-spec}]
  (let [cpds (doto (ComboPooledDataSource.)
               (.setDriverClass classname)
               (.setJdbcUrl (str "jdbc:" subprotocol ":" subname))
               (.setUser user)
               (.setPassword password))]
    {:datasource cpds}))

;(def pooled-db (pooled-spec db))

(defn run-sql-query [db-spec query]
   (sql/with-connection db-spec
      (sql/do-commands query)
   )
)

(defn get-hive-conn
    [url user password]
    (clojure.lang.RT/loadClassForName "org.apache.hadoop.hive.jdbc.HiveDriver")
    (DriverManager/getConnection url user password)
)

(defn- execute-count-query
  [q-id sql-str]
  (try
    (let [count-sql-str (str "select count(*) cnt from (" sql-str ") a")
          conn (get-hive-conn (hive/get-hive-conn-str) "" "")
          ^Statement stmt (.createStatement conn)
          ^ResultSet rs-count (.executeQuery stmt count-sql-str)
          row-count (if (.next rs-count) (.getInt rs-count "cnt") 0)
          ]
        (debug "count is:" row-count)
        (swap! result-count-map update-in [q-id] assoc :count row-count)
    )
  (catch SQLException ex
    (error "fail to execute count query" (util/except->str ex))
    (swap! result-count-map update-in [q-id] assoc :count -1)
  ))
)

(defn execute-query
  [sql-str]
    (let [conn (get-hive-conn (hive/get-hive-conn-str) "" "")
        result-size (config/get-key :max-result-size)
        _ (prn "result-size:" result-size)
        ^Statement stmt (.createStatement conn)
        ^ResultSet rs (.executeQuery stmt sql-str)
        rsmeta (.getMetaData rs)
        idxs (range 1 (inc (.getColumnCount rsmeta)))
        col-name (->> idxs (map (fn[i] (.getColumnLabel rsmeta i))))
        row-values (fn [] (map (fn [i] (.getString rs i)) idxs))
        rows (fn rowfn [] (when (.next rs)
                            (cons (vec (row-values)) (lazy-seq (rowfn)))))
        result-4-save (doall (take result-size (rows)))
        _ (debug (str result-4-save))
        ]

    {:titles (vec col-name)
     :values result-4-save
    }

  )
)

(defn persist-query-result
  [result-set filename]
  (let [titles (:titles result-set)
        values (:values result-set)
        done-file (str filename ".done")
       ]
    (try
      (with-open [wrtr (io/writer filename)]
        (csv/write-csv wrtr (cons titles values))
      )
      (spit done-file "")
    (catch IOException e
      (error "can't save query result:" (util/except->str e))
    )
    )
  )
)

(defn update-history-query
  [q-id stats error url end-time duration]
  (let [sql-str (format "update TblHistoryQuery set ExecutionStatus=%d, Error=\"%s\", Url=\"%s\", EndTime=\"%s\", Duration=%d where QueryId=%d"
                        (mysql/status-convert stats)
                        error
                        url
                        (unparse (formatters :date-hour-minute-second)
                                  (from-long end-time))
                        duration
                        q-id
                )]
    (debug "update-history-query" :sql-str sql-str)
    (run-sql-query (mysql/get-mysql-db) sql-str)
   ; (orm/exec-raw mysql/korma-db sql-str)
  )
)

(defn transform-result
  [raw-result]

  (let [ret-rs-size (config/get-key :ret-result-size)
        _ (prn "ret-rs-size" ret-rs-size)
        titles (get raw-result :titles)
        values (doall (take ret-rs-size (get raw-result :values)))
       ]

    {
      :titles titles
      :values values
    }
  )
)

(defn update-result-map
  [q-id stats ret-result error-message csv-url & {:keys [log-message] :or {log-message nil}}]
  (debug "update status" :qid q-id :stats stats :error error-message)
  (let [start-time (:submit-time (@result-map q-id))
        cur-time (System/currentTimeMillis)
        duration (if (nil? start-time) 0 (- cur-time start-time))
        ]
    (case stats
     "running" (swap! result-map update-in [q-id] assoc
             :status stats
             :submit-time cur-time
             :progress [0 1]
             :log "query is running"
             :result ret-result)
     "succeeded"(swap! result-map  update-in [q-id] assoc
             :status stats
             :end-time cur-time
             :progress [1 1]
             :log (if (nil? log-message) "query is succeeded!" log-message)
             :duration duration
             :url csv-url
             :count  (ret-result :count)
             :result (select-keys ret-result [:titles :values]))
     "failed" (swap! result-map  update-in [q-id] assoc
             :status stats
             :end-time cur-time
             :progress [1 1]
             :log "query is failed!"
             :duration duration
             :error error-message
             :result ret-result)
    )
     (update-history-query q-id stats error-message csv-url cur-time duration)
  )
)

(defn- get-count-by-qid
  [qid]
  (while (nil? (:count (@result-count-map qid))))
  (-> (:count (@result-count-map qid)))
)

(defn process-query
  [q-id rs]
  (let [{{q-time :submit-time} q-id} @result-map
	ret-result (transform-result rs)
        filename (format "%s/%d_%d_result.csv" (config/get-key :result-file-dir) q-id q-time)
        csv-url (format "/result/%d_%d_result.csv" q-id q-time)
        count (get-count-by-qid q-id)
       ]
        (debug (format "result count for qid:%s is %s" q-id count))
    (let [res (assoc ret-result :count count)]
        (update-result-map q-id "succeeded" res nil csv-url)
        (future (persist-query-result rs filename))
    )
  )
)

(defn add-view-or-ctas
  [q-id context-info hql view-or-ctas]
  (try
    (let [ns (str (:accountid context-info) "." (:appname context-info)  
                  "." (:appversion context-info) "." (:database context-info) "." (:tablename context-info))
          _ (info "namespace:" ns)
          hive-name (:hive-name context-info)
          log-message (if (= view-or-ctas :view) 
                        (str "create view " (:tablename context-info) "successed!")
                        (str "create table " (:tablename context-info) "successed!")
                      )

         ]
    
      (cond
        (= view-or-ctas :view)
        (do
          (hive/create-view hive-name hql)
          (orm/insert mysql/TblMetaStore (orm/values [{:NameSpace ns :AppName (:appname context-info) :AppVersion (:appversion context-info)
                                             :DBName (:database context-info) :TableName (:tablename context-info) 
                                             :hive_name (:hive-name context-info) :NameSpaceType 3}]))
        )
        (= view-or-ctas :ctas)
        (do
          (hive/create-CTAS hive-name hql)
          (orm/insert mysql/TblMetaStore (orm/values [{:NameSpace ns :AppName (:appname context-info) :AppVersion (:appversion context-info)
                                             :DBName (:database context-info) :TableName (:tablename context-info) 
                                             :hive_name hive-name :NameSpaceType 2}]))
        )
      )
      (update-result-map q-id "succeeded" {:count 1 :titles nil :values nil} nil nil log-message)
    )
  (catch SQLException sql-ex
    (error "can't create view/table" (util/except->str sql-ex))
    (update-result-map q-id "failed" nil (str "can't create view/table " (:hive-name context-info) (.getMessage sql-ex)) nil)
    (update-result-map q-id "failed" nil (str "can't create view/table " (:hive-name context-info)) nil)
  )
  )
)

(defn drop-view-or-ctas
  [q-id context-info view-or-ctas]
  (try
    (let [ns (str (:accountid context-info) "." (:appname context-info)
                  "." (:appversion context-info) "." (:database context-info) "." (:tablename context-info))
          ns-type (orm/select mysql/TblMetaStore (orm/fields [:NameSpaceType]) (orm/where {:NameSpace ns}))
          drop-violate? (if (= (:NameSpace (first ns-type)) 2) true false)
          _ (info "namespace:" ns)
          log-message (if (= view-or-ctas :view) 
                        (str "drop view " (:tablename context-info) "successed!")
                        (str "drop table " (:tablename context-info) "successed!")
                      )
          ]
      (if drop-violate? 
        (update-result-map q-id "failed" nil (str "can't drop raw table " (:tablename context-info)) nil )
        (do
          (when (= :view view-or-ctas) 
            (hive/drop-view (:hive-name context-info))
          )
          (when (= :ctas view-or-ctas)
        ;we need to check if it is a raw mysql table
            (hive/drop-CTAS (:hive-name context-info))
          )
          (orm/delete mysql/TblMetaStore (orm/where {:NameSpace ns}))
          (update-result-map q-id "succeeded" {:count 0 :titles nil :values nil} nil nil log-message)
       )
     )
   )
  (catch SQLException sql-ex
    (error "can't drop view/table" (util/except->str sql-ex))
    (update-result-map q-id "failed" nil (str "can't drop view/table " (:hive-name context-info) (.getMessage sql-ex)) nil)
  ))
) 

(defn translate-query
  [context query-str]
  (let [
        dfg (trans/parse-sql context query-str)
        _ (info (str "dfg:" dfg))
        type (:type dfg)
       ]
    (cond (= type :select)
      {
       :clause-type :select-clause 
       :hql (trans/dump-hive context dfg)
      }
    
      (#{:create-view :create-ctas} type)
        {
         :clause-type :create-clause 
         :type (if (= type :create-view) :view :ctas)
         :tablename (str (first (:value (:name dfg))))
         :hql (str (trans/dump-hive context (:as dfg)))
        }
      
       (#{:drop-view :drop-ctas} type)
        {
       :clause-type :drop-clause
       :type (if (= type :drop-view) :view :ctas)
       :tablename (str (:name (:name dfg)))
       :hive-name (:hive-name (:name dfg))
        }
    )
  )
)

(defn gen-context-info
  [account-id context tablename tabletype hivename]
  (let [
        hive-name (
                   if-not (nil? hivename)
                    hivename
                   (do
                     (
                      if (= tabletype :view) 
                       (str "vn_" (ad/sha1-hash (str tablename)))
                       (str "tn_" (ad/sha1-hash (str tablename)))
                     )
                   )
                  )
       ]
    {
     :accountid account-id
     :appname (first (:default-ns context))
     :appversion (second (:default-ns context))
     :database "test"
     :tablename tablename
     :hive-name hive-name
    }
  )
)

(defn run-shark-query
  [context q-id account-id query-str]
  (try
   (debug "run-shark-query" :qid q-id)
    (prn "context:" (str context))
    (let [ 
           translated-result (translate-query context query-str)
           _ (prn "translated-result" (str translated-result))
           clause-type (:clause-type translated-result)
         ]
     (if (= clause-type :select-clause)
       (do
         (let [
               hive-query (:hql translated-result)
               ret-rs (execute-query hive-query)]
           (future (execute-count-query q-id hive-query))
           (process-query q-id ret-rs)
         )
       )
       (do
         (let [table-name (:tablename translated-result)
               table-type (:type translated-result)
               context-info (gen-context-info account-id context table-name table-type nil)
              ]
         (when (= clause-type :create-clause ) 
           (add-view-or-ctas q-id context-info (:hql translated-result) table-type )
         )
         (when (= clause-type :drop-clause )
           (drop-view-or-ctas q-id context-info table-type)
         )
       )
     )
    )
  )
  (catch Exception ex
  (do
   (error "can't execute shark query:" (util/except->str ex))
    (update-result-map q-id "failed" nil (.getMessage ex) nil )
    ; we should seperate exception
    )))
)

(defn submit-query
  [context q-id account-id query-str]
 ;; {:pre [not (str/blank? query-str)]}
  ;; TODO add query syntax check, only allow select clause
  (debug "submit-query" :qid q-id :query query-str)
  (update-result-map q-id "running" {} nil nil)
  (future (run-shark-query context q-id account-id query-str))
)

(defn get-result
  [q-id]
  (get @result-map q-id)
)

(defn clear-result-map
  [q-id]
  (swap! result-map dissoc [q-id])
)
