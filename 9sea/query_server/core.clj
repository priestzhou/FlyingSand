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
)
;; (:use [logging.core :only [deffloggers]])
)

(defloggers debug info warn error)

(def ^:private result-map (atom {}))
(def ^:private result-count-map (atom {}))

(def ^:private hive-db (ref {:classname "org.apache.hadoop.hive.jdbc.HiveDriver"
                             :subprotocol "hive"
                             :user ""
                             :password ""
                            }
                       )
)

(def ^:private hive-conn-str (ref ""))

(defn set-hive-db
  [host port]
  (dosync
    (alter hive-db conj {:subname (format "//%s:%s" host port)})
    (ref-set hive-conn-str (format "jdbc:hive://%s:%s" host port))
  )
  (prn "hive-db" @hive-db)
  (prn "hive-conn-str" @hive-conn-str)
)

(defn get-hive-db
  []
  (-> @hive-db)
)

(defn get-hive-conn-str
  []
  (-> @hive-conn-str)
)

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
          conn (get-hive-conn (get-hive-conn-str) "" "")
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
    (let [conn (get-hive-conn (get-hive-conn-str) "" "")
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
  [q-id stats ret-result error-message csv-url]
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
             :log "query is succeeded!"
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

(defn run-shark-query'
  [q-id query-str]
  (try
   (debug "run-shark-query" :qid q-id)
    (prn (str "query-str:" query-str))
  ( sql/with-connection (get-hive-db)
    (sql/with-query-results rs [query-str]
      (doall rs)
      ))
  (catch Exception exception
    (error "run shark query:" (util/except->str exception))
    ; we should seperate exception
    (update-result-map q-id "failed" nil exception)
    ))
)

(defn run-shark-query
  [context q-id query-str]
  (try
   (debug "run-shark-query" :qid q-id)
    (let [
      dfg (trans/parse-sql context query-str)
      hive-query (trans/dump-hive context dfg)
          ret-rs (execute-query hive-query)]
      (future (execute-count-query q-id hive-query))
      (process-query q-id ret-rs)
    )
  (catch Exception ex
  (do
   (error "can't execute shark query:" (util/except->str ex))
    (update-result-map q-id "failed" nil (.getMessage ex) nil )
    ; we should seperate exception
    )))
)

(defn submit-query
  [context q-id query-str]
 ;; {:pre [not (str/blank? query-str)]}
  ;; TODO add query syntax check, only allow select clause
  (debug "submit-query" :qid q-id :query query-str)
  (update-result-map q-id "running" {} nil nil)
  (future (run-shark-query context q-id query-str))
)

(defn get-result
  [q-id]
  (get @result-map q-id)
)

(defn clear-result-map
  [q-id]
  (swap! result-map dissoc [q-id])
)
