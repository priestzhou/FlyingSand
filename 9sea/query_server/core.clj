(ns query-server.core
(:require 
  [clojure.java.jdbc :as sql]
  [clojure.string :as str]
  [query-server.conf :as conf]
  [clojure.data.json :as json]
  [korma.core :as orm ]
  [clojure.java.io :as io]
  [query-server.mysql-connector :as mysql]
  [clj-time.core :as time]
  )
(:import [com.mchange.v2.c3p0 ComboPooledDataSource DataSources PooledDataSource]
         [java.io IOException]
)

(:use
  [korma.db]
  [korma.config]
  [logging.core :only [defloggers]]
  [clj-time.coerce]
  [clojure.set]
  [clj-time.format]
)
;; (:use [logging.core :only [deffloggers]])
)

;(defloggers debug info warn error)

(let [config {}]
(let [host "192.168.1.100"
      dbname "200"
      port 10000
      user ""
      password ""]
  
(def db {:classname "org.apache.hadoop.hive.jdbc.HiveDriver"
          :subprotocol "hive"
          :subname  (format "//%s:%s" host port)
          :user user
          :password password})))

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
      (sql/with-query-results res query
         (doall res))))

(def ^:private result-map (atom {}))

(def ^:private max-result-size 400)
(def ^:private result-file-dir "/home/admin/fancong/9sea/publics/result")
(def ^:private ret-result-size 100)

(defn persist-query-result
  [result-set filename]
    (with-open [wrtr (io/writer filename)]
    (try
      (doseq [res result-set]
        (.write wrtr (format "%s\n" (str res)))
        )
    (catch IOException e
      (println e))
    (finally 
      (.close wrtr)
    )
    )
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
    (run-sql-query mysql/db sql-str)
   ; (orm/exec-raw mysql/korma-db sql-str)
  )
)

(defn transform-result
  [raw-result]

  (let [r (first raw-result)
        titles (keys r)
        values (vec (for [r raw-result] 
            (vec (for [t titles] (r t)))
        ))
       ]

    {
      :titles titles
      :values values
    }
  )
)

(defn update-result-map
  [q-id stats ret-result error-message csv-filename update-history-fn]
  (println "update status:" q-id stats)
  (println "error:" error-message)
  (let [start-time (:submit-time (@result-map q-id))
        cur-time (System/currentTimeMillis)
        duration (if (nil? start-time) 0 (- cur-time start-time))
        url (format "queries/%d/csv" q-id)
        ]
    (case stats
     "running" (swap! result-map update-in [q-id] assoc 
             :status stats 
             :submit-time cur-time
             :progress [0 1]
             :log "running"
             :result ret-result)
     "succeeded"(swap! result-map  update-in [q-id] assoc 
             :status stats 
             :end-time cur-time
             :progress [1 1]
             :log ""
             :duration duration
             :url url
             :result ret-result)
     "failed" (swap! result-map  update-in [q-id] assoc
             :status stats 
             :end-time cur-time
             :log "1 stage 1"
             :duration duration
             :error error-message
             :result ret-result)
    )
   ; (apply update-history-fn [q-id stats error-message url cur-time duration])
     (update-history-query q-id stats error-message url cur-time duration)
  )
)
      
(defn process-query
  [q-id result-set update-history-fn]
  (let [{{q-time :submit-time} q-id} @result-map
        ret-result (doall (take ret-result-size result-set))
	transformed-result (transform-result ret-result)
        result-to-save (atom (take max-result-size result-set))
        filename (format "%s/%d_%d_result.csv" result-file-dir q-id q-time)
       ]
        (update-result-map q-id "succeeded" transformed-result nil filename update-history-fn)
        (persist-query-result @result-to-save filename)
       ; (persist-query-result result-set q-id q-time)
  )
)

(defn run-shark-query
  [q-id query-str update-history-fn]
  (try
   (println (str "run-shark-query:" q-id))
  (sql/with-connection db
    (sql/with-query-results rs [query-str]
      
      (process-query q-id rs update-history-fn)
      ))
  (catch Exception exception
  (do
   (.printStackTrace exception)
    (update-result-map q-id "failed" nil (.getMessage exception) nil update-history-fn)
    ; we should seperate exception
    )))
)

(defn run-shark-query'
  [q-id query-str]
  (try
   (println (str "run-shark-query:" q-id))
  ( sql/with-connection db
    (sql/with-query-results rs [query-str]
  ; (println (str rs))
      
      (doall rs)
      ))
  (catch Exception exception
   (.printStackTrace exception)
    ; we should seperate exception
  ;  (update-result-map q-id "Failed" nil exception)
    ))
)

(defn submit-query
  [q-id query-str update-history-fn]
 ;; {:pre [not (str/blank? query-str)]}
  ;; TODO add query syntax check, only allow select clause
  (println (str q-id ":" query-str))
  (update-result-map q-id "running" {} nil nil update-history-fn )
  (future (run-shark-query q-id query-str update-history-fn))
)
      
(defn get-result
  [q-id]
  (println "in core/get-result")
  (println @result-map)
  (get @result-map q-id)
)
  
(defn clear-result-map
  [q-id]
  (swap! result-map dissoc [q-id])
)
