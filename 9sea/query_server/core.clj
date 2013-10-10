(ns query-server.core
(:require 
  [clojure.java.jdbc :as sql]
  [clojure.string :as str]
  [query-server.conf :as conf]
  [clojure.data.json :as json]
  [clojure.java.io :as io]
  )
(:import [com.mchange.v2.c3p0 ComboPooledDataSource DataSources PooledDataSource]
         [java.io IOException]
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

(def pooled-db (pooled-spec db))

(def ^:private result-map (atom {}))

(def ^:private max-result-size 400)
(def ^:private result-file-dir "/home/admin/fancong/result")
(def ^:private ret-result-size 100)

(defn persist-query-result
  [result-set filename]
    (with-open [wrtr (io/writer filename)]
    (try
      (doseq [res result-set]
       ; (println res)
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
  [q-id stats ret-result error-message csv-filename]
  (println "update status:" stats)
  (case stats
    "Running" (swap! result-map update-in [q-id] assoc 
             :status stats 
             :submit-time (System/currentTimeMillis)
             :end-time 0 
             :log "0 stage:0"
             :error-message error-message
             :result ret-result)
    "Success" (swap! result-map  update-in [q-id] assoc 
             :status stats 
             :end-time (str (System/currentTimeMillis))
             :log "1 stage 1"
             :error-message error-message
             :uri csv-filename
             :result ret-result)
    "Failed" (swap! result-map  update-in [q-id] assoc 
             :status stats 
             :end-time (str (System/currentTimeMillis))
             :log "1 stage 1"
             :error-message error-message
             :result ret-result)
    )
  )
      
(defn process-query
  [q-id result-set]
  (let [{{q-time :start-time} q-id} @result-map
        ret-result (doall (take ret-result-size result-set))
	transformed-result (transform-result ret-result)
        result-to-save (atom (take max-result-size result-set))
        filename (format "%s/%d_%d_result.csv" result-file-dir q-id q-time)
       ]
        (println q-time transformed-result)
        (update-result-map q-id "Success" transformed-result nil filename)
        (persist-query-result @result-to-save filename)
       ; (persist-query-result result-set q-id q-time)
      ))

(defn run-shark-query
  [q-id query-str]
  (try
   (println (str "run-shark-query:" q-id))
  ( sql/with-connection db
    (sql/with-query-results rs [query-str]
  ; (println (str rs))
      
      (process-query q-id rs)
      ))
  (catch Exception exception
  (do
  ; (.printStackTrace exception)
    (update-result-map q-id "Failed" nil (.getMessage exception) nil)
   (println q-id)
    ; we should seperate exception
    ))))

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
    )))

(defn submit-query
  [q-id query-str]
 ;; {:pre [not (str/blank? query-str)]}
  ;; TODO add query syntax check, only allow select clause
  (println (str q-id ":" query-str))
  (update-result-map q-id "Running" {} nil nil)
  (future (run-shark-query q-id query-str)))
 ;  (run-shark-query q-id query-str))
      
(defn get-result
  [q-id]
 ; (println (get @result-map q-id))
  (get @result-map q-id)
)
  
(defn clear-result-map
  [q-id]
  (swap! result-map dissoc [q-id])
  )


          
