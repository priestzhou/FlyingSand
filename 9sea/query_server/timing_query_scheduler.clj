(ns query-server.timing-query-scheduler
    (:use
        [utilities.core :only (except->str)]
        [logging.core :only [defloggers]]
    )
    (:require 
        [org.httpkit.client :as client]
        [query-server.query-backend :as qb]
        [clojure.java.jdbc :as jdbc]
        [clojure.data.json :as js]
        [utilities.core :as uc]
        [hdfs.core :as hc]
        [query-server.mysql-connector :as mysql]
        [query-server.config :as config]
    )
    (:gen-class)
)

(defloggers debug info warn error)

(defn- get-group-time-second [timeValue]
    (let [modTime (mod timeValue 1000)
            groupTime (- timeValue modTime)
        ]
        (.format 
            (java.text.SimpleDateFormat. "yyyy-MM-dd-HH_mm_ss") 
            groupTime
        )
    )
)

(def ^:private taskQueue 
    (atom [])
)

(def testrecord 
    {   
        :timing_query_id 1
        :accountid 1
        :userid 1
        :appname "test"
        :appversion "v1"
        :sqlstr "select * from user"
        :startTime "1370000"
        :endTime "1390000"
        
    }
)


(defn- filter-query-by-time [query]
    (let [starTime (:starttime query)
            endTime (:endtime query)
            timeSpan (:timeSpan query)
            now (System/currentTimeMillis)
            timeList (->> 
                    startTime
                    (iterate #(+ timeSpan % ))
                    (take-while #(> endTime %))
                    (map #(- now %))
                    (filter 
                        #(< 
                            (config/get-key :timing_query_window_min) 
                            %
                            (config/get-key :timing_query_window_max)
                        )
                    )
                )
        ]
        (cond 
            (< now starTime) false
            (< endTime now) false

        )
    )
)

(defn timing-query-check []
    (try 
        ()
        (Thread/sleep (config))
        (catch Exception e
            (error " the timing query check failed " :except (except->str e))
        )
    )
    (recur)
)



