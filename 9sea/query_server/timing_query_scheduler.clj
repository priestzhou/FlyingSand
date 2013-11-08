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
        [query-server.web-server :as ws]
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
    (ref (sequence []))
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

(defn- get-now []
    
)

(defn- get-query-from-db []
    [testrecord]
)

(defn- get-query-by-time [query]
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
            (< now starTime) nil
            (< endTime now) nil
            (not (empty? timeList) ) (first timeList)
        )
    )
)

(defn add-to-task-queue [taskitem]
    (let [taskmap-raw (first taskitem)
            taskmap (assoc taskmap-raw :task-run-time (last taskitem))
        ]
        (dosync
            (when 
                (empty? 
                    (filter 
                        #(and 
                            (= (:timing_query_id taskitem) (:timing_query_id %))
                            (= (:task-run-time taskitem) (:task-run-time %))
                        )
                        @taskQueue
                    )
                )
                (alter taskQueue concat [taskitem])
            )
        )
    )
)

(defn pop-first-from-task-queue []
    (dosync
        (when-let [item (first @taskQueue)]
            (alter taskQueue rest)
            item
        )
    )
)

(defn- wait-for-task [qid retryTimes]
    (let [result (qb/get-query-result qid)]
        (cond 
            (or (nil? result) (= "failed" (:status result))) 
            (do 
                (add-task-run-record qid "failed" )
                (error "the query failed or can't find" :qid qid)
            )
            (= "running" (:status result))
            (do 
                (when (= retryTimes (config/get-key :timing-query-retryTimes))
                    (error "the query time out" :qid qid)
                    
                )
                (Thread/sleep (config/get-key :timing-query-runner-interval))
                (recur qid (+ 1 retryTimes))
            )
        )
    )

)

(defn- submit-task [task]
    (let [{:keys [timing_query_id appname appversion accountid sqlstr userid]} task
            context (ws/gen-context accountid appname appversion nil)
            qid (submit-query context userid sqlstr)
        ]
        (wait-for-task qid 0)
        (check-task-result (qb/get-query-result qid))
    )
)

(defn timing-query-runner []
    (try
        (if-let [task (pop-first-from-task-queue)] 
            (submit-task task)
            (Thread/sleep (config/get-key :timing-query-runner-interval))
        )
        
        (catch Exception e
            (error " the timing query run failed " :except (except->str e))
        )
    )
)

(defn timing-query-check []
    (try 
        (let [query-list (get-query-from-db)
                task-list
                    (->>
                        query-list
                        (map #(vector % (get-query-by-time% )))
                        (filter 
                            #(->>
                                %
                                last
                                nil?
                                not
                            )
                        )
                    )

            ]
            (map add-to-task-queue task-list)
        )
        (Thread/sleep (config/get-key :timing-query-check-interval))
        (catch Exception e
            (error " the timing query check failed " :except (except->str e))
        )
    )
    (recur)
)



