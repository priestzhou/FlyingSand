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
        [clj-time.core :as time]
        [clj-time.coerce :as coerce]
        [hdfs.core :as hc]
        [query-server.mysql-connector :as mysql]
        [query-server.config :as config]
        [query-server.web-server :as ws]
        [mailtool.core :as mail]
        [query-server.timing-query-mail :as tqmail]
    )
    (:gen-class)
)

(defloggers debug info warn error)

(def ^:private taskQueue 
    (ref [])
)

(defn- get-now []
    (coerce/to-long (time/now))
)

(defn- get-query-from-db []
    (let [sql (str "select * from TblTimingQuery"
            )
        ]
        (mysql/runsql sql)
    )
)

(defn- get-query-by-time [query]
    (let [startTime (:starttime query)
            endTime (:endtime query)
            timeSpan (:timespan query)
            now (get-now)
            ;seq-start-time (- now (mod (- now startTime) timeSpan))
            seq-end-time (min (+ now timeSpan) endTime) 
            timeList (->> 
                    startTime
                    (iterate #(+ timeSpan % ))
                    (take-while #(> seq-end-time %))
                    (filter 
                        #(< 
                            (+ now (config/get-key :timing_query_window_min))
                            %
                            (+ now (config/get-key :timing_query_window_max))
                        )
                    )
                )
        ]
        (debug "get-query-by-time" :now now :startTime startTime 
            :timeList timeList
        )
        (cond 
            (< now startTime) nil
            (< endTime now) nil
            (not (empty? timeList) ) (first timeList)
        )
    )
)

(defn add-to-task-queue [taskitem]
    (debug "add-to-task-queue" :taskitem taskitem)
    (let [taskmap-raw (first taskitem)
            taskmap (assoc taskmap-raw :task-run-time (last taskitem))
        ]
        (dosync
            (when 
                (empty? 
                    (filter 
                        #(and 
                            (= (:timing_query_id taskmap) (:timing_query_id %))
                            (= (:task-run-time taskmap) (:task-run-time %))
                        )
                        @taskQueue
                    )
                )
                (alter taskQueue conj taskmap)
            )
        )
    )
)

(defn pop-first-from-task-queue []
    (dosync
        (when-let [item (first @taskQueue)]
            (debug "pop item!")
            (alter taskQueue rest)
            item
        )
    )
)

(defn add-task-run-record [tid qid status runtime]
    (let [sql (str "insert into TblTimingTaskLog "
                "(TimingQueryID,queryID,status,runtime) "
                "values (" tid "," qid ",\"" status "\"," runtime")"
            )
        ]
        (mysql/runupdate sql)
    )
)

(defn- wait-for-task [tid qid runtime retryTimes mailflag]
    (let [result (qb/get-query-result qid)]
        (cond 
            (or (nil? result) (= "failed" (:status result))) 
            (do 
                (add-task-run-record tid qid "failed" runtime)
                (error "the query failed or can't find" :qid qid)
                (when (= "true" mailflag)
                    (tqmail/send-mail tid qid)
                )
                nil
            )
            (= "running" (:status result))
            (do 
                (when (= retryTimes (config/get-key :timing-query-retryTimes))
                    (error "the query time out" :qid qid)
                )
                (Thread/sleep (config/get-key :timing-query-runner-interval))
                (recur tid qid runtime (+ 1 retryTimes) mailflag)
            )
            (= "succeeded" (:status result))
            (do 
                (info "the query succeeded " :qid qid)
                (add-task-run-record tid qid "succeeded" runtime)
                result
            )
        )
    )
)

(defn- check-task-result [tid qid result noflag anyflag]
    (let [rcount (:count result)]
        (when 
            (or 
                (and (= "true" anyflag) (> rcount 0)) 
                (and (= "true" noflag) (= rcount 0))
            )
            (tqmail/send-mail tid qid)
        )        
    )
)

(defn- submit-task [task]
    (debug "submit-task"  (str task))
    (let [{:keys 
                [timingqueryid appname appversion 
                    accountid sqlstring userid 
                    failmailflag noresultmailflag anyresultmailflag
                ]
            }
                task
            context (ws/gen-context accountid appname appversion nil)
            qid (qb/submit-query context accountid userid sqlstring)
            runtime (:task-run-time task)
            result (wait-for-task 
                    timingqueryid qid runtime 0 failmailflag 
                )
        ]
        (check-task-result 
            timingqueryid qid result noresultmailflag anyresultmailflag
        )
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
    (recur)
)

(defn timing-query-check []
    (try 
        (debug (str "timing-query-check run  " (get-now)))
        (let [query-list (get-query-from-db)
                task-list
                    (->>
                        query-list
                        (map #(vector % (get-query-by-time %)))
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
            (dorun (map add-to-task-queue task-list))
        )
        (Thread/sleep (config/get-key :timing-query-check-interval))
        (catch Exception e
            (error " the timing query check failed " :except (except->str e))
        )
    )
    (recur)
)

(defn start-timing-query []
    (when (config/get-key :timing-query-start-flag)
        (info "timing-query starting ")
        (future (timing-query-check))
        (future (timing-query-runner))
        (future (timing-query-runner))
        (info "timing-query started ")
    )
)
