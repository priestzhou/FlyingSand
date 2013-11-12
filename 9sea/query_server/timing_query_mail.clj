(ns query-server.timing-query-mail
    (:use
        [utilities.core :only (except->str)]
        [logging.core :only [defloggers]]
    )
    (:require 
        [org.httpkit.client :as client]
        [query-server.query-backend :as qb]
        [clojure.data.json :as js]
        [utilities.core :as uc]
        [clj-time.core :as time]
        [clj-time.coerce :as coerce]
        [query-server.mysql-connector :as mysql]
        [query-server.config :as config]
        [mailtool.core :as mail]
        [clojure.string :as cstr]
    )
)

(def mailcontext "<p>尊敬的用户：</p> <p>您的周期任务，任务名称：_taskname_，
    于_starttime_开始，于_endtime_结束。</p> <p>
    任务执行结果:_flag_，请登录系统查看详情。</p>")

(def mailtitle "_taskname_已经执行完毕,_titleflag_")

(defn- get-query-from-db-by-id [id]
    (let [sql (str "select * from TblTimingQuery where TimingQueryID=" id
            )
        ]
        (first (mysql/runsql sql))
    )
)

(defn- parsetime [time-long]
    (->
        time-long
        (coerce/from-long)
        (time/to-time-zone  (time/time-zone-for-offset +8))
        (str )
    )
)

(defn- send-mail' [maillist taskname starttime endtime flag titleflag tablecontext]
    (println "endtime" endtime)
    (println "starttime" starttime)
    (println "taskname" taskname)
    (let [ title 
            (-> 
                mailtitle
                (cstr/replace #"_titleflag_" titleflag)
                (cstr/replace #"_taskname_" taskname)
            )
            context 
            (->
                mailcontext
                (cstr/replace #"_taskname_" taskname)
                (cstr/replace #"_starttime_" (parsetime starttime))
                (cstr/replace #"_endtime_" (parsetime endtime))
                (cstr/replace #"_flag_" flag)   
            )
        ]
        (mail/send-html-mail maillist title (str context tablecontext))
    )
)

(defn- gen-one-tr [values]
    (reduce #(str %1 "<td>" %2 "</td>") "" values)
)

(defn- gen-query-table [result]
    (let [thead (:titles result)
            values (:values result)
        ]
        (str 
            "<table border=\"1\">"
                "<thead> <tr>"
                (reduce  #(str %1 "<th>" %2 "</th>") "" thead)
                "</tr> </thead>"
                "<tbody>"
                (reduce #(str %1 "<tr>" (gen-one-tr %2) "</tr>") "" values)
                "</tbody>"
            "</table>"
        )
    )
)

(defn send-mail [tid qid] 
    (let [qinfo (get-query-from-db-by-id tid)
            _ (println "qinfo" qinfo)
            {:keys [maillist taskname]} qinfo
            result (qb/get-query-result qid)
            status (:status result)
            rcount (:count result)
            starttime (:submit-time result)
            endtime (:end-time result)
            _ (println "result" (:result result) )
        ]
        (cond 
            (= status "failed")
            (send-mail' maillist taskname starttime endtime "失败" "查询失败" "")
            (= 0 rcount)
            (send-mail' maillist taskname starttime endtime "成功" "查询结果集为空" "")
            (< 0 rcount)
            (send-mail' 
                maillist taskname starttime endtime "成功" "查询结果有内容"
                (gen-query-table (:result result))
            )
        )
    )
)