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
    )
)

(def mailcontext "尊敬的用户：<br> 您的周期任务，任务名称：_taskname_，
    于_starttime_开始，于_endtime_结束。<br>
    任务执行结果:_flag_，请登录系统查看详情。<br>")

(def mailtitle "_taskname_已经执行完毕,_titleflag_")

(defn- get-query-from-db-by-id [id]
    (let [sql (str "select * from TblTimingQuery where TimingQueryID=" id
            )
        ]
        (first (mysql/runsql sql))
    )
)

(defn- send-mail' [maillist taskname starttime endtime flag titleflag]
    (let [ title 
            (-> 
                mailtitle
                (replace #"_titleflag_" titleflag)
                (replace #"_taskname_" taskname)
            )
            context 
            (->
                mailcontext
                (replace #"_taskname_" taskname)
                (replace #"_starttime_" starttime)   
                (replace #"_endtime_" endtime)   
                (replace #"_flag_" flag)   
            )
        ]
        (mail/send-html-mail maillist title context)
    )
)

(defn send-mail [tid qid] 
    (let [qinfo (get-query-from-db-by-id tid)
            {:keys [maillist taskname]} qinfo
            result (qb/get-query-result qid)
            status (:status result)
            rcount (:count result)
            starttime (:submit-time result)
            endtime (:end-time result)
        ]
        (cond 
            (= status "failed")
            (send-mail' maillist taskname starttime endtime "失败" "查询失败")
            (= 0 rcount)
            (send-mail' maillist taskname starttime endtime "成功" "查询结果集为空")
            (< 0 rcount)
            (send-mail' maillist taskname starttime endtime "成功" "查询结果有内容")
        )
    )
)