(ns smoketest.query-server.main
 (:use
        [testing.core]
        [clj-time.format]
        [clj-time.coerce]
        [korma.db]
    )
    (:require
        [query-server.query-backend :as qs]
        [query-server.mysql-connector :as mysql]
        [query-server.hive-adapt :as hive]
        [query-server.core :as query]
        [query-server.config :as config]
        [query-server.web-server :as web]
        [clj-time.core]
        [korma.core :as orm]
        [clojure.java.io :as io]
    ) 
)

(def config-content
  "
{
 :shark-host \"192.168.9.100\"
 :shark-dbname \"200\"
 :shark-port \"10000\"
 :username \"\"
 :password \"\"
 :mysql-host \"192.168.9.101\"
 :mysql-db \"meta_test\"
 :mysql-port \"3306\"
 :mysql-user \"root\"
 :mysql-password \"fs123\"
 :max-result-size 500
 :ret-result-size 2
 :result-file-dir \"./\"
 :new-agent-check-interval 600000
 :inc-data-check-interval 500000
 :all-data-check-interval 86400000
 :inc-data-group-time 3600000
 :mismatch-agent-check-interval 60000
 }
")

(suite "query result"
       (:testbench
         (fn [test]
           (spit "./webserver_props.conf" config-content)
           (spit "./978_1384228898242_result.csv" "id\n820\n830")
           
            (config/set-config-path "./webserver_props.conf")
            (mysql/set-mysql-db
              (config/get-key :mysql-host)
              (config/get-key :mysql-port)
              (config/get-key :mysql-db)
              (config/get-key :mysql-user)
              (config/get-key :mysql-password)
            )
          (hive/set-hive-db
            (config/get-key :shark-host)
            (config/get-key :shark-port)
          )
           (try
              (test "test")
           (finally
             (io/delete-file "./webserver_props.conf")
             (io/delete-file "./978_1384228898242_result.csv")
           ))
         )
       )
       (:fact query:result:test-recover-result
            (fn [x]
              (
               query/recover-result 978
              )
              (query/get-result 978)
            )
              :eq
              (fn [_]
                {:duration "1", 
                 :url "/result/978_1384228898242_result.csv", 
                 :status "succeeded", 
                 :result {:titles ["id"], :values [["820"] ["830"]]},
                 :end-time 1382502088000, 
                 :log "query is succeeded!"}
              )
       )
)


