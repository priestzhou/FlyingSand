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
    ) 
)

(suite "query result"
       (:testbench
         (fn [test]
              (config/set-config-path
           "/home/admin/fancong/flyingsand/FlyingSand/webserver_props.conf")
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
            (test "test")
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
                {:978 {}}
              )
       )
)


