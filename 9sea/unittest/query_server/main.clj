(ns unittest.query-server.main
    (:use 
        [testing.core]   
        [clj-time.format]
        [clj-time.coerce]
	[korma.db]
    )
    (:require 
        [query-server.query-backend :as qs]
        [clj-time.core]
        [korma.core :as orm]
    )
)

(suite "get metaschema"
       (:fact test-get-metastore-tree
               (let [r (qs/get-metastore-tree 2)]
                 r
               )
              :is
              []
        )
       (:fact test-update-history-queries
              (
               qs/update-history-query 110 nil nil nil nil 10
               )
              :is
              []
       )
       (:fact test-update-queries-status
              (
               qs/update-query-status 110 "failed"
               )
              :is
              nil
       )


)

(defdb korma-db (mysql {:host "192.168.1.101" :port 3306 :db "meta" :user "root" :password "fs123"}))

(orm/defentity application
    (orm/pk :application_id)
    (orm/database korma-db)
)

(orm/defentity metastore
  (orm/pk :application_version_id)
  (orm/database korma-db)
  )

(orm/defentity TblHistoryQuery
  (orm/pk :query_id)
  (orm/database korma-db)
  )

(orm/defentity TblSavedQuery
  (orm/pk :QueryId)
  (orm/database korma-db)
  )

(def start-time 
(unparse (formatters :date-hour-minute-second) (from-long (System/currentTimeMillis))))




(suite "query server backend"
    (:testbench
      (fn [test]
          (try (test) 
            (finally (do
		(prn "clean up!")
                (orm/delete TblSavedQuery (orm/where {:QueryName [= "myquery"]}))
                (orm/delete TblHistoryQuery (orm/where {:QueryId "1"}))
            )))))

    (:fact test-save-query
     ( 
      (qs/save-query "myquery" "app1" "ver1" "default" "select * from test" start-time (rand-int 1000))
      (prn (orm/sql-only (orm/select TblSavedQuery (orm/where {:QueryName "myquery"}))))
      (let [res (orm/select TblSavedQuery (orm/where {:QueryName "myquery"}))]
	(prn (str res))
       (-> res (first) (:QueryName) ))
     )
        :is
(do
      "myquery" 
)
    )

    (:fact test-log-query
      (do
         (let [q-id (qs/log-query "select * from test" (rand-int 1000) start-time)]
         (-> q-id (nil?)))
      )
        :is
        false	
    )

    (:fact test-status-fetch
	(qs/status-convert "submitted")
	:is
	0
    )

   (:fact test-check-query-name-nil
       (qs/check-query-name "myquery")
       :is
	nil
   )

  (:fact test-check-query-duplicate
     (do
      (qs/save-query "myquery" "app1" "ver1" "default" "select * from test" start-time (rand-int 1000))
      (let [q-id (qs/check-query-name "myquery")]
         (prn "query-id:" q-id)
        (-> q-id (number?))
      )
     )
      :is
      true
  )

)
