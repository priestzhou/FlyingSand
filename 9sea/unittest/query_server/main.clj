(ns unittest.query-server.main
    (:use 
        [testing.core]   
        [clj-time.format]
        [clj-time.coerce]
	[korma.db]
    )
    (:require 
        [query-server.query-backend :as qs]
        [query-server.mysql-connector :as mysql]
        [query-server.core :as shark]
        [query-server.config :as config]
        [query-server.web-server :as web]
        [clj-time.core]
        [korma.core :as orm]
    )
)

(def test-context {
                   :ns 
                   [{:children 
                     [
                      {
                         :children 
                         [
                          {
                           :type "table", :name "acter", :hive-name "tn_84e56395378035cd6850fb913f9658a130d976f4", 
                           :children [{:name "id",:type "int"}]
                           } 
                          {
                           :type "table", :name "user", :hive-name "tn_79719c20120d0a7ef0e4d87c873a985eba87fc07", 
                           :children []
                          }
                         ], 
                         :type "namespace", :name "v1"
                      }
                      {
                         :children 
                         [
                          {
                           :type "table", :name "acter", :hive-name "tn_24e56395378035cd6850fb913f9658a130d976f4", 
                           :children [{:name "id",:type "int"}]
                           } 
                          {
                           :type "table", :name "user", :hive-name "tn_29719c20120d0a7ef0e4d87c873a985eba87fc07", :children []
                          }
                         ], 
                         :type "namespace", :name "v2"
                       }
                       
                       
                      ], 
                     :type "namespace", :name "御剑三国"}
                    ], 
                  :default-ns ["御剑三国" "v1"]
                  }
)
(def test-context' {
                   :ns 
                   [{:children 
                     [{:children 
                       [
                        {
                         :type "table", :name "acter", :hive-name "tn_84e56395378035cd6850fb913f9658a130d976f4", :children []
                         } 
                        {
                         :type "table", :name "user", :hive-name "tn_79719c20120d0a7ef0e4d87c873a985eba87fc07", :children []
                        }
                        {
                         :type "view", :name "v1", :hive-name "vn_79719c20120d0a7ef0e4d87c873a985eba87fc07", :children []
                        }
                        {
                         :type "ctas", :name "t1", :hive-name "tn_89719c20120d0a7ef0e4d87c873a985eba87fc07", :children []
                        }
                       ], 
                       :type "namespace", :name "1.0"}
                      ], 
                     :type "namespace", :name "御剑三国"}
                    ], 
                  :default-ns ["御剑三国" "1.0"]
                  }
)

(suite "create view-ctas"
      (:fact query:view:test-create-view-in-two-version
              (
               shark/translate-query test-context "create view view1 as select a.* from v1.acter a join v2.acter b on a.id=b.id"
              )
          :is
{:clause-type :create-clause, :type :view, :appname "御剑三国", :appversion "v1", :tablename "view1", :hql "CREATE VIEW vn_ed14e5f64a25f8fbdb0fccb42f8668e8c3323189 AS SELECT a.* FROM tn_84e56395378035cd6850fb913f9658a130d976f4 a JOIN tn_24e56395378035cd6850fb913f9658a130d976f4 b ON (a.id = b.id)", :hive-name "vn_ed14e5f64a25f8fbdb0fccb42f8668e8c3323189"}              
          
       )
       (:fact query:view:test-create-view
              (
               shark/translate-query test-context "create view v1 as select * from acter"
              )
          :is
          {:clause-type :create-clause, :type :view, :appname "御剑三国", :appversion "v1", :tablename "v1", :hql "CREATE VIEW vn_65581ccac20e6dc57a2c59331dd536de74b9f540 AS SELECT * FROM tn_84e56395378035cd6850fb913f9658a130d976f4", :hive-name "vn_65581ccac20e6dc57a2c59331dd536de74b9f540"}
       )
       (:fact query:view:test-drop-view
              (
               shark/translate-query test-context' "drop view v1"
              )
          :is
          {:clause-type :drop-clause, :type :ctas, :appname "御剑三国", :appversion "1.0", :tablename "v1", :hql "DROP VIEW IF EXISTS vn_79719c20120d0a7ef0e4d87c873a985eba87fc07"}
       )
       (:fact query:view:test-select-table
              (
               shark/translate-query test-context "select * from acter"
              )
          :is
          {:clause-type :select-clause, :hql "SELECT * FROM tn_84e56395378035cd6850fb913f9658a130d976f4"}
      )
     (:fact query:view:test-create-ctas
            (
             shark/translate-query test-context "create table t1 as select * from acter"
            )
        :is
        {:clause-type :create-clause, :type :ctas, :appname "御剑三国", :appversion "v1", :tablename "t1", :hql "CREATE TABLE tn_078731f2a2b2eff6bec4fc7e6f0bcfa35a733a04 AS SELECT * FROM tn_84e56395378035cd6850fb913f9658a130d976f4", :hive-name "tn_078731f2a2b2eff6bec4fc7e6f0bcfa35a733a04"}
     )
    (:fact query:view:test-drop-ctas
            (
             shark/translate-query test-context' "drop table t1"
            )
            :is
           {:clause-type :drop-clause, :type :ctas, :appname "御剑三国", :appversion "1.0", :tablename "t1", :hql "DROP TABLE IF EXISTS tn_89719c20120d0a7ef0e4d87c873a985eba87fc07"}
    )

)

(suite "get metaschema"
       (:fact test-get-metastore-tree
               (let [r (qs/get-metastore-tree 4)]
                 r
               )
              :is
              []
        )
       (:fact test-update-history-queries
              (
               shark/update-history-query 110 nil nil nil nil 10
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
       (:fact test-execute-query
              (
               shark/execute-query "select * from tn_df3807a49308ac0d35bc7f87245853226a32411e a
                                   join tn_df3807a49308ac0d35bc7f87245853226a32411e b on a.id=b.id limit 10"
              )
              :is
              nil
       )

       (:fact test-config
              (
               config/get-key :shark-server-host
              )
              :is 
              "192.168.1.100"
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
	(mysql/status-convert "submitted")
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
