(ns query-server.mysql-connector
(:require 
  [clojure.java.jdbc :as jdbc]
  [korma.core :as orm ]
  [query-server.conf :as conf]
  [clojure.java.io :as io]
  [clojure.data.json :as json]
  [clj-time.core :as time]
  )
(:use 
  [korma.db]
  [korma.config]
  [logging.core :only [defloggers]]
  [clj-time.coerce]
  [clojure.set]
  [clj-time.format]
  )
;(:import [org.apache.hadoop.hive.ql.parse ParseDriver])
)

(defloggers debug info warn error)

(def query-status ["submitted" "running" "successed" "failed"])

(defn status-convert
     [stat]
     (.indexOf query-status stat)
)

(def db {:classname "com.mysql.jdbc.Driver"
         :subprotocol "mysql"
         :subname "192.168.1.101:3306/meta"
         :user "root"
         :password "fs123"}
)

(defdb korma-db (mysql {:host "192.168.1.101" :port 3306 :db "meta" :user "root" :password "fs123"}))
(set-delimiters "`")

(orm/defentity TblApplication
    (orm/pk :application_id)
    (orm/database korma-db)
)

(orm/defentity TblMetaStore
  (orm/pk :NameSpace)
  (orm/database korma-db)
)

(orm/defentity TblHistoryQuery 
  (orm/pk :QueryId)
  (orm/entity-fields :ExecutionStatus)
  (orm/database korma-db)
)

(orm/defentity TblSavedQuery 
  (orm/pk :QueryId)
  (orm/database korma-db)
)

(orm/defentity TblApplication
  (orm/pk :ApplicationId)
  (orm/database korma-db)
)

