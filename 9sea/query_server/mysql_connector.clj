(ns query-server.mysql-connector
(:require 
  [clojure.java.jdbc :as jdbc]
  [korma.core :as orm ]
  [query-server.config :as config]
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
 (:import
  [java.net URI]
  [java.sql BatchUpdateException DriverManager
        PreparedStatement ResultSet SQLException Statement Types]
  [java.util Hashtable Map Properties]
  [javax.naming InitialContext Name]
  [javax.sql DataSource])
)

(defloggers debug info warn error)

(def query-status ["running" "succeeded" "failed"])

(defn status-convert
     [stat]
     (.indexOf query-status stat)
)

(def db (ref {:classname "com.mysql.jdbc.Driver" :subprotocol "mysql"}))
(def ^:private korma-db (ref {}))

(defn set-mysql-db
  [host port database user password]
  (prn host)
  (prn port)
  (prn database)
  (prn user)
  (prn password)
  (let [spec (mysql {:host host :port port :db database :user user :password password})]
    (dosync
      (ref-set korma-db (create-db spec))
    )
    (default-connection @korma-db)
  )
  (dosync
    (alter db conj {:subname (format "//%s:%s/%s" host port database)
                    :user user
                    :password password
                   }
    )
  )
)

(defn get-korma-db
  []
  (-> @korma-db)
)


(defn get-mysql-db
  []
  (-> @db)
)

(set-delimiters "`")

(orm/defentity TblApplication
    (orm/pk :application_id)
)

(orm/defentity TblUsers
  (orm/pk :UserId)
)

(orm/defentity TblMetaStore
  (orm/pk :NameSpace)
)

(orm/defentity TblHistoryQuery 
  (orm/pk :QueryId)
  (orm/entity-fields :ExecutionStatus)
)

(orm/defentity TblSavedQuery 
  (orm/pk :QueryId)
)

(orm/defentity TblApplication
  (orm/pk :ApplicationId)
)

(orm/defentity TblAgent
  (orm/pk :id)
)

