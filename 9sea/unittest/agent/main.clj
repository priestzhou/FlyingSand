(ns unittest.agent.main
    (:use testing.core   
    )
    (:require 
        [agent.dbadapt :as dba]
    )
)

(defn- get-dbname [in]
    "test db"
)

(defn- get-table-schema [in1 in2]
    [{:id "id2" :name "col2"} {:id "id1" :name "col1"}]
)

(def  mock-map    
    {
        :get-dbname get-dbname
        :get-table-schema get-table-schema
    }
)

(defn- get-schemas [in]
    (binding [dba/*db-func-map* mock-map] 
        (dba/get-schemas in)
    )
)

(defn- get-db-table-list [in]
    (binding [dba/*db-func-map* mock-map] 
        (dba/get-db-table-list in)
    )
)

(suite "test"
    (:fact test1
        (->>
            {}
            get-schemas
        )
        :is
        nil
    )
)