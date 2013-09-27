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
        (let [out (dba/get-schemas in)]
            (println out)
            out
        )
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
            {:database 
                [
                    {:dbconnstr "test1" 
                        :dbuser "root" 
                        :dbpassword "ff" 
                        :tables [
                            {:tablename "test-table1"} 
                            {:tablename "test-table2"}
                        ]
                    }
                ] 
            }
            get-schemas
        )
        :is
        [{:dbname "test db", 
            :tables [
            {:tablename "test-table1", :cols [{:name "col2", :id "id2"} {:name "col1", :id "id1"}]} 
            {:tablename "test-table2", :cols [{:name "col2", :id "id2"} {:name "col1", :id "id1"}]}
            ]
        }]
    )
)