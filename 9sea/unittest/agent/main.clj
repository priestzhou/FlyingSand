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

(defn- get-table-all-data [in qstr]
    (binding [dba/*db-func-map* mock-map]
        (dba/get-table-all-data in qstr)
    )
)

(def dbsetting                     
            {
            :app "testapp"
            :appversion "ves-1"
            :database 
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
)

(suite "test"
    (:fact get-schemas-from-db-0
        (->>
            {:database 
                [] 
            }
            get-schemas
        )
        :is
        []
    )
    (:fact get-schemas-from-db-1
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
    (:fact get-schemas-from-db-2
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
                    {:dbconnstr "test2" 
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
            count
        )
        :is
        2
    )
    (:fact test-table-all-data-apperro
        (->>
            (get-table-all-data
                dbsetting
                "abc.bsc.def.test1"
            )
            :errCode
        )
        :is
        "app mismach"
    )
    (:fact test-table-all-data-versionerro
        (->>
            (get-table-all-data
                dbsetting
                "testapp.bsc.def.test1"
            )
            :errCode
        )
        :is
        "version mismach"
    )
    (:fact test-table-all-data-dberro
        (->>
            (get-table-all-data
                dbsetting
                "testapp.ves-1.def.test1"
            )
            :errCode
        )
        :is
        "db not find"
    )
    (:fact test-table-all-data-tberro
        (->>
            (get-table-all-data
                dbsetting
                "testapp.ves-1.test db.test1"
            )
            :errCode
        )
        :is
        "table not find"
    )
    (:fact test-table-all-data-allmetapass
        (->>
            (get-table-all-data
                dbsetting
                "testapp.ves-1.test db.test-table1"
            )
            :errCode
        )
        :is
        nil
    )
)

