(ns smoketest.zktools.core
    (:require
        [zktools.core :as zkt]
        [utilities.shutil :as sh]
    )
    (:use
        testing.core
    )
    (:import 
        org.apache.zookeeper.ZooKeeper
        org.apache.zookeeper.Watcher
        org.apache.zookeeper.CreateMode
        org.apache.zookeeper.data.Stat
    )
)

(suite "zookeeper: start and stop"
    (:testbench
        (fn [test]
            (let [rt (sh/getPath "zk_start_stop")]
                (try
                    (with-open [_ (zkt/start 10240 rt)]
                        (test)
                    )
                (finally
                    (sh/rmtree rt)
                ))
            )
        )
    )
    (:fact start-stop
        (with-open [
                zk (ZooKeeper. "localhost:10240" 5000 
                    (proxy [Watcher] []
                        (process [])
                    )
                )
            ]
            (.create zk "/foo" (.getBytes "bar") 
                org.apache.zookeeper.ZooDefs$Ids/OPEN_ACL_UNSAFE
                org.apache.zookeeper.CreateMode/PERSISTENT
            )
            (-> 
                (.getData zk "/foo" false (Stat.))
                (String.)
            )
        )
        :is "bar" 
    )
)
