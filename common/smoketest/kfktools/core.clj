(ns smoketest.kfktools.core
    (:use testing.core)
    (:require
        [utilities.shutil :as sh]
        [zktools.core :as zk]
        [kfktools.core :as kfk]
    )
    (:import
        [java.util Properties ArrayList HashMap]
        [java.nio.file Path Files]
        [java.nio.file.attribute FileAttribute]
        [kfktools ConsumerWrapper]
    )
)

(suite "kafka: start and stop"
    (:testbench
        (fn [test]
            (let [rt (sh/getPath "kafka_start_stop")
                    zkdir (sh/getPath rt "zk")
                    kfkdir (sh/getPath rt "kfk")
                    kfkprp (sh/getPath rt "kafka.properties")
                ]
                (try
                    (sh/mkdir zkdir)
                    (sh/mkdir kfkdir)
                    (with-open [z (zk/start 10240 zkdir)
                            k (kfk/start 
                                :zookeeper.connect "localhost:10240"
                                :broker.id 0
                                :log.dirs (.toAbsolutePath kfkdir)
                            )
                        ]
                        (test)
                    )
                (finally
                    (sh/rmtree rt)
                    (sh/rmtree kfkprp)
                ))
            )
        )
    )
    (:fact start-stop
        (fn []
            (kfk/createTopic "localhost:10240" "test")
            (->>
                (kfk/listTopic "localhost:10240" "test")
                (:out)
                (re-find #"topic:\s+(\S+)\s+")
                (#(get % 1))
            )
        )
        :eq 
        (fn [] 
            "test"
        )
    )
)

(suite "kafka: produce and consume"
    (:testbench
        (fn [test]
            (let [rt (sh/tempdir)
                    zkdir (sh/getPath rt "zk")
                    kfkdir (sh/getPath rt "kfk")
                    kfkprp (sh/getPath rt "kafka.properties")
                ]
                (printf "workdir: %s" rt)
                (println)
                (try
                    (sh/mkdir zkdir)
                    (sh/mkdir kfkdir)
                    (with-open [z (zk/start 10240 zkdir)
                            k (kfk/start 
                                :zookeeper.connect "localhost:10240"
                                :broker.id 0
                                :log.dirs (.toAbsolutePath kfkdir)
                            )
                        ]
                        (kfk/createTopic "localhost:10240" "test")
                        (test)
                    )
                (finally
                    (sh/rmtree rt)
                    (sh/rmtree kfkprp)
                ))
            )
        )
    )
    (:fact produce-consume
        (fn []
            (with-open [p (kfk/newProducer :metadata.broker.list "localhost:6667")]
                (kfk/produce p [
                    {:topic "test" :message (.getBytes "haha")} 
                    {:topic "test" :key (.getBytes "xixi") :message (.getBytes "hehe")}
                ])
            )
            (with-open [c (kfk/newConsumer 
                    :zookeeper.connect "localhost:10240" 
                    :group.id "alone" 
                    :auto.offset.reset "smallest")
                ]
                (let [cseq (take 2 (kfk/listenTo c "test"))]
                    (for [{key :key message :message} (doall cseq)]
                        {
                            :key (if key (String. key) nil) 
                            :message (if message (String. message) nil)
                        }
                    )
                )
            )
        )
        :eq 
        (fn [] 
            [
                {:key nil :message "haha"}
                {:key "xixi" :message "hehe"}
            ]
        )
    )
)
