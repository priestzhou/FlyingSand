(ns kfktools.core
    (:require
        [clojure.java.io :as io]
        [clojure.string :as str]
        [utilities.shutil :as sh]
    )
    (:use
        [utilities.core :only (throw-if throw-if-not)]
    )
    (:import 
        [java.util Properties ArrayList]
        [java.nio.file Path Files]
        [java.nio.file.attribute FileAttribute]
        kafka.producer.KeyedMessage
        [kfktools ConsumerWrapper ProducerWrapper]
    )
)

(defn- saveProperties [f props]
    (let [f (if (instance? Path f) (.toFile f) f)]
        (with-open [wrt (io/writer f)]
            (doseq [[k v] props]
                (->>
                    (format "%s=%s" (name k) v)
                    (.write wrt)
                )
                (.newLine wrt)
            )
        )
    )
)

(def ^:private libs
    ".:common/extlib/kafka-0.8.0-beta1.jar"
)

(defn start [& props]
    (let [
            props (merge 
                {
                    :auto.create.topics.enable false
                    :controlled.shutdown.enable false
                }
                (apply array-map props)
            )
            pf (Files/createTempFile nil nil (into-array FileAttribute []))
        ]
        (throw-if-not (:zookeeper.connect props)
            IllegalArgumentException.
            "must have :zookeeper.connect"
        )
        (throw-if-not (:broker.id props)
            IllegalArgumentException.
            "must have :broker.id"
        )
        (throw-if-not (:log.dirs props)
            IllegalArgumentException.
            "must have :log.dirs"
        )
        (saveProperties pf props)
        (let [
                p (sh/popen
                    [
                        "java" "-cp" libs
                        "kafka.Kafka"
                        (str (.toAbsolutePath pf))
                    ]
                )
            ]
            (Thread/sleep 5000)
            (sh/newCloseableProcess p)
        )
    )
)

(defn createTopic [zk topic & props]
    (let [
            props (merge 
                {
                    :partition 1
                    :replica 1
                    :replica-assignment-list ""
                }
                (apply array-map props)
            )
        ]
        (sh/execute
            (flatten
                (concat
                    [
                        "java" "-cp" libs "kafka.admin.CreateTopicCommand"
                        "--zookeeper" zk "--topic" topic
                    ]
                    (for [[k v] props]
                        [(str "--" (name k)) (str v)]
                    )
                )
            )
        )
    )
)

(defn listTopic [zk topic]
    (sh/execute
        [
            "java" "-cp" libs "kafka.admin.ListTopicCommand"
            "--zookeeper" zk "--topic" topic
        ]
        :out :pipe
    )
)

(defn- map->Properties [props]
    {
        :pre [props]
    }
    (let [p (Properties.)]
        (doseq [[k v] props]
            (.put p (name k) (str v))
        )
        p
    )
)

(defn newProducer 
    ([props]
    (throw-if-not props
        IllegalArgumentException.
        "props must non-null"
    )
    (throw-if-not (:metadata.broker.list props)
        IllegalArgumentException.
        "must have :metadata.broker.list"
    )
    (->> props
        (map->Properties)
        (ProducerWrapper.)
    )
    )

    ([key1 val1 & args]
    (let [
            props (merge
                {:request.required.acks 1}
                {key1 val1}
                (apply hash-map args)
            )
        ]
        (newProducer props)
    ))
)

(defn wrap-messages [msgs]
    (let [lst (ArrayList.)]
        (doseq [{:keys [topic key message]} msgs]
            (.add lst
                (if key
                    (KeyedMessage. topic key message)
                    (KeyedMessage. topic message)
                )
            )
        )
        lst
    )
)

(defn produce [producer msgs]
    (let [lst (wrap-messages msgs)]
        (.send producer lst)
    )
)

(defn kafka-stream-to-seq [kafka-stream-iter]
    (lazy-seq
        (when (.hasNext kafka-stream-iter)
            (let [msg (.next kafka-stream-iter)]
                (cons
                    (if-let [k (.key msg)]
                        {:key k :message (.message msg)}
                        {:message (.message msg)}
                    )
                    (kafka-stream-to-seq kafka-stream-iter)
                )
            )
        )
    )
)

(defn listenTo [^ConsumerWrapper consumer ^String topic]
    (kafka-stream-to-seq (.listenTo consumer topic))
)

(defn newConsumer [& props]
    (let [props (apply hash-map props)]
        (throw-if-not (:zookeeper.connect props)
            IllegalArgumentException.
            "must have :zookeeper.connect"
        )
        (throw-if-not (:group.id props)
            IllegalArgumentException.
            "must have :group.id"
        )
        (ConsumerWrapper. (map->Properties props))
    )
)
