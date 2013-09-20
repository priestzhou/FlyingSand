(ns zktools.core
    (:require 
        [clojure.string :as str]
        [utilities.shutil :as sh]
    )
    (:import
        java.lang.Process
    )
)

(defn- waitUntilZkReady [port]
    (Thread/sleep 1000)
    (loop []
        (let [
                {out :out} (sh/execute 
                    ["nc" "localhost" (format "%d" port)] 
                    :in "ruok" :out :pipe
                )
            ]
            (when-not (= out "imok")
                (Thread/sleep 500)
                (recur)
            )
        )
    )
)

(defn start [port dataDir]
    (let [p (sh/popen
                [
                    "java" "-cp" ".:common/extlib/zookeeper-3.4.5.jar"
                    "org.apache.zookeeper.server.quorum.QuorumPeerMain"
                    (format "%d" port)
                    (str (.toAbsolutePath dataDir))
                ]
            )
        ]
        (waitUntilZkReady port)
        (sh/newCloseableProcess p)
    )
)
