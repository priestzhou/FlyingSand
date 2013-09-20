(ns utilities.net
    (:import
        [java.net NetworkInterface InetAddress]
    )
)

(defn- enum->lazyseq! [enum]
    (when (.hasMoreElements enum)
        (let [x (.nextElement enum)]
            (lazy-seq (cons x (enum->lazyseq! enum)))
        )
    )
)

(defn- localhost' []
    (for [intf (enum->lazyseq! (NetworkInterface/getNetworkInterfaces))
        :when (.isUp intf)
        :when (not (.isLoopback intf))
        inet (enum->lazyseq! (.getInetAddresses intf))
        :when (= 4 (alength (.getAddress inet))) ; ipv4
        ]
        inet
    )
)

(def localhost (memoize localhost'))
