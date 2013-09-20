(ns smoketest.utilities.net
    (:use
        testing.core
        utilities.net
    )
    (:require
        [clojure.string :as str]
        [utilities.shutil :as sh]
    )
)

(suite "get host name & ip of local address"
    (:fact get-localhost
        (for [ia (localhost)]
            [(.getHostName ia) (.getHostAddress ia)]
        )
        :is
        (let [inet (->>
                (sh/execute ["/sbin/ifconfig" "eth0"] :out :pipe)
                (:out)
                (re-find #"inet addr:(\d+[.]\d+[.]\d+[.]\d+)")
                (second)
            )
            host (->>
                (sh/execute ["hostname"] :out :pipe)
                (:out)
                (str/trim-newline)
            )
            ]
            [[host inet]]
        )
    )
)
