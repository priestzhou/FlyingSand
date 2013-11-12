(ns smoketest.master.main
    (:use
        [testing.core :only (suite)]
    )
    (:require
        [clojure.data.json :as json]
        [org.httpkit.client :as hc]
        [master.web :as mw]
    )
)

(deftype CloseableServer [stop-server]
    java.lang.AutoCloseable
    (close [this]
        (stop-server)
    )
)

(defn- extract-response [req]
    (let [
        response @req
        status (:status response)
        content-type (cond
            (nil? (:headers response)) nil
            (nil? ((:headers response) "Content-Type")) nil
            :else ((:headers response) "Content-Type")
        )
        body (:body response)
        body (case content-type
            "application/json" (json/read-str body)
            body
        )
        ]
        [status body]
    )
)

(suite "ruok"
    (:fact ruok
        (with-open [s (CloseableServer. (mw/start-server {:port 11110}))]
            (extract-response (hc/get "http://localhost:11110/ruok"))
        )
        :is
        [200 "imok"]
    )
)

(suite "slaves"
)
