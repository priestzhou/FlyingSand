(ns smoketest.master.main
    (:use
        [testing.core :only (suite)]
    )
    (:require
        [clojure.data.json :as json]
        [org.httpkit.client :as hc]
        [utilities.shutil :as sh]
        [master.web :as mw]
        [master.git :as git]
    )
    (:import
        [java.nio.file Path]
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
        _ (prn response)
        content-type (cond
            (nil? (:headers response)) nil
            (nil? (:content-type (:headers response))) nil
            :else (:content-type (:headers response))
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
    (:fact slaves:add
        (with-open [s (CloseableServer. (mw/start-server {:port 11110}))]
            (let [r0 (hc/post "http://localhost:11110/slaves/" {
                    :query-params {
                        "url" "http://localhost:11111/"
                        "type" "staging"
                    }
                })
                r0 (extract-response r0)
                r1 (hc/get "http://localhost:11110/slaves/")
                r1 (extract-response r1)
                ]
                [r0 r1]
            )
        )
        :is
        [
            [201 nil]
            [200 [["http://localhost:11111/" "staging"]]]
        ]
    )
)

(comment
(suite "ver"
    (:fact ver:get:tag
        (let [
            rt (sh/tempdir)
            opt {
                :port 11110
                :repository (str (sh/getPath rt "smile"))
                :workdir (str (sh/getPath rt "ws"))
            }
            ]
            (sh/execute ["git" "init" "smile"] :dir rt)
            (with-open [s (CloseableServer. (mw/start-server opt))]
                (let [r0 (hc/get "http://localhost:11110/repository/" {
                        :query-params {"tag" "master"}
                    })
                    r0 (extract-response r0)
                    ]
                    (sh/spitFile (sh/getPath rt "smile" "smile.txt") "hehe")
                    (sh/execute ["git" "add" "smile.txt"] :dir (sh/getPath rt "smile"))
                    (sh/execute ["git" "commit" "-am" "hehe"] :dir (sh/getPath rt "smile"))
                    (let [r1 (hc/get "http://localhost:11110/repository/" {
                            :query-params {"tag" "master"}
                        })
                        r1 (extract-response r1)
                        ]
                        [r0 r1]
                    )
                )
            )
        )
        :is
        [
            [200 {"tag" "xxx"}]
            [200 {"tag" "xxx"}]
        ]
    )
)
)