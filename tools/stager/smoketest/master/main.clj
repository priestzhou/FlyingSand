(ns smoketest.master.main
    (:use
        [testing.core :only (suite)]
    )
    (:require
        [clojure.data.json :as json]
        [clj-time.core :as time]
        [clj-time.coerce :as coer]
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

(defn- update-repo [now]
    (Thread/sleep 100)
    (let [r (hc/get
            (format "http://localhost:11110/remote?timestamp=%d" now)
        )
        r @r
        ]
        (cond
            (= (:status r) 102) (recur now)
            :else r
        )
    )
)

(suite "repository"
    (:fact repo:update
        (let [
            rt (sh/tempdir)
            repo (sh/getPath rt "repo")
            opt {
                :port 11110
                :workdir rt
            }
            ]
            (git/init repo)
            (sh/spitFile (sh/getPath repo "smile.txt") "hehe")
            (git/commit repo :msg "hehe")
            (git/clone (sh/getPath rt "remote") :src (.toUri repo))
            (git/branch repo :branch "smile")
            (with-open [s (CloseableServer. (mw/start-server opt))]
                (let [r (update-repo (coer/to-long (time/now)))]
                    (cond
                        (= (:status r) 200) (-> r
                            (:body)
                            (json/read-str)
                            (dissoc "recent-sync")
                            (keys)
                            (sort)
                        )
                        :else (:status r)
                    )
                )
            )
        )
        :is
        ["origin/master" "origin/smile"]
    )
)