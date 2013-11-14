(ns smoketest.master.main
    (:use
        [testing.core :only (suite)]
        [slingshot.slingshot :only (try+ throw+)]
        [logging.core :only (defloggers)]
    )
    (:require
        [clojure.data.json :as json]
        [clj-time.core :as time]
        [clj-time.coerce :as coer]
        [org.httpkit.client :as hc]
        [utilities.core :as util]
        [utilities.shutil :as sh]
        [master.web :as mw]
        [master.git :as git]
    )
    (:import
        [java.nio.file Path]
    )
)

(defloggers debug info warn error)

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
            (= (:status r) 200) (json/read-str (:body r))
            :else (throw+ r)
        )
    )
)

(defn- build-repo [ver]
    (Thread/sleep 1000)
    (let [r (hc/get
            (format "http://localhost:11110/repository/%s/" ver)
        )
        r @r
        ]
        (cond
            (= (:status r) 102) (recur ver)
            (= (:status r) 200) (json/read-str (:body r))
            :else (throw+ r)
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
                    (-> r
                        (dissoc "recent-sync")
                        (keys)
                        (sort)
                    )
                )
            )
        )
        :is
        ["origin/master" "origin/smile"]
    )
    (:fact repo:build
        (let [
            rt (sh/tempdir)
            tmp (sh/getPath rt "tmp")
            repo (sh/getPath rt "repo")
            opt {
                :port 11110
                :workdir rt
            }
            ]
            (sh/mkdir tmp)
            (git/init repo)
            (sh/spitFile (sh/getPath repo "SConstruct") "
env = Environment(tools=['textfile'])
env.Textfile('smile.txt', source=['haha'])
")
            (git/commit repo :msg "scons")
            (git/clone (sh/getPath rt "remote") :src (.toUri repo))
            (with-open [s (CloseableServer. (mw/start-server opt))]
                (let [
                    r (update-repo (coer/to-long (time/now)))
                    ver (r "origin/master")
                    r1 (build-repo ver)
                    r2 (hc/get
                        (format "http://localhost:11110/repository/%s/smile.txt" ver)
                    )
                    r2 @r2
                    ]
                    [(sort r1) (slurp (:body r2))]
                )
            )
        )
        :is
        [["SConstruct" "smile.txt"] "haha"]
    )
)

(suite "apps"
    (:fact apps:add
        (let [
            rt (sh/tempdir)
            apps (sh/getPath rt "apps")
            opt {
                :port 11110
                :workdir rt
            }
            ]
            (git/init apps)
            (sh/spitFile (sh/getPath apps "config") "{}")
            (git/commit apps :msg "empty")
            (with-open [s (CloseableServer. (mw/start-server opt))]
                (let [
                    r0 (hc/put "http://localhost:11110/apps/a0" {
                            :query-params {
                                :method "clone"
                                :src "master"
                            }
                        }
                    )
                    r0 @r0
                    _ (debug "clone" :app "a0" :src "master" :response r0)
                    _ (assert (= (:status r0) 201))
                    r1 (hc/put "http://localhost:11110/apps/a0" {
                                :query-params {
                                    :method "update"
                                    :author "taoda"
                                }
                                :body (json/write-str {
                                    :smile "hehe"
                                }
                            )
                        }
                    )
                    r1 @r1
                    _ (debug "update" :app "a0" :src "master" :response r1)
                    _ (assert (= (:status r1) 202))
                    r2 (hc/put "http://localhost:11110/apps/a1" {
                            :query-params {
                                :method "clone"
                                :src "a0"
                            }
                        }
                    )
                    r2 @r2
                    _ (debug "clone" :app "a1" :src "master" :response r2)
                    _ (assert (= (:status r2) 201))
                    r3 (hc/put "http://localhost:11110/apps/a1" {
                                :query-params {
                                    :method "update"
                                    :author "taoda"
                                }
                                :body (json/write-str {
                                    :smile "haha"
                                }
                            )
                        }
                    )
                    r3 @r3
                    _ (debug "update" :app "a1" :src "master" :response r3)
                    _ (assert (= (:status r3) 202))

                    _ (git/checkout apps :branch "master")
                    m (json/read-str (sh/slurpFile (sh/getPath apps "config")))
                    _ (git/checkout apps :branch "a0")
                    a0 (json/read-str (sh/slurpFile (sh/getPath apps "config")))
                    _ (git/checkout apps :branch "a1")
                    a1 (json/read-str (sh/slurpFile (sh/getPath apps "config")))
                    ]
                    [m a0 a1]
                )
            )
        )
        :is
        [{} {"smile" "hehe"} {"smile" "haha"}]
    )
)
