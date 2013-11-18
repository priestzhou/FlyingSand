(ns smoketest.master.main
    (:use
        [testing.core :only (suite)]
        [slingshot.slingshot :only (try+ throw+)]
        [compojure.core :only (defroutes GET PUT POST DELETE HEAD ANY)]
        [logging.core :only (defloggers)]
    )
    (:require
        [clojure.java.io :as io]
        [clojure.data.json :as json]
        [clj-time.core :as time]
        [clj-time.coerce :as coer]
        [compojure.handler :as handler]
        [compojure.route :as route]
        [org.httpkit.server :as http]
        [org.httpkit.client :as hc]
        [utilities.core :as util]
        [utilities.shutil :as sh]
        [master.web :as mw]
        [master.git :as git]
        [master.app :as app]
    )
    (:import
        [java.nio.file Path]
        [java.net URI]
    )
)

(defloggers debug info warn error)

(deftype CloseableServer [stop-server]
    java.lang.AutoCloseable
    (close [this]
        (stop-server)
    )
)

(defn- resp [req]
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
            "application/octet-stream" (slurp body)
            body
        )
        ]
        [status body]
    )
)

(def ^:private default-url (URI. "http://localhost:11110/"))

(defn- http-resolve [res args]
    (if (empty? args)
        [res {}]
        (let [[x & xs] args]
            (if (keyword? x)
                [res (apply hash-map args)]
                (recur (.resolve res x) xs)
            )
        )
    )
)

(defmacro def-http-method [symb method]
    `(defn- ~symb [& args#]
        (let [[url# params#] (http-resolve default-url args#)]
            (~method (str url#) {:query-params params#})
        )
    )
)

(defn- d [p]
    (if (.endsWith p "/") p (str p "/"))
)

(def-http-method http-get hc/get)
(def-http-method http-post hc/post)
(def-http-method http-put hc/put)

(suite "ruok"
    (:fact ruok
        (with-open [s (CloseableServer. (mw/start-server {:port 11110}))]
            (resp (http-get "ruok"))
        )
        :is
        [200 "imok"]
    )
)

(defn- tb [test]
    (let [
        rt (sh/tempdir)
        opt {:port 11110, :workdir rt
            :my-repository "http://localhost:11110/repository/"
        }
        ]
        (with-open [master (CloseableServer. (mw/start-server opt))]
            (test rt opt)
        )
    )
)

(suite "slaves"
    (:testbench tb)
    (:fact slaves:add
        (fn [rt opt]
            (let [
                r0 (resp (http-post "slaves/"
                    :url "http://localhost:11111/" :type "staging"))
                r1 (resp (http-get "slaves/"))
                ]
                [r0 r1]
            )
        )
        :eq
        (fn [& _]
            [
                [201 nil]
                [200 [["http://localhost:11111/" "staging"]]]
            ]
        )
    )
)

(defn- continuable-request [fire]
    (Thread/sleep 100)
    (let [r (resp (fire))
        ]
        (cond
            (= (first r) 102) (recur fire)
            (= (first r) 200) r
            :else (throw+ r)
        )
    )

)

(defn- update-repo []
    (continuable-request (partial http-get "remote"
        :timestamp (coer/to-long (time/now))))
)

(defn- build-repo [ver]
    (continuable-request (partial http-get "repository/" (d ver)))
)

(suite "repository"
    (:testbench tb)
    (:fact repo:update
        (fn [rt opt]
            (let [repo (sh/getPath rt "repo")]
                (git/init repo)
                (sh/spitFile (sh/getPath repo "smile.txt") "hehe")
                (git/commit repo :msg "hehe")
                (git/clone (sh/getPath rt "remote") :src (.toUri repo))
                (git/branch repo :branch "smile")
                (-> (update-repo)
                    (second)
                    (dissoc "recent-sync")
                    (keys)
                    (sort)
                )
            )
        )
        :eq
        (fn [& _]
            ["origin/master" "origin/smile"]
        )
    )
    (:fact repo:build
        (fn [rt opt]
            (let [
                tmp (sh/getPath rt "tmp")
                repo (sh/getPath rt "repo")
                _ (sh/mkdir tmp)
                _ (git/init repo)
                _ (sh/spitFile (sh/getPath repo "SConstruct") "
env = Environment(tools=['textfile'])
env.Textfile('smile.txt', source=['haha'])
")
                _ (git/commit repo :msg "scons")
                _ (git/clone (sh/getPath rt "remote") :src (.toUri repo))
                r (update-repo)
                ver ((second r) "origin/master")
                r1 (build-repo ver)
                r2 (resp (http-get "repository/" (d ver) "smile.txt"))
                ]
                [(sort (second r1)) (second r2)]
            )
        )
        :eq
        (fn [& _]
            [["SConstruct" "smile.txt"] "haha"]
        )
    )
)

(suite "apps"
    (:testbench tb)
    (:fact apps:add
        (fn [rt opt]
            (let [
                apps (sh/getPath rt "apps")
                _ (git/init apps)
                _ (sh/spitFile (sh/getPath apps "config") "{}")
                _ (git/commit apps :msg "empty")

                r0 (resp (http-put "apps/a0" :method "clone" :src "master"))
                _ (debug "clone" :app "a0" :src "master" :response r0)
                _ (assert (= (first r0) 201))

                r1 (resp (http-put "apps/a0" :method "update" :author "taoda"
                    :body (json/write-str {:smile "hehe"})
                ))
                _ (debug "update" :app "a0" :src "master" :response r1)
                _ (assert (= (first r1) 202))

                r2 (resp (http-put "apps/a1" :method "clone" :src "a0"))
                _ (debug "clone" :app "a1" :src "master" :response r2)
                _ (assert (= (first r2) 201))

                r3 (resp (http-put "apps/a1" :method "update" :author "taoda"
                    :body (json/write-str {:smile "haha"})
                ))
                _ (debug "update" :app "a1" :src "master" :response r3)
                _ (assert (= (first r3) 202))

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
        :eq
        (fn [& _]
            [{} {"smile" "hehe"} {"smile" "haha"}]
        )
    )
    (:fact apps:get
        (fn [rt opt]
            (let [
                apps (sh/getPath rt "apps")
                _ (git/init apps)
                _ (sh/spitFile (sh/getPath apps "config") "{}")
                _ (git/commit apps :msg "empty")

                r0 (resp (http-put "apps/a0" :method "clone" :src "master"))
                _ (debug "clone" :app "a0" :src "master" :response r0)
                r1 (resp (http-put "apps/a0" :method "update" :author "taoda"
                    :body (json/write-str {:smile "hehe"})
                ))
                _ (debug "update" :app "a0" :src "master" :response r1)

                r2 (resp (http-get "apps/"))
                ]
                r2
            )
        )
        :eq
        (fn [& _]
            [200 {"master" {}, "a0" {"smile" "hehe"}}]
        )
    )
)

(defn- tb [test]
    (let [
        rt (sh/tempdir)
        opt {:port 11110, :workdir rt
            :my-repository "http://localhost:11110/repository/"
        }
        ]
        (with-open [master (CloseableServer. (mw/start-server opt))]
            (test rt opt)
        )
    )
)

(defn- start-slave [req->slave]
    (http/run-server
        (handler/api
            (defroutes app-routes
                (PUT "/apps/" req
                    (reset! req->slave
                        (-> req (:body) (io/reader) (json/read :key-fn keyword))
                    )
                    (debug :req->slave req)
                    {:status 202}
                )
                (route/not-found "Not Found")
            )
        )
        {:port 11111}
    )
)

(suite "deploy"
    (:testbench
        (fn [test]
            (let [
                rt (sh/tempdir)
                opt {:port 11110, :workdir rt
                    :my-repository "http://localhost:11110/repository/"
                }
                req->slave (atom nil)
                ]
                (with-open [
                    master (CloseableServer. (mw/start-server opt))
                    slave (CloseableServer. (start-slave req->slave))
                    ]
                    (test rt opt req->slave)
                )
            )
        )
    )
    (:fact deploy:get:no-deployment
        (fn [& _]
            (first (resp (http-get "deploy")))
        )
        :eq
        (fn [& _] 404)
    )
    (:fact deploy:post
        (fn [rt opt req->slave]
            (let [
                slave-url "http://localhost:11111/"
                app-opt {
                    "app" {
                        :slaves [slave-url]
                        :ver "ver"
                        :todo "cheese"
                    }
                }
                ]
                (with-redefs [
                    app/slaves (ref {slave-url "staging"})
                    app/versions (ref {"ver" ["smile.txt"]})
                    app/apps (ref app-opt)
                    ]
                    (http-post "deploy" :apps (json/write-str app-opt))
                    (Thread/sleep 1000)
                    (let [
                        req->slave @req->slave
                        resp<-master (resp (http-get "deploy"))
                        ]
                        [req->slave resp<-master]
                    )
                )
            )
        )
        :eq
        (fn [& _]
            [
                [{:app "app" :ver "ver" :todo "cheese"
                    :sources ["http://localhost:11110/repository/"]}]
                [200 [{"slave" "http://localhost:11111/", "status" 202}]]
            ]
        )
    )
)

(suite "failover"
    (:fact failover
        (let [
            rt (sh/tempdir)
            opt {:port 11110, :workdir rt}
            slaves-file (sh/getPath rt "slaves")
            repo (sh/getPath rt "repository")
            apps (sh/getPath rt "apps")
            ]
            (debug "failover" :opt opt)

            (sh/spitFile slaves-file
                (json/write-str [["http://localhost:11111/" "staging"]]))

            (sh/spitFile (sh/getPath repo "a" "smile.txt") "haha")

            (git/init apps)
            (sh/spitFile (sh/getPath apps "config") "{}")
            (git/commit apps :msg "empty")
            (git/branch apps :branch "a0")
            (sh/spitFile (sh/getPath apps "config") (json/write-str {"smile" "hehe"}))
            (git/commit apps :msg "a0")

            (app/init opt)

            (shutdown-agents)
            (let [
                vs (into (sorted-map) (for [
                    [k v] @@#'app/versions
                    ]
                    [k @v]
                ))
                ]
                [@@#'app/slaves vs @#'app/apps]
            )
        )
        :is
        [
            {"http://localhost:11111/" "staging"}
            {"a" ["smile.txt"]}
            {"master" {}, "a0" {"smile" "hehe"}}
        ]
    )
)
