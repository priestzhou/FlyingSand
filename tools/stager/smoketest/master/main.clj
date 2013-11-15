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
        opt {:port 11110, :workdir rt}
        ]
        (with-open [s (CloseableServer. (mw/start-server opt))]
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
