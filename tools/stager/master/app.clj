(ns master.app
    (:require
        [clojure.string :as str]
        [clojure.java.io :as io]
        [clojure.data.json :as json]
        [clojure.pprint :as pp]
        [clj-time.core :as time]
        [clj-time.coerce :as coer]
        [utilities.core :as util]
        [utilities.shutil :as sh]
        [master.git :as git]
    )
    (:use
        [slingshot.slingshot :only (try+ throw+)]
        [logging.core :only (defloggers)]
        [master.core :only (throw+if throw+if-not)]
    )
    (:import
        [java.io IOException StringWriter]
        [java.nio.file Path Files CopyOption StandardCopyOption]
    )
)

(defloggers debug info warn error)

(defmacro must [condition & {:keys [else]}]
    `(when-not ~condition
        (throw+ {:status ~else})
    )
)

(defmacro must-not [condition & {:keys [else]}]
    `(when ~condition
        (throw+ {:status ~else})
    )
)

(def ^:private slaves (ref (sorted-map)))
(def ^:private save-slaves-agent (agent nil))

(defn- watch-slaves [_ _ old new]
    (when (not= old new)
        (send-off save-slaves-agent (fn [f]
            (with-open [wrt (io/writer f)]
                (pp/pprint new wrt)
            )
            f
        ))
    )
)

(defn init-slaves [opts]
    (let [
        f (:slaves opts)
        s (->> f
            (slurp)
            (read-string)
        )
        ]
        (debug "init-slaves" :slaves (str s))
        (dosync
            (alter slaves into s)
        )
        (send save-slaves-agent (fn [_] f))
        (add-watch slaves :save watch-slaves)
    )
)

(defn get-slaves []
    (let [res (vec (for [[url ty] @slaves] [url ty]))]
        {
            :status 200
            :headers {"Content-Type" "application/json"}
            :body (json/write-str res)
        }
    )
)

(defn add-slave [params]
    (let [
        {:keys [url type]} params
        type (keyword type)
        ]
        (info "add-slave" :slave url :type type)
        (dosync
            (throw+if (@slaves url) {
                :status 409
                :headers {"Content-Type" "application/json"}
                :body (json/write-str {:error "duplicated url"})
            })
            (alter slaves assoc url type)
        )
        {
            :status 201
            :headers {"Content-Type" "application/json"}
            :body "null"
        }
    )
)

(def ^:private remote-worker (agent {:recent-sync 0}))
(def ^:private remote-receptionist (agent :waiting))

(defn- fetch-remote-work [repo old]
    (try
        (info "fetch remote")
        (git/fetch repo)
        (info "remote fetched")
        (let [
            ts (coer/to-long (time/now))
            branches (git/show-branches repo :remote true)
            ]
            (send remote-receptionist (constantly :waiting))
            (into {:recent-sync ts}
                (doall (for [x branches]
                    [x (git/show-commit repo :branch x)]
                ))
            )
        )
    (catch IOException ex
        (error "fail to fetch remote" :error (util/except->str ex))
        old
    ))
)

(defn fetch-remote [opts params]
    (let [
        ts (Long/parseLong (:timestamp params))
        repo (sh/getPath (:workdir opts) "remote")
        w @remote-worker
        r @remote-receptionist
        ]
        (info "fetch-remote" :timestamp ts :receptionist r :worker w)
        (cond
            (<= ts (:recent-sync w)) {
                :status 200
                :headers {"Content-Type" "application/json"}
                :body (json/write-str w)
            }
            (= r :busy) {:status 102}
            :else (do
                (send remote-receptionist
                    (fn [_]
                        (send-off remote-worker (partial fetch-remote-work repo))
                        :busy
                    )
                )
                {:status 102}
            )
        )
    )
)

(def ^:private versions (ref {}))

(defn- build [opts ver old]
    (let [
        ws (:workdir opts)
        tmp-repo (sh/getPath ws "tmp" ver)
        dst-repo (sh/getPath ws "repository" ver)
        remote (sh/getPath ws "remote")
        ]
        (sh/mkdir (.getParent tmp-repo))
        (sh/mkdir (.getParent dst-repo))
        (debug "start to clone" :ver ver)
        (git/clone tmp-repo :src (.toUri remote))
        (git/checkout tmp-repo :branch ver)
        (debug "cloned. start to build" :ver ver)
        (let [
            cout (sh/getPath tmp-repo "build.log")
            r (sh/execute ["scons"] :out cout :err :out :dir tmp-repo)
            ]
            (when-not (= (:exitcode r) 0)
                (error "fail to build" :ver ver)
                (throw (RuntimeException. "fail to build"))
            )
            (debug "built" :ver ver)
            (Files/move tmp-repo dst-repo
                (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE])
            )
            (doall (for [
                f (sh/postwalk dst-repo)
                :let [f (.relativize dst-repo f)]
                :when (not (= f (sh/getPath "build.log")))
                :when (not (= f (sh/getPath ".sconsign.dblite")))
                :when (not (.startsWith f (sh/getPath ".git")))
                :when (not= (str f) "")
                ]
                (str f)
            ))
        )
    )
)

(defn fetch-ver [opts params]
    (debug :fetch-ver @versions)
    (let [
        ver (:ver params)
        path (:* params)
        _ (info "fetch-ver" :ver ver :path path)
        repo (sh/getPath (:workdir opts) "repository" ver)
        res (dosync
            (if-let [ag (@versions ver)]
                @ag
                (let [ag (agent {:status 102})]
                    (alter versions assoc ver ag)
                    (send-off ag (partial build opts ver))
                    {:status 102}
                )
            )
        )
        ]
        (cond
            (:status res) res
            (empty? path) {
                :status 200
                :headers {"Content-Type" "application/json"}
                :body (json/write-str res)
            }
            (.endsWith path "/")
            (let [fs (for [
                    f res
                    :when (.startsWith f path)
                    ]
                    f
                )
                ]
                {
                    :status 200
                    :headers {"Content-Type" "application/json"}
                    :body (json/write-str fs)
                }
            )
            :else (let [f (.toFile (sh/getPath repo path))]
                (if-not (.exists f)
                    {:status 404}
                    {
                        :status 200
                        :headers {"Content-Type" "application/octet-stream"}
                        :body f
                    }
                )
            )
        )
    )
)


(def ^:private apps {"master" {}})

(defn- clone-app [repo app src]
    (must-not (nil? app) :else 400)
    (must-not (nil? src) :else 400)
    (locking #'apps
        (info "app" :method "clone" :app app :src src)
        (must-not (nil? (apps src)) :else 400)
        (must (nil? (apps app)) :else 409)
        (try+
            (git/checkout repo :branch src)
            (git/branch repo :branch app)
            (let [
                c (sh/slurpFile (sh/getPath repo "config"))
                c (json/read-str c)
                ]
                (alter-var-root #'apps assoc app c)
                {:status 201}
            )
        (catch IOException ex
            (throw+ {:status 422})
        ))
    )
)

(defn- update-app [repo app author body]
    (must-not (nil? app) :else 400)
    (must-not (nil? author) :else 400)
    (must-not (nil? body) :else 400)
    (locking #'apps
        (info "app" :method "update" :app app :author author)
        (must-not (nil? (apps app)) :else 404)
        (try+
            (let [
                body (json/read (io/reader body))
                swrt (StringWriter.)
                _ (pp/pprint body swrt)
                s (str swrt)
                ]
                (git/checkout repo :branch app)
                (sh/spitFile (sh/getPath repo "config") s)
                (git/commit repo :msg author)
                (alter-var-root #'apps assoc app body)
                {:status 202}
            )
        (catch IOException ex
            (throw+ {:status 422})
        ))
    )
)

(defn put-app [opts {:keys [method app author src]} body]
    (must-not (nil? method) :else 400)
    (let [repo (sh/getPath (:workdir opts) "apps")]
        (case method
            "clone" (clone-app repo app src)
            "update" (update-app repo app author body)
            (do
                (error "unknown app method" :method method)
                (throw+ {:status 405})
            )
        )
    )
)

(defn get-app [opts]
    (locking #'apps
        (info "app" :method "get")
        {
            :status 200
            :headers {"Content-Type" "application/json"}
            :body (json/write-str apps)
        }
    )
)
