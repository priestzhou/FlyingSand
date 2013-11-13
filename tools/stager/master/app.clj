(ns master.app
    (:require
        [clojure.string :as str]
        [clojure.java.io :as io]
        [clojure.data.json :as json]
        [clojure.pprint :as pp]
        [clj-time.core :as time]
        [clj-time.coerce :as coer]
        [utilities.shutil :as sh]
        [master.git :as git]
    )
    (:use
        [slingshot.slingshot :only (try+ throw+)]
        [logging.core :only [defloggers]]
        [master.core :only (throw+if throw+if-not)]
    )
    (:import
        [java.io IOException]
    )
)

(defloggers debug info warn error)

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
        (git/fetch repo)
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
