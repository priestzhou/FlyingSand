(ns master.app
    (:require
        [clojure.string :as str]
        [clojure.java.io :as io]
        [clojure.data.json :as json]
        [clojure.pprint :as pp]
    )
    (:use
        [slingshot.slingshot :only (try+ throw+)]
        [logging.core :only [defloggers]]
        [master.core :only (throw+if throw+if-not)]
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
    (prn "get-slaves")
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
        (prn "add-slave" :slave url :type type)
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
