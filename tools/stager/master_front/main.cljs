(ns master-front.main
    (:require
        [clojure.string :as str]
        [domina :as dom]
        [domina.events :as evt]
        [ajax.core :as ajax]
        [hiccups.runtime :as hic]
    )
    (:use-macros
        [hiccups.core :only (html)]
    )
    (:use
        [utilities.core :only (->js-obj ->cljs-coll enumerate zip nested-merge)]
    )
)

(defn- handle-error [resp]
    (dom/log (pr-str resp))
)

(def ^:private pages [
    [:home "nav-docs" "doc-page"]
    [:slaves "nav-slaves" "slaves-page"]
])

(def slaves (atom []))

(defn- refresh-table [_ _ _ _]
    (let [ss @slaves]
        (dom/set-html! (dom/by-id "slaves-table")
            (html
                (for [[k] ss]
                    [:tr [:td k]]
                )
            )
        )
    )
)

(defn- fetch-slaves []
    (ajax/GET "slaves/"
        {
            :response-format :json
            :handler (fn [response]
                (reset! slaves response)
            )
        }
    )
)

(defn- add-slave []
    (let [
        url (-> "slave-url" (dom/by-id) (dom/value))
        t (-> "slave-type" (dom/by-id) (dom/text))
        ]
        (ajax/POST (ajax/uri-with-params "slaves/" {:url url :type t})
            {
                :response-format :json
                :handler (fn [] (fetch-slaves))
                :error-handler handle-error
            }
        )
    )
)

(defn ^:export on-load []
    (add-watch slaves :refresh refresh-table)
    (fetch-slaves)
    (-> "slave-add-btn"
        (dom/by-id)
        (evt/listen! :click add-slave)
    )
)
