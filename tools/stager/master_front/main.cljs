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

(defn- now []
    (Date/now)
)

(def ^:private pages [
    [:home "nav-docs" "doc-page"]
    [:slaves "nav-slaves" "slaves-page"]
])

(def slaves (atom []))
(def apps (atom {}))

(defn- apps-contains-slave [slave apps]
    (for [
        [app cfg] apps
        :let [xs (get cfg "slaves")]
        :when (some #{slave} xs)
        ]
        app
    )
)

(defn- slave-not-in-any-apps [slaves apps]
    (for [
        [s] slaves
        :when (empty? (apps-contains-slave s apps))
        ]
        s
    )
)

(defn- refresh-table [_ _ _ _]
    (let [
        slaves @slaves
        apps @apps
        ]
        (dom/set-html! (dom/by-id "dashboard-table")
            (html
                (concat
                    (for [
                        [s] slaves
                        app (apps-contains-slave s apps)
                        ]
                        [:tr [:td s] [:td app]]
                    )
                    (slave-not-in-any-apps slaves apps)
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

(defn- switch-slave-type [ty]
    (-> "slave-type"
        (dom/by-id)
        (dom/set-html! (str (name ty) " <span class=\"caret\"></span>"))
    )
)

(defn- pull-branches' [ts]
    (ajax/GET (ajax/uri-with-params "remote" {:timestamp ts}) {
            :response-format :json
            :handler (fn [resp] (dom/log resp))
            :error-handler handle-error
        })
)

(defn- pull-branches []
    (pull-branches' (now))
)

(defn ^:export on-load []
    (add-watch slaves :slaves refresh-table)
    (add-watch apps :apps refresh-table)
    (fetch-slaves)
    (-> "slave-add-btn"
        (dom/by-id)
        (evt/listen! :click add-slave)
    )
    (-> "slave-type-staging"
        (dom/by-id)
        (evt/listen! :click (partial switch-slave-type :staging))
    )
    (-> "slave-type-production"
        (dom/by-id)
        (evt/listen! :click (partial switch-slave-type :production))
    )
    (-> "pull-branches-btn"
        (dom/by-id)
        (evt/listen! :click pull-branches)
    )
)
