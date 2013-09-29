(ns collector-admin.core
    (:use
        [domina.css :only (sel)]
    )
    (:require
        [clojure.string :as str]
        [domina :as dom]
        [domina.events :as evt]
        [ajax.core :as ajax]
    )
)

(def collectors (atom {}))

(defn on-error [{:keys [status status-text]}]
    (dom/log (format "fetch updates error: %d %s" status status-text))
)

(defn render-collectors-tbl [response]
    (str/join "\n"
        (for [
            [k v] response
            :let [name (v "name")]
            :let [status (v "status")]
            :let [recent-sync (v "recent-sync")]
            :let [synced-data (v "synced-data")]
            ]
            (format "<tr><td>%s</td><td>%s</td><td>%d</td><td>%s</td></tr>"
                name
                (if recent-sync (.toISOString (js/Date. recent-sync)) "")
                (if synced-data synced-data 0)
                status
            )
        )
    )
)

(defn render-cids [cids]
    (str/join "\n"
        (for [cid cids]
            (format "<option>%s</option>" cid)
        )
    )
)

(defn render-collectors [response]
    (-> "collectors"
        (dom/by-id)
        (dom/set-html! (render-collectors-tbl response))
    )
    (let [cids (keys response)]
        (-> "cids"
            (dom/by-id)
            (dom/set-html! (render-cids cids))
        )
        (when-not (empty? cids)
            (let [
                cid (first cids)
                name ((response cid) "name")
                url ((response cid) "url")
                ]
                (-> "name" (dom/by-id) (dom/set-value! name))
                (if url
                    (-> "url" (dom/by-id) (dom/set-value! url))
                )
            )
        )
    )
    (reset! collectors response)
)

(defn fetch-collectors []
    (ajax/GET "collectors/"
        {
            :handler render-collectors
            :error-handler on-error
        }
    )
)

(defn change-collector []
    (let [
        selector (dom/by-id "cids")
        idx (.-selectedIndex selector)
        option (.item selector idx)
        cid (dom/text option)
        ]
        (let [
            collector (@collectors cid)
            name (collector "name")
            url (collector "url")
            ]
            (-> "name" (dom/by-id) (dom/set-value! name))
            (if url
                (-> "url" (dom/by-id) (dom/set-value! url))
            )
        )
    )
)

(defn ^:export on-load []
    (-> "cids"
        (dom/by-id)
        (evt/listen! :change change-collector)
    )
    (fetch-collectors)
)
