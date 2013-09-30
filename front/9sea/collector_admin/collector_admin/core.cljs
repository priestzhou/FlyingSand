(ns collector-admin.core
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

(defn add-collector []
    (let [
        name (-> "name" (dom/by-id) (dom/value) (str/trim))
        url (-> "url" (dom/by-id) (dom/value) (str/trim))
        ]
        (when (and name url)
            (ajax/POST
                (ajax/uri-with-params "collectors/" {
                    :name name
                    :url url
                })
                {
                    :response-format :raw
                    :handler (fn [response]
                        (dom/log response)
                    )
                    :error-handler (fn [{:keys [status status-text response]}]
                        (dom/log status-text)
                        (js/alert (format "error %d: %s" status response))
                    )
                }
            )
        )
    )
)

(defn del-collector []
    (let [
        selector (dom/by-id "cids")
        idx (.-selectedIndex selector)
        option (.item selector idx)
        cid (dom/text option)
        ]
        (ajax/ajax-request (format "collectors/%s" cid) "DELETE"
            (ajax/transform-opts {
                :response-format :raw
                :handler (fn [response]
                    (dom/log response)
                )
                :error-handler (fn [{:keys [status status-text response]}]
                    (dom/log status-text)
                    (js/alert (format "error %d: %s" status response))
                )
            })
        )
    )
)

(defn edit-collector []
    (let [
        selector (dom/by-id "cids")
        idx (.-selectedIndex selector)
        option (.item selector idx)
        cid (dom/text option)
        name (-> "name" (dom/by-id) (dom/value) (str/trim))
        url (-> "url" (dom/by-id) (dom/value) (str/trim))
        ]
        (cond
            (nil? name) (js/alert "missing name")
            (nil? url) (js/alert "missing url")
            :else (ajax/ajax-request (format "collectors/%s" cid) "PUT"
                (ajax/transform-opts {
                    :params {
                        :name name
                        :url url
                    }
                    :format :json
                    :response-format :raw
                    :handler (fn [response]
                        (dom/log response)
                    )
                    :error-handler (fn [{:keys [status status-text response]}]
                        (dom/log status-text)
                        (js/alert (format "error %d: %s" status response))
                    )
                })
            )
        )
    )
)

(defn ^:export on-load []
    (-> "cids"
        (dom/by-id)
        (evt/listen! :change change-collector)
    )
    (-> "add"
        (dom/by-id)
        (evt/listen! :click add-collector)
    )
    (-> "delete"
        (dom/by-id)
        (evt/listen! :click del-collector)
    )
    (-> "edit"
        (dom/by-id)
        (evt/listen! :click edit-collector)
    )
    (fetch-collectors)
)
