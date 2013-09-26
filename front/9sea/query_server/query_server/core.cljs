(ns query-server.core
    (:require
        [clojure.string :as str]
        [domina :as dom]
        [domina.events :as evt]
        [ajax.core :as ajax]
    )
)

(defn on-error [{:keys [status response]}]
    (dom/log (format "fetch updates error: %d %s" status response))
)

(defn validate-sql [sql]
    (cond 
        (empty? sql) (do
            (js/alert "sql不能为空")
            false
        )
        :else true
    )
)

(defn format-titles [titles]
    (str/join ""
        (for [t titles]
            (format "<td align=\"right\">%s</td>" t)
        )
    )
)

(defn format-values [values]
    (str/join ""
        (for [r values]
            (format "<tr>%s</tr>"
                (str/join ""
                    (for [v r]
                        (str "<td align=\"right\">" v "</td>")
                    )
                )
            )
        )
    )
)

(defn format-table [result]
    (let [
        titles (result "titles")
        values (result "values")
        ]
        (format "<table><thead>%s</thead><tbody>%s</tbody></table>"
            (format-titles titles)
            (format-values values)
        )
    )
)

(defn render-result [response]
    (when-let [result (response "result")]
        (let [html (format-table result)]
            (-> "result_tbl"
                (dom/by-id)
                (dom/set-html! html)
            )
        )
    )
    (when-let [new-log (response "log")]
        (let [
            l (dom/by-id "log")
            cur-log (dom/value l)
            ]
            (dom/set-value! l (+ cur-log new-log))
        )
    )
    (when-let [progress (response "progress")]
        (let [p (dom/by-id "progress")]
            (dom/set-attrs! p {:value (first progress) :max (second progress)})
        )
    )
    (when-let [error-msg (response "error")]
        (let [
            l (dom/by-id "log")
            ]
            (dom/set-value! l error-msg)
        )
    )
    (when-let [status (response "status")]
        (= status "running")
    )
)

(defn fetch-result [qid]
    (ajax/GET 
        (ajax/uri-with-params "/sql/GetResult" {
            :id qid
            :timestamp (Date/now)
        }) 
        {
            :handler (fn [response]
                (if (render-result response)
                    (js/setTimeout (partial fetch-result qid) 1000)
                )
            )
            :error-handler on-error
        }
    )
)

(defn submit-sql []
    (let [
        app (-> "app" (dom/by-id) (dom/value) (str/trim))
        ver (-> "version" (dom/by-id) (dom/value) (str/trim))
        sql (-> "sql-input" (dom/by-id) (dom/value) (str/trim))
        ]
        (when (validate-sql sql)
            (ajax/POST 
                (ajax/uri-with-params "/sql/SubmitQuery" {
                    :query sql
                    :app app
                    :version ver
                }) 
                {
                    :handler (fn [response]
                        (when-let [qid (response "id")]
                            (fetch-result qid)
                        )
                    )
                    :error-handler (fn [{:keys [status response]}]
                        (js/alert (format "error %d: %s" status response))
                    )
                }
            )
        )
    )
)

(defn ^:export on-load []
    (-> "submit-sql"
        (dom/by-id)
        (evt/listen! :click submit-sql)
    )
    (-> "log"
        (dom/by-id)
        (dom/set-value! "")
    )
)
