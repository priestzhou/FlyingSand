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

(declare render-meta-node)

(defn render-meta-table [lvl node]
    (let [
        name (node "name")
        ys (node "columns")
        samples (node "samples")
        ]
        (format "
<h6>%s</h6>
<table>
<thead>
<tr>%s</tr>
<tr>%s</tr>
</thead>
<tbody>
%s
</tbody>
</table>
" 
            name
            (str/join ""
                (for [y ys]
                    (format "<th align=\"right\">%s</th>" (y "name"))
                )
            )
            (str/join ""
                (for [y ys]
                    (format "<th align=\"right\">%s</th>" (y "type"))
                )
            )
            (str/join "\n"
                (for [rows samples]
                    (format "<tr>%s</tr>"
                        (str/join ""
                            (for [cell rows]
                                (format "<td align=\"right\">%s</td>" cell)
                            )
                        )
                    )
                )
            )
        )
    )
)

(defn render-meta-namespace [lvl node]
    (let [
        name (node "name")
        ys (node "children")
        h (case lvl
            1 "h1"
            2 "h2"
            3 "h3"
            4 "h4"
            5 "h5"
        )
        ]
        (format "<%s>%s</%s>%s" h name h
            (str/join ""
                (for [y ys]
                    (render-meta-node (inc lvl) y)
                )
            )
        )
    )
)

(defn render-meta-node [lvl node]
    (let [type (node "type")]
        (case type
            "namespace" (render-meta-namespace lvl node)
            "table" (render-meta-table lvl node)
        )
    )
)

(defn render-meta-top [xs]
    (str/join ""
        (for [x xs]
            (render-meta-node 1 x)
        )
    )
)

(defn render-meta [response]
    (let [html (render-meta-top response)]
        (-> "meta"
            (dom/by-id)
            (dom/set-html! html)
        )
    )
)

(defn fetch-meta []
    (ajax/GET "/sql/GetMeta"
        {
            :handler render-meta
        }
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
    (fetch-meta)
)
