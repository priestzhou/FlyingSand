(ns query-server.core
    (:require
        [clojure.string :as str]
        [domina :as dom]
        [domina.events :as evt]
        [ajax.core :as ajax]
    )
)

(defn on-error [{:keys [status status-text]}]
    (dom/log (format "fetch updates error: %d %s" status status-text))
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

(defn sniff-csv [url]
    (ajax/ajax-request 
        (ajax/uri-with-params url {
            :timestamp (Date/now)
        }) 
        "HEAD"
        (ajax/transform-opts {
            :format :raw
            :response-format :raw
            :handler (fn []
                (-> "url"
                    (dom/by-id)
                    (dom/set-attr! :href url)
                )
            )
            :error-handler (fn [{:keys [status]}]
                (js/setTimeout (partial sniff-csv url) 1000)
            )
        })
    )
)

(defn fetch-result [qid]
    (ajax/GET 
        (ajax/uri-with-params (format "/sql/queries/%s/" qid) {
            :timestamp (Date/now)
        }) 
        {
            :handler (fn [response]
                (if (render-result response)
                    (js/setTimeout (partial fetch-result qid) 1000)
                    (js/setTimeout (partial sniff-csv (response "url")) 1000)
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
        db (-> "db" (dom/by-id) (dom/value) (str/trim))
        ]
        (when (validate-sql sql)
            (ajax/POST 
                (ajax/uri-with-params "/sql/queries/" {
                    :query sql
                    :app app
                    :version ver
                    :db db
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
    (ajax/GET "/sql/meta"
        {
            :handler render-meta
        }
    )
)

(defn render-saved-queries [response]
    (format "
<table>
<thead><tr>
<th align=\"left\">name</th>
<th align=\"left\">app</th>
<th align=\"left\">version</th>
<th align=\"left\">db</th>
<th align=\"left\">query</th>
</tr></thead>
<tbody>%s</tbody>
</table>
"
        (str/join ""
            (for [[_ v] response]
                (format "<tr><td align=\"left\">%s</td><td align=\"left\">%s</td><td align=\"left\">%s</td><td align=\"left\">%s</td><td align=\"left\">%s</td></tr>" 
                    (v "name") (v "app") (v "version") (v "db") (v "query")
                )
            )
        )
    )
)

(def saved-queries (atom {}))

(defn fetch-saved-queries []
    (ajax/GET "/sql/saved/"
        {
            :response-format :json
            :handler (fn [response]
                (reset! saved_queries response)
                (let [html (render-saved-queries response)]
                    (-> "saved_queries"
                        (dom/by-id)
                        (dom/set-html! html)
                    )
                )
            )
        }
    )
)

(defn save-sql []
    (let [
        qname (-> "query_name" (dom/by-id) (dom/value) (str/trim))
        app (-> "app" (dom/by-id) (dom/value) (str/trim))
        ver (-> "version" (dom/by-id) (dom/value) (str/trim))
        db (-> "db" (dom/by-id) (dom/value) (str/trim))
        sql (-> "sql-input" (dom/by-id) (dom/value) (str/trim))
        ]
        (ajax/POST 
            (ajax/uri-with-params "/sql/saved/" {
                :name qname
                :app app
                :version ver
                :db db
                :query sql
            }) 
            {
                :response-format :raw
                :handler (fn [response]
                    (fetch-saved-queries)
                )
                :error-handler (fn [{:keys [response]}]
                    (js/alert (str "duplicated name: " response))
                )
            }
        )
    )
)

(defn delete-sql []
    (let [
        qname (-> "query_name" (dom/by-id) (dom/value) (str/trim))
        qid (for [
            [qid v] @saved_queries
            :let [name (v "name")]
            :when (= name qname)
            ]
            qid
        )
        ]
        (dom/log qid)
        (when-not (empty? qid)
            (ajax/ajax-request (format "/sql/saved/%d/" (first qid))
                "DELETE"
                (ajax/transform-opts {
                    :response-format :raw
                    :handler (fn []
                        (dom/log "OK")
                        (fetch-saved-queries)
                    )
                    :error-handler (fn [{:keys [status-text]}]
                        (dom/log "FAILED" status-text)
                        (fetch-saved-queries)
                    )
                })
            )
        )
    )
)

(defn fetch-history []
    (ajax/GET
        (ajax/uri-with-params "/sql/queries/" {
            :timestamp (Date/now)
        })
        {
            :response-format :json
            :handler (fn [response]
                (-> "history"
                    (dom/by-id)
                    (dom/set-html! (format "
<table>
<thead><tr>
<th align=\"right\">query</th>
<th align=\"right\">status</th>
<th align=\"right\">duration</th>
<th align=\"right\">submit time</th>
<th align=\"url\">url</th>
</tr></thead>
<tbody>
%s
</tbody>
</table>
"                       (str/join "\n"
                            (for [
                                [_ v] response
                                :let [query (v "query")]
                                :let [status (v "status")]
                                :let [submit-time (v "submit-time")]
                                :let [duration (v "duration")]
                                :let [url (v "url")]
                                ]
                                (format 
"<tr><td align=\"right\">%s</td><td align=\"right\">%s</td><td align=\"right\">%s</td><td align=\"right\">%s</td></tr>"
                                    query status duration (.toISOString (js/Date. submit-time)) url
                                )
                            )
                        )
                    ))
                )
            )
            :error-handler (fn [{:keys [status status-text]}]
                (js/alert (format "error %d: %s" status status-text))
            )
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
    (-> "add"
        (dom/by-id)
        (evt/listen! :click save-sql)
    )
    (-> "delete"
        (dom/by-id)
        (evt/listen! :click delete-sql)
    )
    (fetch-meta)
    (fetch-saved-queries)
    (fetch-history)
)
