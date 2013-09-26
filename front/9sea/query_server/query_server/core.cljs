(ns query-server.core
    (:require
        [clojure.string :as str]
        [domina :as dom]
        [domina.events :as evt]
        [ajax.core :as ajax]
    )
)

(defn on-error [{:keys [status status-text]}]
    (dom/log (format "fetch updates error: %d %s " status status-text))
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

(defn process-result [response]
    (when-let [result (response "result")]
        (dom/log result)
    )
    (when-let [new-log (response "log")]
        (dom/log logs)
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
                (if (process-result response)
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
                    :error-handler on-error
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
)
