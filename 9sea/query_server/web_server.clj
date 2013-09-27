(ns query-server.web-server
    (:require
        [compojure.handler :as handler]
        [compojure.route :as route]
        [korma.core :as db]
        [clojure.data.json :as json]
        [clojure.string :as str]
        [utilities.core :as util]
        [utilities.shutil :as sh]
    )
    (:use
        [compojure.core :only (defroutes GET PUT POST DELETE)]
        [ring.adapter.jetty :only (run-jetty)]
        [korma.db :only (defdb sqlite3)]
        [logging.core :only [defloggers]]
    )
    (:import
        [java.security MessageDigest]
        [java.nio.charset StandardCharsets]
        [java.nio.file Path]
        [java.sql SQLException]
    )
)

(defloggers debug info warn error)

(defdb flying-sand 
    (sqlite3 {:db "flyingsand.db"})
)

(db/defentity users
    (db/pk :user_id)
)

(defn sha1 [^String str]
    (->> str
        (util/str->bytes)
        (.digest (MessageDigest/getInstance "sha1"))
    )
)

(defn authenticate [email psw]
    (let [res (db/select users
            (db/fields :user_id)
            (db/where {:email email :password (-> psw (sha1) (util/hexdigits))})
        )
        ]
        (if (empty? res)
            nil
            (-> res (first) (:user_id))
        )
    )
)

(defn extract-user-id [cookies]
    (when-let [user-id (cookies "user_id")]
        (:value user-id)
    )
)

(def results (ref {}))
(def csv (ref {}))

(defn progress [qid result total-stages current-stage]
    (if (< current-stage total-stages)
        (do
            (dosync
                (let [
                    log (-> (@results qid) (:log) (.concat (format "stage %d\n" current-stage)))
                    ]
                    (alter results 
                        update-in [qid] 
                        assoc :progress [current-stage total-stages] :log log
                    )
                )
            )
            (Thread/sleep 600)
            (recur qid result total-stages (inc current-stage))
        )
        (do
            (let []
                (dosync
                    (alter results
                        update-in [qid]
                        assoc 
                            :result result 
                            :status "done" 
                            :progress [total-stages total-stages]
                            :url (format "query/%d/csv" qid)
                    )
                    (alter csv
                        assoc qid (str/join "\n"
                            (cons
                                (str/join "," (:titles result))
                                (map #(str/join "," %) (:values result))
                            )
                        )
                    )
                )
            )
        )
    )
)

(defn reformat-row [titles row]
    (vec (for [
        t titles
        :let [v (row t)]
        ]
        v
    ))
)

(defn reformat-result [result]
    (let [
        titles (->> result (map keys) (map set) (reduce into) (vec))
        values (->> result (map (partial reformat-row titles)) (vec))
        ]
        {:titles titles :values values}
    )
)

(defn do-query [qid query]
    (let [progress-stages (inc (rand-int 10))]
        (try
            (let [result (db/exec-raw [query] :results)]
                (future (progress qid (reformat-result result) progress-stages 0))
            )
        (catch SQLException ex
            (dosync
                (alter results update-in [qid] assoc :status "failed" :error "invalid sql")
            )
        ))
        {
            :status 201
            :headers {
                "Content-Type" "application/json"
            }
            :body (json/write-str {:id qid})
        }
    )
)

(defn submit-query [params cookies]
    (let [
        user-id (extract-user-id cookies)
        app (:app params)
        ver (:version params)
        query (:query params)
        qid (rand-int 10000)
        ]
        (dosync
            (alter results assoc qid {:status "running" :log ""})
        )
        (do-query qid query)
    )
)

(defn get-result [params]
    (let [
        qid (Long/parseLong (:id params))
        result (dosync
            (let [r (@results qid)]
                (alter results update-in [qid] assoc :log "")
                r
            )
        )
        ]
        (prn qid result)
        {
            :status 200
            :headers {"Content-Type" "application/json"}
            :body (json/write-str result)
        }
    )
)

(defn get-meta []
    {
        :status 200
        :headers {"Content-Type" "application/json"}
        :body (json/write-str
            [
                {
                    :type "namespace"
                    :name "WoW"
                    :children [
                        {
                            :type "namespace"
                            :name "panda"
                            :children [
                                {
                                    :type "table"
                                    :name "smile"
                                    :columns [
                                        {
                                            :name "item"
                                            :type "varchar(255)"
                                        }
                                        {
                                            :name "id"
                                            :type "integer primary key autoincrement"
                                        }
                                    ]
                                    :samples [
                                        ["hehe" 1]
                                        ["haha" 2]
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        )
    }
)

(def saved-queries (ref {}))

(defn get-saved-queries []
    {
        :status 200
        :headers {"Content-Type" "application/json"}
        :body (json/write-str @saved-queries)
    }
)

(defn add-query [params cookies]
    (let [
        qname (:name params)
        sql (:query params)
        r (dosync
            (if (contains? @saved-queries qname)
                qname
                (do
                    (alter saved-queries assoc qname sql)
                    nil
                )
            )
        )
        ]
        (prn qname sql r)
        (if r
            {
                :status 400
                :headers {"Content-Type" "text/plain"}
                :body r
            }
            {
                :status 201
                :headers {"Content-Type" "text/plain"}
                :body "OK"
            }
        )
    )
)

(defn delete-saved-query [params cookies]
    (let [
        qname (:name params)
        sql (:query params)
        r (dosync
            (if-let [q (@saved-queries qname)]
                (if (= q sql)
                    (do
                        (alter saved-queries dissoc qname)
                        nil
                    )
                    {:error "query" :message sql}
                )
                {:error "name" :message qname}
            )
        )
        ]
        (prn qname sql r)
        (if r
            {
                :status 404
                :headers {"Content-Type" "application/json"}
                :body (json/write-str r)
            }
            {
                :status 200
                :headers {"Content-Type" "application/json"}
                :body (json/write-str {})
            }
        )
    )
)

(defn download [qid]
    (if-let [r (@csv qid)]
        {
            :status 200
            :headers {"Content-Type" "text/csv"}
            :body r
        }
        {
            :status 404
        }
    )
)

(defn app [opts]
    (handler/site
        (defroutes app-routes
            (POST "/sql/" {params :params}
                (if-let [auth (authenticate (:email params) (:password params))]
                    {
                        :status 201
                        :headers {
                            "Content-Type" "text/html"
                        }
                        :cookies {"user_id" {:value auth :path "/sql" :max-age 36000}}
                        :body "
<!doctype html>
<html>
<head>
<meta http-equiv='refresh' content='1;url=/sql'>
</head>
</html>
"
                    }
                    {
                        :status 401
                    }
                )
            )
            (POST "/sql/SubmitQuery" {:keys [cookies params]}
                (submit-query params cookies)
            )
            (POST "/sql/AddQuery" {:keys [cookies params]}
                (add-query params cookies)
            )
            (GET "/sql/GetResult" {:keys [params]}
                (get-result params)
            )
            (GET "/sql/query/:qid/csv" [qid]
                (download (Long/parseLong qid))
            )
            (GET "/sql/GetMeta" {:keys [params cookies]}
                (get-meta)
            )
            (GET "/sql/GetSavedQueries" {:keys [cookies]}
                (get-saved-queries)
            )
            (DELETE "/sql/DeleteSavedQuery" {:keys [params cookies]}
                (delete-saved-query params cookies)
            )
            (GET "/sql/" {:keys [cookies]}
                (if (and cookies (get cookies "user_id"))
                    (slurp (.toFile (sh/getPath (:dir opts) "query.html")))
                    (slurp (.toFile (sh/getPath (:dir opts) "index.html")))
                )
            )
            (route/files "/sql/" {:root (:dir opts) :allow-symlinks? true})
            (route/not-found "Not Found")
        )
    )
)

(defn start [opts]
    (run-jetty (app opts)
        {
            :port (:port opts)
            :join? true
        }
    )
)
