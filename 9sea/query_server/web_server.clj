(ns query-server.web-server
    (:require
        [compojure.handler :as handler]
        [compojure.route :as route]
        [korma.core :as db]
        [clojure.data.json :as json]
        [clojure.string :as str]
        [utilities.core :as util]
        [utilities.shutil :as sh]
        [clj-time.core :as time]
    )
    (:use
        [compojure.core :only (defroutes GET PUT POST DELETE HEAD ANY)]
        [ring.adapter.jetty :only (run-jetty)]
        [korma.db :only (defdb sqlite3)]
        [logging.core :only [defloggers]]
        [clj-time.coerce :only (to-long)]
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
                            :url (format "queries/%d/csv" qid)
                    )
                )
                (Thread/sleep 3000)
                (dosync
                    (alter csv
                        assoc qid (str/join "\n"
                            (cons
                                (str/join "," (:titles result))
                                (map #(str/join "," %) (:values result))
                            )
                        )
                    )
                )
                (prn "done" @csv)
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
        db (:db params)
        query (:query params)
        qid (rand-int 10000)
        ]
        (dosync
            (alter results assoc qid {
                :status "running" 
                :query query
                :log "" 
                :submit-time (to-long (time/now))
            })
        )
        (do-query qid query)
    )
)

(defn get-result [qid]
    (let [
        qid (Long/parseLong qid)
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
                                    :type "namespace"
                                    :name "db"
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
                }
            ]
        )
    }
)

(def saved-queries (ref {}))

(defn get-saved-queries []
    (prn @saved-queries)
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
                    (alter saved-queries assoc (rand-int 1000) {:name qname :query sql})
                    nil
                )
            )
        )
        ]
        (prn r)
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

(defn delete-saved-query [cookies params]
    (let [
        id (->> params (:qid) (Long/parseLong))
        r (dosync
            (if-let [q (@saved-queries id)]
                (do
                    (alter saved-queries dissoc id)
                    id
                )
            )
        )
        ]
        (if r
            {
                :status 200
            }
            {
                :status 404
            }
        )
    )
)

(defn download [qid]
    (prn qid)
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

(defn sniff [qid]
    (prn qid @csv)
    (if-let [r (@csv qid)]
        {
            :status 200
        }
        {
            :status 404
        }
    )
)

(defn list-queries [cookies]
    (let [
        user-id (extract-user-id cookies)
        r (dosync
            (let [ks (keys @results)]
                (into {}
                    (for [
                        k ks
                        :let [v (@results k)]
                        :let [{:keys [query status url submit-time]} v]
                        ]
                        [k (merge 
                                {:query query :status status :submit-time submit-time}
                                (if url {:url url} {})
                            )
                        ]
                    )
                )
            )
        )
        ]
        {
            :status 200
            :headers {"Content-Type" "application/json"}
            :body (json/write-str r)
        }
    )
)

(defn app [opts]
    (handler/site
        (defroutes app-routes
            (ANY "/sql" {}
                {
                    :status 200
                    :headers {"Content-Type" "text/html"}
                    :body "
<!doctype html>
<html>
<head>
<meta http-equiv='refresh' content='1;url=/sql/'>
</head>
</html>
"
                }
            )
            (POST "/sql/" {params :params}
                (if-let [auth (authenticate (:email params) (:password params))]
                    {
                        :status 201
                        :headers {
                            "Content-Type" "text/html"
                        }
                        :cookies {"user_id" {:value auth :path "/sql/" :max-age 36000}}
                        :body "
<!doctype html>
<html>
<head>
<meta http-equiv='refresh' content='1;url=/sql/'>
</head>
</html>
"
                    }
                    {
                        :status 401
                    }
                )
            )

            (GET "/sql/meta/" {:keys [cookies]}
                (get-meta)
            )

            (POST "/sql/queries/" {:keys [params cookies]}
                (submit-query params cookies)
            )
            (GET "/sql/queries/" {:keys [cookies]}
                (list-queries cookies)
            )
            (GET "/sql/queries/:qid/" [qid]
                (get-result qid)
            )
            (HEAD "/sql/queries/:qid/csv" [qid]
                (sniff (Long/parseLong qid))
            )
            (GET "/sql/queries/:qid/csv" [qid]
                (download (Long/parseLong qid))
            )


            (POST "/sql/saved/" {:keys [cookies params]}
                (add-query params cookies)
            )
            (GET "/sql/saved/" {:keys [cookies]}
                (get-saved-queries)
            )
            (DELETE "/sql/saved/:qid/" {:keys [cookies params]}
                (delete-saved-query cookies params)
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
