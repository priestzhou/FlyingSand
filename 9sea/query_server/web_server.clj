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

(defn log-in [email psw]
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
    (if cookies
        (when-let [user-id (cookies "user_id")]
            (:value user-id)
        )
    )
)

(defn authenticate [cookies]
    (if-let [user-id (extract-user-id cookies)]
        (let [
            res (db/select users
                (db/fields :user_id)
                (db/where {:user_id user-id})
            )
            ]
            (if (empty? res)
                nil
                (-> res (first) (:user_id))
            )
        )
    )
)

; query

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
            (let [now (to-long (time/now))]
                (dosync
                    (alter results
                        update-in [qid]
                        assoc 
                            :result result 
                            :status "succeeded" 
                            :progress [total-stages total-stages]
                            :url (format "queries/%d/csv" qid)
                            :duration (- now (:submit-time (@results qid)))
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
        {:keys [app version db query]} params
        qid (rand-int 10000)
        now (to-long (time/now))
        ]
        (println "POST queries/ " (pr-str {:user_id user-id :app app :version version :db db :query query}))
        (dosync
            (alter results assoc qid {
                :status "running" 
                :query query
                :log "" 
                :submit-time now
            })
        )
        (do-query qid query)
    )
)

(defn get-result [qid]
    (let [
        qid (Long/parseLong qid)
        _ (println (format "GET queries/%d/" qid))
        result (dosync
            (let [r (@results qid)]
                (alter results update-in [qid] assoc :log "")
                r
            )
        )
        ]
        {
            :status 200
            :headers {"Content-Type" "application/json"}
            :body (json/write-str result)
        }
    )
)

(defn get-meta [cookies]
    (let [user-id (extract-user-id cookies)]
        (println "GET meta" (pr-str {:user_id user-id}))
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
)

(def saved-queries (ref {}))

(defn get-saved-queries [cookies]
    (let [user-id (extract-user-id cookies)]
        (println (format "GET saved/ %s" (pr-str {:user_id user-id})))
        {
            :status 200
            :headers {"Content-Type" "application/json"}
            :body (json/write-str @saved-queries)
        }
    )
)

(defn add-query [params cookies]
    (let [
        user-id (extract-user-id cookies)
        {:keys [name app version db query]} params
        qname name
        _ (println (format "POST saved/?name=%s&app=%s&version=%s&db=%s&query=%s %s" app version db qname query (pr-str {:user_id user-id})))
        r (dosync
            (let [qid (for [
                    [id {:keys [name]}] @saved-queries
                    :when (= name qname)
                    ] 
                    id
                )
                ]
                (if (empty? qid)
                    (let [new-id (rand-int 1000)]
                        (alter saved-queries assoc new-id {
                            :name qname 
                            :app app
                            :version version
                            :db db
                            :query query
                        })
                        new-id
                    )
                    nil
                )
            )
        )
        ]
        (if r
            {
                :status 201
                :headers {"Content-Type" "text/plain"}
                :body (format "%d" r)
            }
            {
                :status 400
                :headers {"Content-Type" "text/plain"}
                :body qname
            }
        )
    )
)

(defn delete-saved-query [cookies params]
    (let [
        user-id (extract-user-id cookies)
        id (->> params (:qid) (Long/parseLong))
        _ (println (format "DELETE saved/%d/ %s" id (pr-str {:user_id user-id})))
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
    (println (format "GET queries/%d/csv" qid))
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
    (println (format "HEAD queries/%d/csv" qid))
    (if-let [r (@csv qid)]
        {
            :status 200
            :headers {"Content-Type" "application/json"}
            :body "{}"
        }
        {
            :status 404
            :headers {"Content-Type" "application/json"}
            :body "{}"
        }
    )
)

(defn list-queries [cookies]
    (let [
        user-id (extract-user-id cookies)
        _ (println "GET queries/" (pr-str {:user_id user-id}))
        r (dosync
            (let [ks (keys @results)]
                (into {}
                    (for [
                        k ks
                        :let [v (@results k)]
                        :let [{:keys [query status url submit-time duration]} v]
                        ]
                        [k (merge 
                                {:query query :status status :submit-time submit-time}
                                (if duration {:duration duration} {})
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

; collector adminstration

(def collectors (ref {
    1 {
        :name "xixi"
        :url "http://1.1.1.1:1111/xixi"
        :status "running"
        :recent-sync (to-long (time/now))
        :synced-data 12345
    }
    2 {
        :name "hehe"
        :url "http://2.2.2.2:2222/hehe"
        :status "stopped"
        :recent-sync (to-long (time/now))
        :synced-data 54321
    }
    3 {
        :name "haha"
        :url "http://3.3.3.3:3333/haha"
        :status "no-sync"
    }
}))

(defn add-collector [params cookies]
    (println "POST /sql/collectors" (assoc params :user_id (extract-user-id cookies)))
    (if-not (authenticate cookies)
        {:status 401}
        (let [
            name (:name params)
            url (:url params)
            cid (rand-int 1000)
            r (dosync
                (if-not (empty? (for [
                            [_ v] @collectors
                            :let [x (:name v)]
                            :when (= x name)
                        ]
                        x
                    ))
                    {
                        :status 409
                        :headers {"Content-Type" "text/plain"}
                        :body "duplicated name"
                    }
                    (let [
                        duplicated-urls (for [
                            [_ v] @collectors
                            :let [x (:url v)]
                            :when (= x url)
                            ]
                            x
                        )
                        ]
                        (if-not (empty? duplicated-urls)
                            {
                                :status 409
                                :headers {"Content-Type" "text/plain"}
                                :body "duplicated url"
                            }
                            (do
                                (alter collectors
                                    assoc cid
                                    {
                                        :name name
                                        :url url
                                        :status "no-sync"
                                    }
                                )
                                {
                                    :status 201
                                    :headers {"Content-Type" "application/json"}
                                    :body (json/write-str {:id cid})
                                }
                            )
                        )
                    )
                )
            )
            ]
            r
        )
    )
)

(defn list-collectors [cookies]
    (println "GET /sql/collectors/" {:user_id (extract-user-id cookies)})
    (if-not (authenticate cookies)
        {:status 401}
        {
            :status 200
            :headers {"Content-Type" "application/json"}
            :body (json/write-str @collectors)
        }
    )
)

(defn delete-collector [params cookies]
    (println (format "DELETE /sql/collectors/%s" (:cid params)) {:user_id (extract-user-id cookies)})
    (if-not (authenticate cookies)
        {:status 401 :headers {"Content-Type" "text/plain"} :body ""}
        (let [cid (Long/parseLong (:cid params))]
            (dosync
                (if (contains? @collectors cid)
                    (do
                        (alter collectors dissoc cid)
                        {:status 200 :headers {"Content-Type" "text/plain"} :body ""}
                    )
                    {:status 404 :headers {"Content-Type" "text/plain"} :body ""}
                )
            )
        )
    )
)

(defn app [opts]
    (handler/site
        (defroutes app-routes
            (GET "/sql" {}
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
                (if-let [auth (log-in (:email params) (:password params))]
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

            (GET "/sql/meta" {:keys [cookies]}
                (get-meta cookies)
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
                (get-saved-queries cookies)
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

            ; the following is for collector adminitration page

            (POST "/sql/collectors/" {:keys [params cookies]}
                (add-collector params cookies)
            )
            (GET "/sql/collectors/" {:keys [cookies]}
                (list-collectors cookies)
            )
            (DELETE "/sql/collectors/:cid" {:keys [params cookies]}
                (delete-collector params cookies)
            )

            (GET "/sql/admin.html" {:keys [cookies]}
                (println "GET /sql/admin.html" {:user_id (extract-user-id cookies)})
                (if (authenticate cookies)
                    (slurp (.toFile (sh/getPath (:dir opts) "admin.html")))
                    (slurp (.toFile (sh/getPath (:dir opts) "index.html")))
                )
            )

            ; the defaults
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
