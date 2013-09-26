(ns query-server.web-server
    (:require
        [compojure.handler :as handler]
        [compojure.route :as route]
        [korma.core :as db]
        [clojure.data.json :as json]
        [utilities.core :as util]
        [utilities.shutil :as sh]
    )
    (:use
        [compojure.core :only (defroutes GET PUT POST ANY)]
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

(defn progress [qid result total-stages current-stage]
    (prn current-stage total-stages)
    (if (< current-stage total-stages)
        (do
            (dosync
                (alter results 
                    update-in [qid] 
                    assoc :progress [current-stage total-stages]
                )
            )
            (Thread/sleep 1000)
            (recur qid result total-stages (inc current-stage))
        )
        (do
            (let []
                (dosync
                    (alter results
                        update-in [qid]
                        assoc :result result :status "done" :progress [total-stages total-stages]
                    )
                )
            )
        )
    )
)

(defn do-query [qid query]
    (let [progress-stages (inc (rand-int 10))]
        (try
            (let [result (db/exec-raw [query] :results)]
                (future (progress qid result progress-stages 0))
                {
                    :status 201
                    :headers {
                        "content-type" "application/json"
                    }
                    :body (json/write-str {:id qid})
                }
            )
        (catch SQLException ex
            {
                :status 400
                :headers {
                    "content-type" "text/plain"
                }
                :body "invalid sql query"
            }
        ))
    )
)

(defn submit-query [params cookies]
    (let [
        user-id (extract-user-id cookies)
        app (:app params)
        ver (:version params)
        query (:query params)
        ]
        (cond
            (not= app "WoW") {
                :status 400 
                :headers {"content-type" "text/plain"} 
                :body "app must be WoW"
            }
            (not= ver "panda") {
                :status 400
                :headers {"content-type" "text/plain"}
                :body "version must be panda"
            }
            :else (let [
                qid (rand-int 10000)
                ]
                (dosync
                    (alter results assoc qid {:status "running"})
                )
                (do-query qid query)
            )
        )
    )
)

(defn get-result [params]
    (let [
        qid (Long/parseLong (:id params))
        result (@results qid)
        ; (dosync
        ;     (let [r (@results qid)]
        ;         (alter results update-in [qid] assoc :log "")
        ;         r
        ;     )
        ; )
        ]
        (prn qid result)
        {
            :status 200
            :headers {"content-type" "application/json"}
            :body (json/write-str result)
        }
    )
)

(defn app [opts]
    (handler/site
        (defroutes app-routes
            (POST "/sql" {params :params}
                (if-let [auth (authenticate (:email params) (:password params))]
                    {
                        :status 201
                        :headers {
                            "content-type" "text/html"
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
            (GET "/sql/GetResult" {:keys [params]}
                (get-result params)
            )
            (GET "/sql" {:keys [cookies]}
                (if (and cookies (get cookies "user_id"))
                    (slurp (.toFile (sh/getPath (:dir opts) "query.html")))
                    (slurp (.toFile (sh/getPath (:dir opts) "index.html")))
                )
            )
            (route/files "/" {:root (:dir opts) :allow-symlinks? true})
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
