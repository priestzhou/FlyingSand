(ns query-server.web-server
    (:require
        [compojure.handler :as handler]
        [compojure.route :as route]
        [korma.core :as db]
        [clojure.data.json :as json]
        [clojure.string :as str]
        [clojure.java.io :as io]
        [utilities.core :as util]
        [utilities.shutil :as sh]
        [clj-time.core :as time]
        [query-server.query-backend :as backend]
        [utilities.parse :as prs]
        [query-server.config :as config]
        [query-server.mysql-connector :as mysql]
        
    )
    (:use
        [compojure.core :only (defroutes GET PUT POST DELETE HEAD ANY)]
        [ring.adapter.jetty :only (run-jetty)]
        [korma.db]
        [korma.core]
        [logging.core :only [defloggers]]
        [clj-time.coerce]
        [clj-time.format]
        [slingshot.slingshot :only (try+ throw+)]
    )
    (:import
        [java.security MessageDigest]
        [java.nio.charset StandardCharsets]
        [java.nio.file Path]
        [java.sql SQLException]
        [utilities.parse InvalidSyntaxException]
    )
)

(defloggers debug info warn error)

(defn sha1 [^String str]
    (->> str
        (util/str->bytes)
        (.digest (MessageDigest/getInstance "sha1"))
    )
)

(defn log-in [email psw]
          (prn "password" (-> psw (sha1) (util/hexdigits)))
    (let [res (db/select mysql/TblUsers
            (db/fields :UserId)
            (db/where {:Email email :Password (-> psw (sha1) (util/hexdigits))})
        )
        ]
        (if (empty? res)
            nil
            (-> res (first) (:UserId))
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
            res (db/select mysql/TblUsers
                (db/fields :UserId)
                (db/where {:UserId user-id})
            )
            ]
            (if (empty? res)
                nil
                (-> res (first) (:UserId))
            )
        )
    )
)

; query

(def results (ref {}))
(def csv (ref {}))
(def meta-tree (ref {}))

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

(defn do-hql-query [qid query]
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

(defn get-account-id
    [user-id]
    (let [res (db/select mysql/TblUsers
            (db/fields :AccountId)
            (db/where {:UserId user-id})
    )]
        (if (empty? res)
            nil
            (-> res (first) (:AccountId))
        )
    )
)

(defn gen-context
  [account-id app version db]
  (prn "app:" app "version:" version "db:" db)

  (let [meta-info (backend/get-metastore-tree account-id)]
    
    {:ns meta-info :default-ns [app version]}
  )
)

(defn submit-query [params cookies]
    (let [
        user-id (extract-user-id cookies)
        {:keys [app version db query]} params
         ]

         ; find account by user id
         (let [account-id (get-account-id user-id)
               context (gen-context account-id app version db)
               ]
           (prn "query context" context)
               (let [qid (backend/submit-query context user-id query)]
                 (println (str "qid is:" qid))

                 {
                   :status 201
                   :headers {
                       "Content-Type" "application/json"
                   }
                  :body (json/write-str {:id qid})
                }

              )
            )
      )
)
      
(defn get-result [qid]
    (let [
        qid (Long/parseLong qid)
        _ (println (format "GET queries/%d/" qid))
        result (backend/get-query-result qid)
         ]
        (println result)
        (if (nil? result) 
          {
            :status 404
          }
          (do
            (if-let [csv-url (get result :url)]
              (do
                (prn "csv-url" csv-url)
              )
            )
            {
              :status 200
              :headers {"Content-Type" "application/json"}
              :body (json/write-str result)
            }
          )
        )
    )
)
 
(defn get-meta [cookies]
    (let [user-id (extract-user-id cookies)
          account-id (get-account-id user-id)
          _ (println "account-id: " account-id)
          meta-tree (backend/get-metastore-tree account-id)
          ]
      (if (nil? meta-tree) (println "get meta error!")
        (do
         (println "GET meta" (pr-str {:UserId user-id}))
         {
            :status 200
            :headers {"Content-Type" "application/json"}
            :body (json/write-str meta-tree) 
         }
        )
      )
    )
)

(def saved-queries (ref {}))

(defn get-saved-queries [cookies]
    (authenticate cookies)
    (let [user-id (extract-user-id cookies)
          res (backend/select-saved-queries user-id)
        ]
        (println (format "GET saved/ %s" (pr-str {:UserId user-id})))
        {
            :status 200
            :headers {"Content-Type" "application/json"}
            :body (json/write-str res)
        }
    )
)

(defn add-query [params cookies]
    (let [
        user-id (extract-user-id cookies)
        {:keys [name app version db query]} params
        qname name
        _ (println (format "POST saved/?name=%s&app=%s&version=%s&db=%s&query=%s %s" app version db qname query (pr-str {:UserId user-id})))
        r-qid (backend/check-query-name qname)
        s-time (unparse (with-zone (formatters :date-hour-minute-second)
                          (time/time-zone-for-offset +8))(from-long (System/currentTimeMillis)))
        ]
        (prn "qname:" qname)
        (prn "r-qid:" r-qid)
        (authenticate cookies)
        (if (not (nil? r-qid)) 
            {
                :status 409
                :headers {"Content-Type" "text/plain"}
                :body (format "%d" r-qid)
            }
            (do
                (try
                  (prn "date-time: " s-time)
                  (backend/save-query qname app version db query s-time user-id)
                  {
                    :status 201
                    :headers {"Content-Type" "text/plain"}
                  }

                (catch SQLException ex
                    {
                        :status 400
                        :headers {"Content-Type" "text/plain"}
                        :body qname
                    }

                ))


            )
        )

    )
)

(defn delete-saved-query [cookies params]
    (let [
        user-id (extract-user-id cookies)
        id (->> params (:qid) (Long/parseLong))
        _ (println (format "DELETE saved/%d/ %s" id (pr-str {:UserId user-id})))
        ]
        (authenticate cookies)
        (try
            (let [ownership (backend/check-query-ownership id user-id)
                ]
                (if (nil? ownership)
                {
                    :status 404
                }
                (do
                  (backend/delete-saved-query id)
                  {
                     :status 200
                  }

                )
                )

            )
        (catch SQLException ex
            {
                :status 500

            }
        )
        )
    )
)

(defn download [qid]
    (println (format "GET queries/%d/csv" qid))
    (if-let [r (@csv qid)]
      (do
        (prn "download csv: " r)
        {
            :status 200
            :headers {"Content-Type" "text/csv"}
            :body r
        }
      )
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
        _ (println "GET queries/" (pr-str {:UserId user-id}))
        res (backend/select-history-queries user-id)
        ]
        {
            :status 200
            :headers {"Content-Type" "application/json"}
            :body (json/write-str res)
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

(defn check-collector [msg pred]
    (let [cids (for [
            [cid v] @collectors
            :when (pred cid v)
            ]
            cid
        )
        ]
        (when-not (empty? cids)
            (throw+
                {
                    :status 409 
                    :headers {"Content-Type" "application/json"} 
                    :body (json/write-str {
                        :error msg
                        :collector (first cids)
                    })
                }
            )
        )
    )
)

(defn check-duplicated-name?
    ([cid name]
        (check-collector "duplicated name"
            (fn [c v]
                (and (not= cid c) (= name (:name v)))
            )
        )
    )

    ([name]
        (check-collector "duplicated name"
            (fn [c v]
                (= name (:name v))
            )
        )
    )
)

(defn check-duplicated-url?
    ([cid url]
        (check-collector "duplicated url"
            (fn [c v]
                (and (not= cid c) (= url (:url v)))
            )
        )
    )

    ([url]
        (check-collector "duplicated url"
            (fn [c v]
                (= url (:url v))
            )
        )
    )
)

(defn is-collector-no-sync? [cid]
    (if-let [v (@collectors cid)]
        (when-not (= (:status v) "no-sync")
            (throw+
                {
                    :status 403 
                    :headers {"Content-Type" "application/json"} 
                    :body "null"
                }
            )
        )
    )
)

(defn does-collector-exist? [cid]
    (when-not (contains? @collectors cid)
        (throw+
            {
                :status 404 
                :headers {"Content-Type" "application/json"} 
                :body "null"
            }
        )
    )
)

(defn authenticate* [cookies]
    (if-let [user-id (extract-user-id cookies)]
        (let [
            res (db/select mysql/TblUsers
                (db/fields :UserId)
                (db/where {:UserId user-id})
            )
            ]
            (if (empty? res)
                (throw+
                    {
                        :status 401 
                        :headers {"Content-Type" "application/json"} 
                        :body "null"
                    }
                )
            )
        )
    )
)

(defn add-collector [params cookies]
    (println "POST /sql/collectors" params cookies)
    (try+
        (authenticate* cookies)
        (let [
            name (:name params)
            url (:url params)
            cid (rand-int 1000)
            ]
            (dosync
                (check-duplicated-name? name)
                (check-duplicated-url? url)

                (let [r {:status "no-sync" :name name :url url}]
                    (alter collectors assoc cid r)
                    {
                        :status 201
                        :headers {"Content-Type" "application/json"}
                        :body (json/write-str (assoc r :id cid))
                    }
                )
            )
        )
    (catch map? ex
        ex
    ))
)

(defn list-collectors [cookies]
    (println "GET /sql/collectors/" cookies)
    (try+
        (authenticate* cookies)

        (let [r (dosync
                (for [
                    [cid v] @collectors
                    ]
                    (assoc v :id cid)
                )
            )
            ]
            {
                :status 200
                :headers {"Content-Type" "application/json"}
                :body (json/write-str r)
            }
        )
    (catch map? ex
        ex
    ))
)

(defn delete-collector [params cookies]
    (try+
        (let [cid (-> params (:cid) (Long/parseLong))]
            (println (format "DELETE /sql/collectors/%d" cid) cookies)
            (authenticate* cookies)
            (dosync
                (does-collector-exist? cid)

                (alter collectors dissoc cid)
                {
                    :status 200 
                    :headers {"Content-Type" "application/json"} 
                    :body (json/write-str {})
                }
            )
        )
    (catch map? ex
        ex
    ))
)

(defn edit-collector [params cookies body]
    (try+
        (authenticate* cookies)
        (let [
            cid (Long/parseLong (:cid params))
            body (-> body 
                (io/reader :encoding "UTF-8") 
                (json/read :key-fn keyword)
            )
            name (:name body)
            url (:url body)
            ]
            (println (format "PUT /sql/collectors/%d" cid) cookies body)
            (dosync
                (does-collector-exist? cid)
                (is-collector-no-sync? cid)
                (check-duplicated-name? cid name)
                (check-duplicated-url? cid url)

                (alter collectors update-in [cid] assoc :name name :url url)
                {
                    :status 200 
                    :headers {"Content-Type" "application/json"} 
                    :body (json/write-str {})
                }
            )
        )
    (catch map? ex
        ex
    ))
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
                        :cookies {"user_id" {:value auth :path "/sql/"}}
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
            (PUT "/sql/collectors/:cid" {:keys [params cookies body]}
                (edit-collector params cookies body)
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
