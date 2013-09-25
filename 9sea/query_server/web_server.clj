(ns query-server.web-server
    (:require
        [compojure.handler :as handler]
        [compojure.route :as route]
        [korma.core :as db]
        [utilities.core :as util]
        [utilities.shutil :as sh]
    )
    (:use
        [compojure.core :only (defroutes GET PUT POST)]
        [ring.adapter.jetty :only (run-jetty)]
        [korma.db :only (defdb sqlite3)]
        [logging.core :only [defloggers]]
    )
    (:import
        [java.security MessageDigest]
        [java.nio.charset StandardCharsets]
        [java.nio.file Path]
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

(defn app [opts]
    (handler/site
        (defroutes app-routes
            (POST "/9sea" {params :params}
                (if-let [auth (authenticate (:email params) (:password params))]
                    {
                        :status 201
                        :headers {
                            "content-type" "text/html"
                        }
                        :cookies {"user_id" {:value auth :path "/9sea" :max-age 60}}
                        :body "
<!doctype html>
<html>
<head>
<meta http-equiv='refresh' content='1;url=/9sea'>
</head>
</html>
"
                    }
                    {
                        :status 401
                    }
                )
            )
            (GET "/9sea" {:keys [cookies]}
                (if (and cookies (get cookies "user_id"))
                    (format "Hello, %s" (-> cookies (get "user_id") (:value)))
                    (slurp (.toFile (sh/getPath (:dir opts) "index.html")))
                )
            )
            (route/files "/9sea" {:root (:dir opts) :allow-symlinks? true})
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
