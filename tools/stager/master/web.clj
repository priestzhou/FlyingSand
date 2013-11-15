(ns master.web
    (:require
        [compojure.handler :as handler]
        [compojure.route :as route]
        [clojure.string :as str]
        [clojure.java.io :as io]
        [org.httpkit.server :as http]
        [clojure.data.json :as json]
        [utilities.core :as util]
        [master.app :as app]
    )
    (:use
        [compojure.core :only (defroutes GET PUT POST DELETE HEAD ANY)]
        [slingshot.slingshot :only (try+ throw+)]
        [logging.core :only [defloggers]]
    )
)

(defloggers debug info warn error)

(defn- ruok []
    {
        :status 200
        :headers {"Content-Type" "text/plain"}
        :body "imok"
    }
)

(defn- wrap-response [resp]
    (info "http response" :response resp)
    (let [r (cond
            (nil? (:headers resp)) (assoc resp
                :headers {"Content-Type" "application/json"}
                :body "null"
            )
            (not (contains? (:headers resp) "Content-Type")) (assoc resp
                :headers (assoc (:headers resp) "Content-Type" "application/json")
                :body "null"
            )
            :else resp
        )
        ]
        r
    )
)

(defn- handle [h]
    (try+
        (wrap-response (h))
    (catch map? ex
        (wrap-response ex)
    )
    (catch Exception ex
        (error "exception!" :error (util/except->str ex))
        (wrap-response {:status 500})
    )
    (catch Error ex
        (error "error!" :error (util/except->str ex))
        (System/exit 1)
    ))
)

(defn- app [opts]
    (handler/api
        (defroutes app-routes
            (GET "/ruok" [] (ruok))

            (GET "/slaves/" [] (handle app/get-slaves))
            (POST "/slaves/" {:keys [params]}
                (handle (partial app/add-slave params))
            )

            (GET "/remote" {:keys [params]}
                (handle (partial app/fetch-remote opts params))
            )

            (GET "/repository/:ver/*" {:keys [params]}
                (handle (partial app/fetch-ver opts params))
            )

            (GET "/apps/" []
                (handle app/get-app)
            )
            (PUT "/apps/:app" {:keys [params]}
                (handle (partial app/put-app opts params))
            )

            (route/files "/" {:root (:resource-root opts) :allow-symlinks? true})
            (route/not-found "Not Found")
        )
    )
)

(defn start-server [opts]
    (http/run-server (app opts) {:port (:port opts)})
)
