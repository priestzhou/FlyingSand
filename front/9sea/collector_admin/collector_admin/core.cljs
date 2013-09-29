(ns collector-admin.core
    (:use
        [domina.css :only (sel)]
    )
    (:require
        [clojure.string :as str]
        [domina :as dom]
        [domina.events :as evt]
        [ajax.core :as ajax]
    )
)

(defn ^:export on-load []
    (-> "body"
        (sel)
        (dom/set-html! "<p>Hello World, again!</p>")
    )
)
