(ns mailtool.core
    (:require
        [postal.core :as pc]
    )
)
(defn send-html-mail [to subject htmlbody]
    (pc/send-message 
        ^{:host "smtp.exmail.qq.com"
            :user "admin@flying-sand.com"
            :pass "fs123456"
        }
        {:from "admin@flying-sand.com"
        :to to
        :subject subject
        :body 
            [:alternative
                {:type "text/html"
                    :content htmlbody
                }
            ]
        }

    )
)