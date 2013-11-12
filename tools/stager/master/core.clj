(ns master.core
    (:use
        [slingshot.slingshot :only (try+ throw+)]
    )
)

(defmacro throw+if [pred except]
    `(when ~pred
        (throw+ ~except)
    )
)

(defmacro throw+if-not [pred except]
    `(throw-if (not ~pred) ~except)
)
