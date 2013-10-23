(ns unittest.utilities.aes
    (:use
        [testing.core :only (suite)]
    )
    (:require
        [utilities.aes :as aes]
    )
    (:import
        [java.util Random]
    )
)

(defn- random-text' [sb rng alphabet]
    (let [
        l (.length alphabet)
        i (.nextInt rng l)
        c (.charAt alphabet i)
        ]
        (when (not= c \space)
            (.append sb c)
            (recur sb rng alphabet)
        )
    )
)

(defn- random-text! [rng]
    (let [
        sb (StringBuilder.)
        alphabet "abcdefg " ; \space is the terminating character
        ]
        (random-text' sb rng alphabet)
        (str sb)
    )
)

(defn- tb [test]
    (time
    (let [rng (Random.)]
        (doseq [
            _ (range 10000)
            :let [text (random-text! rng)]
            :let [key (random-text! rng)]
            ]
            (test text key)
        )
    )
    )
)

(suite "aes"
    (:testbench tb)
    (:fact encrypt:decrypt:plain
        (fn [plain key]
            (-> plain
                (aes/encrypt key)
                (aes/decrypt key)
            )
        )
        :eq
        (fn [plain key] plain)
    )
)
