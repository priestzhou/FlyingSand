(ns master.git
    (:require
        [clojure.string :as str]
        [utilities.core :as util]
        [utilities.shutil :as sh]
    )
    (:import
        [java.io IOException]
        [java.net URI]
        [java.nio.file Path]
    )
)

(defn- git [repo cmd]
{
    :pre [
        (instance? Path repo)
        (sequential? cmd)
    ]
}
    (let [
        cmd (vec (cons "git" cmd))
        r (sh/execute cmd :dir repo :out :pipe :out :pipe)
        ]
        (util/throw-if-not (= (:exitcode r) 0)
            IOException. (format "fail to git %s: %s" (pr-str cmd) (:out r))
        )
        r
    )
)

(defn init [repo & {:keys [bare]}]
    (let [
        repo (sh/getPath repo)
        rt (.getParent repo)
        nm (str (.getFileName repo))
        cmd ["init" "-q" nm]
        cmd (if-not bare cmd (conj cmd "--bare"))
        ]
        (git rt cmd)
        (git repo ["config" "user.email" "tyf@flying-sand.com"])
        (git repo ["config" "user.name" "taoda"])
    )
)

(defn commit [repo & {:keys [msg]}]
    (let [
        repo (sh/getPath repo)
        r (git repo ["status" "-s"])
        fs (str/trimr (:out r))
        ]
        (when-not (empty? fs)
            (let [
                fs (for [x (str/split-lines fs)]
                    (-> x
                        (str/trim)
                        (str/split #" " 2)
                        (second)
                    )
                )
                ]
                (doseq [f fs]
                    (git repo ["add" f])
                )
            )
            (git repo ["commit" "-q" "-m" msg])
        )
    )
)

(defn clone [repo & {:keys [src bare]}]
{
    :pre [(instance? URI src)]
}
    (let [
        repo (sh/getPath repo)
        rt (.getParent repo)
        nm (str (.getFileName repo))
        cmd (if bare
            ["clone" "-q" "--bare" (str src) nm]
            ["clone" "-q" (str src) nm]
        )
        ]
        (git rt cmd)
    )
)

(defn branch [repo & {:keys [branch]}]
    (let [repo (sh/getPath repo)]
        (git repo ["checkout" "-q" "-b" branch])
    )
)

(defn checkout [repo & {:keys [branch]}]
    (let [repo (sh/getPath repo)]
        (git repo ["checkout" "-q" branch])
    )
)

(defn show-commit [repo & {:keys [branch]}]
{
    :pre [(not (nil? branch))]
}
    (let [
        repo (sh/getPath repo)
        r (git repo ["show" "--format=format:%H" branch])
        ]
        (->> (:out r)
            (str/split-lines)
            (first)
        )
    )
)
