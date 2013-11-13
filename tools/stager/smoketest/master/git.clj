(ns smoketest.master.git
    (:use
        [testing.core :only (suite)]
    )
    (:require
        [utilities.shutil :as sh]
        [master.git :as git]
    )
    (:import
        [java.nio.file Path]
    )
)

(suite "git"
    (:fact init:add:clone
        (let [
            rt (sh/tempdir)
            repo0 (sh/getPath rt "smile")
            repo1 (sh/getPath rt "cloned")
            ]
            (git/init repo0)
            (sh/spitFile (sh/getPath repo0 "smile.txt") "hehe")
            (git/commit repo0 :msg "hehe")
            (git/clone repo1 :src (.toUri repo0))
            (sh/slurpFile (sh/getPath repo1 "smile.txt"))
        )
        :is
        "hehe"
    )
    (:fact branch
        (let [
            rt (sh/tempdir)
            repo (sh/getPath rt "smile")
            ]
            (git/init repo)
            (sh/spitFile (sh/getPath repo "smile.txt") "hehe")
            (git/commit repo :msg "hehe")
            (git/branch repo :branch "sad")
            (sh/spitFile (sh/getPath repo "smile.txt") "cry")
            (git/commit repo :msg "cry")
            (git/checkout repo :branch "master")
            (sh/slurpFile (sh/getPath repo "smile.txt"))
        )
        :is
        "hehe"
    )
    (:fact show-commit
        (let [
            rt (sh/tempdir)
            repo (sh/getPath rt "smile")
            ]
            (git/init repo)
            (sh/spitFile (sh/getPath repo "smile.txt") "hehe")
            (git/commit repo :msg "hehe")
            (->> (git/show-commit repo :branch "master")
                (re-matches #"[0-9a-f]{40}")
                (nil?)
            )
        )
        :is
        false
    )
    (:fact init:barely
        (let [
            rt (sh/tempdir)
            repo (sh/getPath rt "smile")
            ]
            (git/init repo :bare true)
            (-> (sh/getPath repo ".git")
                (.toFile)
                (.exists)
            )
        )
        :is
        false
    )
    (:fact clone:barely
        (let [
            rt (sh/tempdir)
            repo0 (sh/getPath rt "smile")
            repo1 (sh/getPath rt "cloned")
            ]
            (git/init repo0)
            (git/clone repo1 :src (.toUri repo0) :bare true)
            (-> (sh/getPath repo1 ".git")
                (.toFile)
                (.exists)
            )
        )
        :is
        false
    )
)

