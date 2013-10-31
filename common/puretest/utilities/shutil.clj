(ns puretest.utilities.shutil
    (:use testing.core
        utilities.shutil
    )
    (:import
        java.nio.file.Path
        java.io.StringReader
    )
)

(suite "getPath"
    (:fact getPath-1 (str (getPath "/")) :is "/")
    (:fact getPath-2 (str (getPath "/" "tmp")) :is "/tmp")
    (:fact getPath-3 (str (getPath (getPath "/") "tmp")) :is "/tmp")
    (:fact getPath-4 (str (getPath (getPath "/") "tmp" "hehe")) :is "/tmp/hehe")
)
