(ns utilities.shutil
    (:import 
        [java.nio.file Files Path LinkOption
            FileSystems SimpleFileVisitor FileVisitResult
        ]
        java.nio.file.attribute.FileAttribute
        [java.io StringWriter File InputStream FileInputStream]
    )
    (:require
        [clojure.java.io :as io]
    )
)

(defn sep []
    (->
        (FileSystems/getDefault)
        (.getSeparator)
    )
)

(defn getPath ^Path [base & segs]
    (if-not (instance? Path base)
        (->
            (FileSystems/getDefault)
            (.getPath base (into-array String segs))
        )
        (reduce #(.resolve %1 %2) base segs)
    )
)

(defn- gen-fs-visitor [paths] 
  (proxy [SimpleFileVisitor] []
    (visitFile [file _]
      (conj! paths file)
      (FileVisitResult/CONTINUE)
    )
    (postVisitDirectory [dir _]
      (conj! paths dir)
      (FileVisitResult/CONTINUE)
    )
  )
)

(defn postwalk [path]
    (let [path (getPath path)
            paths (transient [])
        ]
        (when (Files/exists path (into-array LinkOption [LinkOption/NOFOLLOW_LINKS]))
            (Files/walkFileTree path #{} Integer/MAX_VALUE (gen-fs-visitor paths))
        )
        (persistent! paths)
    )
)

(defn rmtree [path]
    (doseq [p (postwalk path)]
        (Files/delete p)
    )
)

(defn mkdir [path]
    (let [p (getPath path)]
        (Files/createDirectories p (into-array FileAttribute []))
    )
)

(defn spitFile [path content]
    (let [p (getPath path)]
        (assert (.getParent p))
        (mkdir (.getParent p))
        (with-open [w (io/writer (.toFile p))]
            (.write w content)
        )
    )
)

(defn tempdir 
    ([prefix]
        (let [p (Files/createTempDirectory prefix (into-array FileAttribute []))]
            (mkdir p)
            p
        )
    )
    ([]
        (tempdir nil)
    )
)

(defn open-file-in-classpath ^InputStream [p]
    (-> (.getClass System)
        (.getResourceAsStream p)
    )
)

(defn open-file-in-fs ^InputStream [p]
    (-> p
        (getPath)
        (.toFile)
        (FileInputStream.)
    )
)

(defn open-file ^InputStream [p]
    (if (and (instance? String p) (= (first p) \@))
        (open-file-in-classpath (.substring p 1))
        (open-file-in-fs p)
    )
)


(deftype CloseableProcess [^Process prc]
    java.lang.AutoCloseable
    (close [this]
        (.destroy prc)
    )
)

(defn newCloseableProcess [p]
    (CloseableProcess. p)
)

(defn popen [cmd & opts]
    "Execute a program in subprocess.

    cmd - command line args to start the program
    opts - environments and redirections
    :in - possible values include :inherit, :pipe, a file or a string.
        :inherit, the default, will redirect stdin of subprocess from the 
            parent process.
        :pipe will open an OutputStream in the parent process, linking to stdin
            of the subprocess.
        a file, i.e., an instance of File or Path, will redirect the stdin of 
            subprocess from the file.
        a string, will push the string into stdin of the subprocess.
    :out - possible values include :inherit, :pipe or a file.
        :inherit, the default, will redirect stdout to that of the parent process.
        a file, i.e., an instance of File or Path, will overwrite the file by 
            contents from stdout.
        :pipe will put stdout of subprocess to an InputStream of the parent process. 
    :err - possible values include :inherit, :pipe, :out or a file.
        :inherit, the default, will redirect stderr to that of the parent process.
        :pipe will put stderr of subprocess to an InputStream of the parent process
        a file, i.e., an instance of File or Path, will overwrite the file by 
            contents from stderr.
        :out will redirect stderr to its stdout.
    This returns the subprocess.
    "
    (let [
            opts (merge {
                    :in :inherit 
                    :out :inherit
                    :err :inherit
                } 
                (apply hash-map opts)
            )
            pb (ProcessBuilder. (into-array String cmd))
        ]
        (let [in (:in opts)]
            (cond 
                (= :inherit in)
                    (.redirectInput pb java.lang.ProcessBuilder$Redirect/INHERIT)
                (= :pipe in)
                    (.redirectInput pb java.lang.ProcessBuilder$Redirect/PIPE)
                (string? in)
                    (.redirectInput pb java.lang.ProcessBuilder$Redirect/PIPE)
                (instance? File in)
                    (.redirectInput pb in)
                (instance? Path in)
                    (.redirectInput pb (.toFile in))
                :else (throw (IllegalArgumentException. 
                        (str "unknown argument for :in " in)
                    ))
            )
        )
        (let [out (:out opts)]
            (cond
                (= :inherit out)
                    (.redirectOutput pb java.lang.ProcessBuilder$Redirect/INHERIT)
                (= :pipe out)
                    (.redirectOutput pb java.lang.ProcessBuilder$Redirect/PIPE)
                (instance? File out)
                    (.redirectOutput pb out)
                (instance? Path out)
                    (.redirectOutput pb (.toFile out))
                :else (throw (IllegalArgumentException. 
                        (str "unknown argument for :out " out)
                    ))
            )
        )
        (let [err (:err opts)]
            (cond
                (= :inherit err)
                    (.redirectError pb java.lang.ProcessBuilder$Redirect/INHERIT)
                (= :pipe err)
                    (.redirectError pb java.lang.ProcessBuilder$Redirect/PIPE)
                (= :out err)
                    (.redirectErrorStream pb true)
                (instance? File err)
                    (.redirectError pb err)
                (instance? Path err)
                    (.redirectError pb (.toFile err))
                :else (throw (IllegalArgumentException. 
                        (str "unknown argument for :err " err)
                    ))
            )
        )
        (let [p (.start pb)]
            (if (string? (:in opts))
                (with-open [wtr (io/writer (.getOutputStream p))]
                    (.write wtr (:in opts))
                )
            )
            p
        )
    )
)

(defn execute [cmd & opts]
    "Execute a program in subprocess.

    cmd - command line args to start the program
    opts - environments and redirections
    :in - possible values include :inherit, a file or a string.
        :inherit, the default, will redirect stdin of subprocess from the 
            parent process.
        a file, i.e., an instance of File or Path, will redirect the stdin of 
            subproces from the file.
        a string, will push the string into stdin of the subprocess.
    :out - possible values include :inherit, :pipe or a file.
        :inherit, the default, will redirect stdout to that of the parent process.
        a file, i.e., an instance of File or Path, will overwrite the file by 
            contents from stdout.
        :pipe will results a string in return, keyed by :out. Use it carefully,
            for it will possibly cause subprocess hung.
    :err - possible values include :inherit, :pipe, :out or a file.
        :inherit, the default, will redirect stderr to that of the parent process.
        :pipe will results a string in return, keyed by :err. Use it carefully,
            for it will possibly cause subprocess hung.
        a file, i.e., an instance of File or Path, will overwrite the file by 
            contents from stderr.
        :out will redirect stderr to its stdout.
    This returns a map which contains :exitcode, and optionally :out and :err.
    "
    (let [
            p (apply popen cmd opts)
            opts (apply hash-map opts)
        ]
        (->
            {:exitcode (.waitFor p)}
            (merge 
                (if (= :pipe (:out opts)) 
                    {:out (slurp (.getInputStream p))} 
                    {}
                )
            )
            (merge 
                (if (= :pipe (:err opts)) 
                    {:err (slurp (.getErrorStream p))} 
                    {}
                )
            )
        )
    )
)

