(ns puretest.argparser.core
    (:use testing.core)
    (:require
        [argparser.core :as arg]
    )
)

(suite "opt:"
    (:fact opt-show-single
        (:desc
            (arg/opt :foo
                "-f|--foo N" "sth about N on foo"
            )
        )
        :is
        ["-f|--foo N" ["sth about N on foo"]]
    )
    (:fact opt-parse-single
        (
            (:parse
                (arg/opt :foo
                    "-f|--foo N" "sth about N on foo"
                )
            )
            ["-f" "bar"]
        )
        :is
        [{:foo ["bar"]} []]
    )
    (:fact opt-parse-single-alternative
        (
            (:parse
                (arg/opt :foo
                    "-f|--foo N" "sth about N on foo"
                )
            )
            ["--foo" "bar"]
        )
        :is
        [{:foo ["bar"]} []]
    )
    (:fact opt-parse-single-remain
        (
            (:parse
                (arg/opt :foo
                    "-f|--foo N" "sth about N on foo"
                )
            )
            ["-f" "bar" "haha" "hehe"]
        )
        :is
        [{:foo ["bar"]} ["haha" "hehe"]]
    )
    (:fact opt-parse-single-noarg
        (fn []
            (
                (:parse
                    (arg/opt :foo
                        "-f|--foo N" "sth about N on foo"
                    )
                )
                ["-f"]
            )
        )
        :throws
        IllegalArgumentException
    )
    (:fact opt-parse-single-notapply
        (
            (:parse
                (arg/opt :foo
                    "-f|--foo N" "sth about N on foo"
                )
            )
            ["-a"]
        )
        :is
        [nil ["-a"]]
    )

    (:fact opt-parse-two
        (
            (:parse
                (arg/opt :foo
                    "-f N M" "sth about both N and M on foo"
                )
            )
            ["-f" "hello" "world"]
        )
        :is
        [{:foo ["hello" "world"]} []]
    )

    (:fact opt-parse-else-single
        (
            (:parse
                (arg/opt :else
                    "N" "a single arg N"
                )
            )
            ["hello" "world"]
        )
        :is
        [{:else ["hello"]} ["world"]]
    )
    (:fact opt-parse-else-plus
        (
            (:parse
                (arg/opt :else
                    "N+" "at least one arg N"
                )
            )
            ["hello" "world"]
        )
        :is
        [{:else ["hello"]} ["world"]]
    )
    (:fact opt-parse-else-star
        (
            (:parse
                (arg/opt :else
                    "N*" "any number of N"
                )
            )
            ["hello" "world"]
        )
        :is
        [{:else ["hello"]} ["world"]]
    )
)

(suite "arg parser: normal process"
       (:fact argparser-1
           (arg/parse {
               :usage "[options] executable"
               :args [
                   (arg/opt :cases
                         "--cases <pattern>" "a regexp which matches all cases to run. default is .*")
                   (arg/opt :dir
                         "-d|--dir <dirname>" "a directory to put results. default is res")
                    ]
               }
               ["--cases" ".*utility.*" "-d" "work"]
            )
            :is
            [{:cases [".*utility.*"]} {:dir ["work"]}]
       )
       (:fact argparser-illegal-arg
           (fn []
               (arg/parse {
                   :args [
                       (arg/opt :cases
                             "--cases <pattern>" "a regexp which matches all cases to run. default is .*")
                        ]
                   }
                   ["--cases" ".*utility.*" "-d" "work"]
                )
            )
            :throws
            IllegalArgumentException
       )
)

(suite "argparser: default transformer"
       (:fact transformer->map
           (arg/transform->map
             [{:dir ["a"]} {:foo ["foo1" "foo2"]} {:dir ["b"]} {:foo ["foo3" "foo4"]}]
           )
           :is
           {:foo ["foo1" "foo2" "foo3" "foo4"], :dir ["a" "b"]}
       )
)

(def spec-grep {
  :usage "Usage: grep [OPTION]... PATTERN [FILE]..."
  :synopsys [
         "Search for PATTERN in each FILE or standard input."
         "Example: grep -i 'hello world' menu.h main.c"
  ]
  :args [
;Regexp selection and interpretation:
  (arg/opt :regexp
  "-e, --regexp=PATTERN"      "use PATTERN for matching")
  (arg/opt :file
  "-f, --file=FILE"           "obtain PATTERN from FILE")
  (arg/opt :ignore-case
  "-i, --ignore-case"         "ignore case distinctions")

;Miscellaneous:
  (arg/opt :version
  "'-V, --version"             "print version information and exit")
  (arg/opt :help
      "--help"                "display this help and exit")

;Output control:
  (arg/opt :quiet
  "-q, --quiet, --silent"     "suppress all normal output")
  (arg/opt :binary-files
      "--binary-files=TYPE"   "assume that binary files are TYPE;"
                              "TYPE is `binary', `text', or `without-match'")

  (arg/opt :else
  "PATTERN"                   "pattern to be searched")
  ]
  :summary [
      "`egrep' means `grep -E'.  `fgrep' means `grep -F'."
      ""
      "General help using GNU software: <http://www.gnu.org/gethelp/>"
  ]
  }
)

(suite "argparser: doc generator"
       (:fact default-doc
           (arg/default-doc spec-grep)
           :is
"Usage: grep [OPTION]... PATTERN [FILE]...

Search for PATTERN in each FILE or standard input.
Example: grep -i 'hello world' menu.h main.c

Options:
 -e, --regexp=PATTERN   use PATTERN for matching
 -f, --file=FILE        obtain PATTERN from FILE
 -i, --ignore-case      ignore case distinctions
 '-V, --version         print version information and exit
 --help                 display this help and exit
 -q, --quiet, --silent  suppress all normal output
 --binary-files=TYPE    assume that binary files are TYPE;
                        TYPE is `binary', `text', or `without-match'
 PATTERN                pattern to be searched

`egrep' means `grep -E'.  `fgrep' means `grep -F'.

General help using GNU software: <http://www.gnu.org/gethelp/>"
       )
)
