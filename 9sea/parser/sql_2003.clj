(ns parser.sql-2003
    (:require
        [clojure.string :as str]
        [utilities.core :as util]
        [utilities.parse :as prs]
    )
)

(def char-map
    (let [
        small-a (int \a)
        small-z (int \z)
        big-a (int \A)
        big-z (int \Z)
        lowers (map char (range small-a (inc small-z)))
        uppers (map char (range big-a (inc big-z)))
        ]
        (into {}
            (concat
                (for [[l u] (util/zip lowers uppers)]
                    [l #{l u}]
                )
                (for [[l u] (util/zip lowers uppers)]
                    [u #{l u}]
                )
            )
        )
    )
)

(defn expect-char-ignore-case [ch]
    (if-let [cs (char-map ch)]
        (prs/expect-char-if cs)
        (prs/expect-char ch)
    )
)

(defn expect-string-ignore-case [s]
    (apply prs/chain (map expect-char-ignore-case s))
)

(defn sql-comment [stream]
    (let [
        [strm] (->> stream
            ((prs/choice
                (prs/between
                    (prs/expect-string "--")
                    (prs/choice
                        (prs/expect-char \newline)
                        (prs/expect-eof)
                    )
                    (prs/expect-any-char)
                )
                (prs/between
                    (prs/expect-string "/*")
                    (prs/expect-string "*/")
                    (prs/expect-any-char)
                )
            ))
        )
        ]
        [strm nil]
    )
)

(defn- blank []
    (prs/choice
        sql-comment
        (prs/expect-char-if prs/whitespace)
    )
)

(defn blank+ [stream]
    (let [
        [strm] (->> stream
            ((prs/many1 (blank)))
        )
        ]
        [strm nil]
    )
)

(defn blank* [stream]
    (let [
        [strm] (->> stream
            ((prs/many (blank)))
        )
        ]
        [strm nil]
    )
)

(defn- paren-parser [left-parser right-parser mid-parser stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                left-parser
                mid-parser
                right-parser
            ))
        )
        ]
        [strm (second prsd)]
    )
)

(defn paren
    ([left-parser right-parser mid-parser]
        (partial paren-parser left-parser right-parser mid-parser)
    )
    ([mid-parser]
        (paren
            (prs/chain
                (prs/expect-char \()
                blank*
            )
            (prs/chain
                blank*
                (prs/expect-char \))
            )
            mid-parser
        )
    )
)


(defn- unsigned-digits []
    (prs/many1 (prs/expect-char-if prs/digit))
)

(defn- leading-integer []
    (prs/chain
        (unsigned-digits)
        (prs/optional
            (prs/chain
                (prs/expect-char \.)
                (unsigned-digits)
            )
        )
    )
)

(defn- pure-fraction []
    (prs/chain
        (prs/expect-char \.)
        (unsigned-digits)
    )
)

(defn- decimal []
    (prs/choice (leading-integer) (pure-fraction))
)

(defn- scientific []
    (prs/chain
        (decimal)
        (expect-char-ignore-case \E)
        (prs/optional (prs/expect-char-if #{\+ \-}))
        (unsigned-digits)
    )
)

(defmacro defliteral [fnname type parser]
    `(defn ~fnname [stream#]
        (let [
            [strm#] (~parser stream#)
            ]
            [strm# {:type ~type, :value (prs/extract-string-between stream# strm#)}]
        )
    )
)

(defliteral unsigned-numeric-literal :numeric-literal
    (prs/choice
        (scientific)
        (decimal)
    )
)

(defliteral numeric-literal :numeric-literal
    (prs/chain
        (prs/optional (prs/expect-char-if #{\+ \-}))
        unsigned-numeric-literal
    )
)

(defn- segment [body]
    (prs/chain
        (prs/expect-char \')
        body
        (prs/expect-char \')
    )
)

(defn- string-literal [leading body]
    (prs/chain
        leading
        (prs/separated-list
            (segment body)
            blank+
        )
    )
)

(defn hex-string [stream]
    (let [
        [strm1] (->> stream
            ((string-literal
                (expect-char-ignore-case \X)
                (prs/many (prs/expect-char-if prs/hexdigit))
            ))
        )
        [strm2 escp] (->> strm1
            ((prs/optional
                (prs/chain
                    blank*
                    (expect-string-ignore-case "ESCAPE")
                )
            ))
        )
        ]
        (if escp
            (prs/gen-ISE strm2 "not support ESCAPE")
            [
                strm1
                {
                    :type :hex-string-literal,
                    :value (prs/extract-string-between stream strm1)
                }
            ]
        )
    )
)

(defn- second-fraction []
    (prs/chain
        (unsigned-digits)
        (prs/optional
            (prs/chain
                (prs/expect-char \.)
                (unsigned-digits)
            )
        )
    )
)

(defn- date-g []
    (prs/chain
        (unsigned-digits)
        (prs/expect-char \-)
        (unsigned-digits)
        (prs/expect-char \-)
        (unsigned-digits)
    )
)

(defn- time-g []
    (prs/chain
        (unsigned-digits)
        (prs/expect-char \:)
        (unsigned-digits)
        (prs/expect-char \:)
        (second-fraction)
        (prs/optional
            (prs/chain
                (prs/expect-char-if #{\+ \-})
                (unsigned-digits)
                (prs/expect-char \:)
                (unsigned-digits)
            )
        )
    )
)

(defliteral date-literal :date-literal
    (prs/chain
        (expect-string-ignore-case "DATE")
        (prs/expect-char \')
        (date-g)
        (prs/expect-char \')
    )
)

(defliteral time-literal :time-literal
    (prs/chain
        (expect-string-ignore-case "TIME")
        (prs/expect-char \')
        (time-g)
        (prs/expect-char \')
    )
)

(defliteral timestamp-literal :timestamp-literal
    (prs/chain
        (expect-string-ignore-case "TIMESTAMP")
        (prs/expect-char \')
        (date-g)
        (prs/expect-char \space)
        (time-g)
        (prs/expect-char \')
    )
)

(defn- interval-string []
    (prs/chain
        (prs/expect-char \')
        (prs/optional (prs/expect-char-if #{\+ \-}))
        (unsigned-digits) ; maybe year, day, hour, minute, second
        (prs/choice
            (prs/chain ; year-month
                (prs/expect-char \-)
                (unsigned-digits)
            )
            (prs/chain ; day hour[:minute[:second[.frac]]]
                (prs/expect-char \space)
                (unsigned-digits)
                (prs/optional
                    (prs/chain
                        (prs/expect-char \:)
                        (unsigned-digits)
                        (prs/optional
                            (prs/chain
                                (prs/expect-char \:)
                                (second-fraction)
                            )
                        )
                    )
                )
            )
            (prs/chain ; hour::minute:second[.frac]]
                (prs/expect-char \:)
                (unsigned-digits)
                (prs/expect-char \:)
                (second-fraction)
            )
            (prs/chain ; minute:second[.frac]
                (prs/expect-char \:)
                (second-fraction)
            )
            (prs/chain ; second.frac
                (prs/expect-char \.)
                (unsigned-digits)
            )
            (prs/foresee (prs/expect-char \')) ; either alone year, day or second
        )
        (prs/expect-char \')
    )
)

(defn- non-second-datetime-field []
    (prs/choice
        (expect-string-ignore-case "YEAR")
        (expect-string-ignore-case "MONTH")
        (expect-string-ignore-case "DAY")
        (expect-string-ignore-case "HOUR")
        (expect-string-ignore-case "MINUTE")
    )
)

(defn- single-datetime-field []
    (prs/choice
        (prs/chain
            (non-second-datetime-field)
            (prs/optional
                (prs/chain
                    (prs/expect-char \()
                    (unsigned-digits)
                    (prs/expect-char \))
                )
            )
        )
        (prs/chain
            (expect-string-ignore-case "SECOND")
            (prs/optional
                (prs/chain
                    (prs/expect-char \()
                    (unsigned-digits)
                    (prs/optional
                        (prs/chain
                            (prs/expect-char \,)
                            (unsigned-digits)
                        )
                    )
                    (prs/expect-char \))
                )
            )
        )
    )
)

(defn- start-to-end-datetime-field []
    (prs/chain
        (non-second-datetime-field)
        (prs/choice
            blank+
            (prs/chain
                (prs/expect-char \()
                (unsigned-digits)
                (prs/expect-char \))
                blank*
            )
        )
        (expect-string-ignore-case "TO")
        blank+
        (prs/choice
            (non-second-datetime-field)
            (prs/chain
                (expect-string-ignore-case "SECOND")
                (prs/optional
                    (prs/chain
                        (prs/expect-char \()
                        (unsigned-digits)
                        (prs/expect-char \))
                    )
                )
            )
        )
    )
)

(defn- interval-qualifier []
    (prs/choice
        (start-to-end-datetime-field)
        (single-datetime-field)
    )
)

(defliteral interval-literal :interval-literal
    (prs/chain
        (expect-string-ignore-case "INTERVAL")
        (interval-string)
        (interval-qualifier)
    )
)

(defn- one-escapable-char [ch]
    (prs/choice
        (prs/chain (prs/expect-char ch) (prs/expect-char ch))
        (prs/expect-char-if #(and (not= % :eof) (not= % ch)))
    )
)

(defliteral national-string-literal :national-string-literal
    (string-literal
        (expect-char-ignore-case \N)
        (prs/many (one-escapable-char \'))
    )
)

(defliteral national-string-literal :national-string-literal
    (string-literal
        (expect-char-ignore-case \N)
        (prs/many (one-escapable-char \'))
    )
)

; this is not completely compliant to SQL-92 for lack of leading charactor set spec
(defliteral character-string-literal :character-string-literal
    (prs/choice
        (string-literal
            (prs/foresee (prs/expect-char \'))
            (prs/many (one-escapable-char \'))
        )
        (prs/chain
            (prs/expect-char \")
            (prs/many (one-escapable-char \"))
            (prs/expect-char \")
            (prs/many
                (prs/chain
                    blank+
                    (prs/expect-char \")
                    (prs/many (one-escapable-char \"))
                    (prs/expect-char \")
                )
            )
        )
    )
)

(defliteral boolean-literal :boolean-literal
    (prs/choice
        (expect-string-ignore-case "TRUE")
        (expect-string-ignore-case "FALSE")
        (expect-string-ignore-case "UNKNOWN")
    )
)

(defliteral null-literal :null-literal
    (expect-string-ignore-case "NULL")
)

(defn literal [stream]
    (->> stream
        ((prs/choice
            null-literal
            hex-string
            date-literal
            time-literal
            timestamp-literal
            interval-literal
            boolean-literal
            national-string-literal
            character-string-literal
            ; do not support unicode literal yet
            numeric-literal
        ))
    )
)

(defn unsigned-literal [stream]
    (->> stream
        ((prs/choice
            hex-string
            date-literal
            time-literal
            timestamp-literal
            interval-literal
            boolean-literal
            national-string-literal
            character-string-literal
            ; do not support unicode literal yet
            unsigned-numeric-literal
        ))
    )
)

(def ^:private reserved-keywords #{
; MySQL-5.5 compliant, many differences with SQL standards
    "ACCESSIBLE" "ADD" "ALL" "ALTER" "ANALYZE" "AND" "AS" "ASC" "ASENSITIVE"

    "BEFORE" "BETWEEN" "BIGINT" "BINARY" "BLOB" "BOTH" "BY"

    "CALL" "CASCADE" "CASE" "CHANGE" "CHAR" "CHARACTER" "CHECK" "COLLATE"
    "COLUMN" "CONDITION" "CONSTRAINT" "CONTINUE" "CONVERT" "CREATE" "CROSS"
    "CURRENT_DATE" "CURRENT_TIME" "CURRENT_TIMESTAMP" "CURRENT_USER" "CURSOR"

    "DATABASE" "DATABASES" "DAY_HOUR" "DAY_MICROSECOND" "DAY_MINUTE" "DAY_SECOND"
    "DEC" "DECIMAL" "DECLARE" "DEFAULT" "DELAYED" "DELETE" "DESC" "DESCRIBE"
    "DETERMINISTIC" "DISTINCT" "DISTINCTROW" "DIV" "DOUBLE" "DROP" "DUAL"

    "EACH" "ELSE" "ELSEIF" "ENCLOSED" "ESCAPED" "EXISTS" "EXIT" "EXPLAIN"

    "FALSE" "FETCH" "FLOAT" "FLOAT4" "FLOAT8" "FOR" "FORCE" "FOREIGN" "FROM"
    "FULLTEXT"
    "FULL" ; SQL standards compliant, necessary for hive

    "GRANT" "GROUP" "HAVING" "HIGH_PRIORITY" "HOUR_MICROSECOND" "HOUR_MINUTE"
    "HOUR_SECOND"

    "IF" "IGNORE" "IN" "INDEX" "INFILE" "INNER" "INOUT" "INSENSITIVE" "INSERT"
    "INT" "INT1" "INT2" "INT3" "INT4" "INT8" "INTEGER" "INTERVAL" "INTO" "IS"
    "ITERATE"

    "JOIN"

    "KEY" "KEYS" "KILL"

    "LEADING" "LEAVE" "LEFT" "LIKE" "LIMIT" "LINEAR" "LINES" "LOAD" "LOCALTIME"
    "LOCALTIMESTAMP" "LOCK" "LONG" "LONGBLOB" "LONGTEXT" "LOOP" "LOW_PRIORITY"

    "MASTER_SSL_VERIFY_SERVER_CERT" "MATCH" "MAXVALUE" "MEDIUMBLOB" "MEDIUMINT"
    "MEDIUMTEXT" "MIDDLEINT" "MINUTE_MICROSECOND" "MINUTE_SECOND" "MOD"
    "MODIFIES"

    "NATURAL" "NOT" "NO_WRITE_TO_BINLOG" "NULL" "NUMERIC"

    "ON" "OPTIMIZE" "OPTION" "OPTIONALLY" "OR" "ORDER" "OUT" "OUTER" "OUTFILE"

    "PRECISION" "PRIMARY" "PROCEDURE" "PURGE"

    "RANGE" "READ" "READS" "READ_WRITE" "REAL" "REFERENCES" "REGEXP" "RELEASE"
    "RENAME" "REPEAT" "REPLACE" "REQUIRE" "RESIGNAL" "RESTRICT" "RETURN"
    "REVOKE" "RIGHT" "RLIKE"

    "SCHEMA" "SCHEMAS" "SECOND_MICROSECOND" "SELECT" "SENSITIVE" "SEPARATOR"
    "SET" "SHOW" "SIGNAL" "SMALLINT" "SPATIAL" "SPECIFIC" "SQL" "SQLEXCEPTION"
    "SQLSTATE" "SQLWARNING" "SQL_BIG_RESULT" "SQL_CALC_FOUND_ROWS"
    "SQL_SMALL_RESULT" "SSL" "STARTING" "STRAIGHT_JOIN"

    "TABLE" "TERMINATED" "THEN" "TINYBLOB" "TINYINT" "TINYTEXT" "TO" "TRAILING"
    "TRIGGER" "TRUE"

    "UNDO" "UNION" "UNIQUE" "UNLOCK" "UNSIGNED" "UPDATE" "USAGE" "USE" "USING"
    "UTC_DATE" "UTC_TIME" "UTC_TIMESTAMP"

    "VALUES" "VARBINARY" "VARCHAR" "VARCHARACTER" "VARYING"

    "WHEN" "WHERE" "WHILE" "WITH" "WRITE"

    "XOR"

    "YEAR_MONTH"

    "ZEROFILL"
})

(defn regular-identifier [stream]
    (let [
        [strm] (->> stream
            ((prs/chain
                (prs/expect-char-if
                    #(and
                        (instance? Character %)
                        (Character/isJavaIdentifierStart %)
                    )
                )
                (prs/many
                    (prs/expect-char-if
                        #(and
                            (instance? Character %)
                            (Character/isJavaIdentifierPart %)
                        )
                    )
                )
            ))
        )
        id (prs/extract-string-between stream strm)
        ]
        (if (reserved-keywords (str/upper-case id))
            (prs/gen-ISE stream "reserved keywords can not be identifiers")
            [strm id]
        )
    )
)

(defn- quoted-identifier [ch stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (prs/expect-char ch)
                (prs/many1
                    (prs/choice*
                        (constantly ch) (prs/chain
                            (prs/expect-char ch)
                            (prs/expect-char ch)
                        )
                        identity (prs/expect-char-if
                            #(and (not= % :eof) (not= % ch))
                        )
                    )
                )
                (prs/expect-char ch)
            ))
        )
        sb (StringBuilder.)
        ]
        (doseq [ch (second prsd)]
            (.append sb ch)
        )
        [strm (str sb)]
    )
)

(defn identifier [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/choice
                (partial quoted-identifier \`)
                regular-identifier
            ))
        )
        ]
        [strm {:type :identifier, :value prsd}]
    )
)

(defn dotted-identifier [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/separated-list identifier (prs/expect-char \.)))
        )
        prsd (map :value prsd)
        ]
        [strm {:type :dotted-identifier, :value prsd}]
    )
)

(defn column-list [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/separated-list
                identifier
                (prs/chain
                    blank*
                    (prs/expect-char \,)
                    blank*
                )
            ))
        )
        prsd (map :value prsd)
        ]
        [strm {:type :column-list, :value prsd}]
    )
)

(defn paren-column-list [stream]
    ((paren column-list) stream)
)

(defn correlation [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (prs/optional
                    (prs/chain
                        (expect-string-ignore-case "AS")
                        blank+
                    )
                )
                identifier
            ))
        )
        [_ {nm :value}] prsd
        ]
        [strm {:type :correlation, :name nm}]
    )
)

(declare table-reference)
(declare value-expr)

(defn- raw-table [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                dotted-identifier
                (prs/choice*
                    second (prs/chain
                        blank+
                        correlation
                    )
                    nil
                )
            ))
        )
        [ext-tbl tbl-cor] prsd
        res {:type :table, :refer (:value ext-tbl)}
        res (if-not tbl-cor res (merge tbl-cor res))
        ]
        [strm res]
    )
)

(defn- derived-table [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                value-expr
                blank*
                correlation
            ))
        )
        [subquery _ as] prsd
        ]
        [strm (-> as (assoc :type :derived-table :value subquery))]
    )
)

(defn- table-reference-base [stream]
    (->> stream
        ((prs/choice
            raw-table
            derived-table
            (paren table-reference)
            ; TODO: lateral derived table
            ; TODO: collection derived table
            ; TODO: TABLE function derived table
            ; TODO: only spec
        ))
    )
)

(defn- join-type [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (prs/choice*
                    (constantly :left) (expect-string-ignore-case "LEFT")
                    (constantly :right) (expect-string-ignore-case "RIGHT")
                    (constantly :full) (expect-string-ignore-case "FULL")
                )
                (prs/optional
                    (prs/chain
                        blank+
                        (expect-string-ignore-case "OUTER")
                    )
                )
            ))
        )
        res (first prsd)
        ]
        [strm res]
    )
)

(defn- on-clause [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (expect-string-ignore-case "ON")
                blank*
                value-expr
            ))
        )
        res (last prsd)
        ]
        [strm {:on res}]
    )
)

(defn- join-spec [stream]
    (on-clause stream)
)

(defn- cross-join [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                blank+
                (expect-string-ignore-case "CROSS")
                blank+
                (expect-string-ignore-case "JOIN")
                blank+
                table-reference
                (prs/choice*
                    last (prs/chain
                        blank+
                        join-spec
                    )
                    nil
                )
            ))
        )
        to (nth prsd 5)
        spec (last prsd)
        res {:type :cross-join, :right to}
        res (if-not spec res (merge res spec))
        ]
        [strm res]
    )
)

(defn- inner-join [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                blank+
                (expect-string-ignore-case "JOIN")
                blank+
                table-reference
                (prs/choice*
                    last (prs/chain
                        blank+
                        join-spec
                    )
                    nil
                )
            ))
        )
        to (nth prsd 3)
        spec (last prsd)
        res {:type :join, :right to}
        res (if-not spec res (merge res spec))
        ]
        [strm res]
    )
)

(defn- outer-join [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                blank+
                join-type
                blank*
                (expect-string-ignore-case "JOIN")
                blank+
                table-reference
                blank+
                join-spec
            ))
        )
        ty (second prsd)
        to (nth prsd 5)
        spec (last prsd)
        res {:type :outer-join, :right to, :join-type ty}
        res (merge res spec)
        ]
        [strm res]
    )
)

(defn- join []
    ; This does not follow SQL standards.
    ; This follows Hive syntax.
    (prs/choice
        cross-join
        inner-join
        outer-join
    )
)

(defn- extend-joins [tr stream]
    (let [
        [strm prsd] (->> stream
            ((prs/optional
                (join)
            ))
        )
        ]
        (if prsd
            (recur (assoc prsd :left tr) strm)
            [stream tr]
        )
    )
)

(defn- table-reference [stream]
    (let [
        [strm1 prsd1] (->> stream
            (table-reference-base)
        )
        [strm2 prsd2] (extend-joins prsd1 strm1)
        ]
        [strm2 prsd2]
    )
)

(defn from-clause [stream]
    (let [
        [strm1] (->> stream
            ((prs/chain
                (expect-string-ignore-case "FROM")
                blank+
            ))
        )
        [strm2 prsd2] (->> strm1
            ((prs/separated-list
                table-reference
                (prs/chain
                    blank*
                    (prs/expect-char \,)
                    blank*
                )
            ))
        )
        ]
        [strm2 {:type :from-clause, :tables prsd2}]
    )
)

(defn- set-quantifier [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/choice*
                (constantly :distinct) (expect-string-ignore-case "DISTINCT")
                (constantly :all) (expect-string-ignore-case "ALL")
                nil
            ))
        )
        ]
        [strm (if prsd {:set-quantifier prsd})]
    )
)

(defn- derived-column [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                value-expr
                (prs/choice*
                    last (prs/chain
                        (prs/choice
                            (prs/chain
                                blank+
                                (expect-string-ignore-case "AS")
                                blank+
                            )
                            blank+
                        )
                        (prs/choice
                            identifier
                            (paren
                                (prs/separated-list
                                    identifier
                                    (prs/chain
                                        blank*
                                        (prs/expect-char \,)
                                        blank*
                                    )
                                )
                            )
                        )
                    )
                    nil
                )
            ))
        )
        [v n] prsd
        res {:type :derived-column, :value v}
        res (if-not n res (assoc res :name n))
        ]
        [strm res]
    )
)

(defn- select-list [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/separated-list
                derived-column
                (prs/chain
                    blank*
                    (prs/expect-char \,)
                    blank*
                )
            ))
        )
        ]
        [strm prsd]
    )
)

(defn- where-clause [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (expect-string-ignore-case "WHERE")
                blank+
                value-expr
            ))
        )
        res (last prsd)
        ]
        [strm res]
    )
)

(defn grouping-list [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/separated-list
                (prs/choice
                    dotted-identifier
                    unsigned-numeric-literal
                )
                (prs/chain
                    blank*
                    (prs/expect-char \,)
                    blank*
                )
            ))
        )
        ]
        [strm prsd]
    )
)

(defn order-list [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/separated-list
                (prs/choice*
                    (fn [[v o]] {:ordering o, :value v}) (prs/chain
                        (prs/choice
                            identifier
                            unsigned-numeric-literal
                        )
                        (prs/choice*
                            (constantly :asc) (prs/chain
                                blank+
                                (expect-string-ignore-case "ASC")
                            )
                            (constantly :desc) (prs/chain
                                blank+
                                (expect-string-ignore-case "DESC")
                            )
                            nil
                        )
                    )
                )
                (prs/chain
                    blank*
                    (prs/expect-char \,)
                    blank*
                )
            ))
        )
        ]
        [strm prsd]
    )
)

(defn select [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (expect-string-ignore-case "SELECT")
                blank+
                set-quantifier
                blank*
                select-list
                blank+
                from-clause
                (prs/choice*
                    last (prs/chain
                        blank+
                        where-clause
                    )
                    nil
                )
                (prs/choice*
                    last (prs/chain
                        blank+
                        (expect-string-ignore-case "GROUP")
                        blank+
                        (expect-string-ignore-case "BY")
                        blank+
                        grouping-list
                    )
                    nil
                )
                (prs/choice*
                    last (prs/chain
                        blank+
                        (expect-string-ignore-case "ORDER")
                        blank+
                        (expect-string-ignore-case "BY")
                        blank+
                        order-list
                    )
                    nil
                )
                (prs/choice*
                    last (prs/chain
                        blank+
                        (expect-string-ignore-case "LIMIT")
                        blank+
                        unsigned-numeric-literal
                    )
                    nil
                )
            ))
        )
        sq (nth prsd 2)
        sl (nth prsd 4)
        from (nth prsd 6)
        where (nth prsd 7)
        group-by (nth prsd 8)
        order-by (nth prsd 9)
        limit (nth prsd 10)
        res {:type :select, :select-list sl, :from-clause (:tables from)}
        res (if-not sq res (merge res sq))
        res (if-not where res (assoc res :where where))
        res (if-not group-by res (assoc res :group-by group-by))
        res (if-not order-by res (assoc res :order-by order-by))
        res (if-not limit res (assoc res :limit limit))
        ]
        [strm res]
    )
)

(defn select-in-union [stream]
    (->> stream
        ((prs/choice*
            identity select
            #(nth % 2) (prs/chain
                (prs/expect-char \()
                blank*
                select
                blank*
                (prs/expect-char \))
            )
        ))
    )
)

(defn query [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/separated-list
                select-in-union
                (prs/chain
                    blank+
                    (expect-string-ignore-case "UNION")
                    blank+
                    (expect-string-ignore-case "ALL")
                    blank+
                )
            ))
        )
        ]
        (if (= (count prsd) 1)
            [strm (first prsd)]
            [strm {:type :union, :qualifier :all, :selects prsd}]
        )
    )
)


(defn- paren:no-transparent [parser stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (prs/expect-char \()
                parser
                (prs/expect-char \))
            ))
        )
        res (second prsd)
        ]
        [strm {:type :paren, :value res}]
    )
)

(defn- remove-paren-if-possible [expr]
    (if (= (:type expr) :paren)
        (recur (:value expr))
        expr
    )
)

(declare value-expr)
(declare value-expr:term)
(declare boolean-primary)
(declare predicate)
(declare bit-expr)
(declare simple-expr)
(declare simple-expr:term)

(defn- value-expr-list [stream]
    (->> stream
        ((prs/separated-list
            value-expr
            (prs/chain
                blank*
                (prs/expect-char \,)
                blank*
            )
        ))
    )
)

(defn- asterisked-identifier [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (prs/choice*
                    first (prs/chain
                        dotted-identifier
                        (prs/expect-char \.)
                    )
                    nil
                )
                (prs/expect-char \*)
            ))
        )
        res (first prsd)
        res (if-not res
            {:type :asterisk}
            {:type :asterisk, :refer (:value res)}
        )
        ]
        [strm res]
    )
)

(defn- binary-operator [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (expect-string-ignore-case "BINARY")
                blank+
                simple-expr
            ))
        )
        res (last prsd)
        ]
        [strm {:type :binary, :value res}]
    )
)

(defn- exists-operator [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (expect-string-ignore-case "EXISTS")
                blank*
                (paren query)
            ))
        )
        res (last prsd)
        ]
        [strm {:type :exists, :value res}]
    )
)

(defn- cast-operator [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (expect-string-ignore-case "CAST")
                blank*
                (paren (prs/chain
                    value-expr
                    blank+
                    (expect-string-ignore-case "AS")
                    blank+
                    (prs/choice*
                        (constantly :int) (expect-string-ignore-case "INT")
                        (constantly :tinyint) (expect-string-ignore-case "TINYINT")
                        (constantly :smallint) (expect-string-ignore-case "SMALLINT")
                        (constantly :bigint) (expect-string-ignore-case "BIGINT")
                        (constantly :decimal) (expect-string-ignore-case "DECIMAL")
                        (constantly :float) (expect-string-ignore-case "FLOAT")
                        (constantly :double) (expect-string-ignore-case "DOUBLE")
                        (constantly :timestamp) (expect-string-ignore-case "TIMESTAMP")
                        (constantly :date) (expect-string-ignore-case "DATE")
                        (constantly :string) (expect-string-ignore-case "STRING")
                        (constantly :varchar) (expect-string-ignore-case "VARCHAR")
                        (constantly :boolean) (expect-string-ignore-case "BOOLEAN")
                        (constantly :binary) (expect-string-ignore-case "BINARY")
                    )
                ))
            ))
        )
        prsd (last prsd)
        v (first prsd)
        t (last prsd)
        ]
        [strm {:type :cast, :left v, :right t}]
    )
)

(defn- case-operator [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (expect-string-ignore-case "CASE")
                blank+
                (prs/choice*
                    first (prs/chain
                        value-expr
                        blank+
                    )
                    nil
                )
                (prs/many1
                    (prs/chain
                        (expect-string-ignore-case "WHEN")
                        blank+
                        value-expr
                        blank+
                        (expect-string-ignore-case "THEN")
                        blank+
                        value-expr
                        blank+
                    )
                )
                (prs/choice*
                    #(nth % 2) (prs/chain
                        (expect-string-ignore-case "ELSE")
                        blank+
                        value-expr
                        blank+
                    )
                    nil
                )
                (expect-string-ignore-case "END")
            ))
        )
        v (nth prsd 2)
        ww (nth prsd 3)
        ww (for [[_ _ x _ _ _ y] ww] [x y])
        else (nth prsd 4)
        res {:type :case, :when ww}
        res (if-not v res (assoc res :value v))
        res (if-not else res (assoc res :else else))
        ]
        [strm res]
    )
)

(defn- check-arity [opt args stream]
{
    :pre [(not (nil? opt))]
}
    (let [
        arity (count args)
        l (:min opt)
        u (:max opt)
        ]
        (when (and l (< arity l))
            (prs/gen-ISE stream
                (format "%s requires at least %d argument(s)" (:name opt) l)
            )
        )
        (when (and u (> arity u))
            (prs/gen-ISE stream
                (format "%s requires at most %d argument(s)" (:name opt) u)
            )
        )
    )
)

(def ^:private distinct-buildin-functions {
    :distinct-count {:min 1, :max 2, :name "COUNT(DISTINCT"}
    :distinct-sum {:min 1, :max 1, :name "SUM(DISTINCT"}
    :distinct-avg {:min 1, :max 1, :name "AVG(DISTINCT"}
})

(defn- distinct-count-sum-avg [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (prs/choice*
                    (constantly :distinct-count) (expect-string-ignore-case "COUNT")
                    (constantly :distinct-sum) (expect-string-ignore-case "SUM")
                    (constantly :distinct-avg) (expect-string-ignore-case "AVG")
                )
                blank*
                (paren (prs/chain
                    blank*
                    (expect-string-ignore-case "DISTINCT")
                    blank+
                    (prs/separated-list
                        value-expr
                        (prs/chain
                            blank*
                            (prs/expect-char \,)
                            blank*
                        )
                    )
                ))
            ))
        )
        ty (first prsd)
        args (->> prsd (last) (last))
        ]
        (check-arity (distinct-buildin-functions ty) args stream)
        [strm {:type ty, :args args}]
    )
)

(def ^:private build-in-functions {
    :power {:min 2, :max 2, :name "POWER/POW"}
    :round {:min 1, :max 2, :name "ROUND"}
    :floor {:min 1, :max 1, :name "FLOOR"}
    :ceil {:min 1, :max 1, :name "CEIL/CEILING"}
    :rand {:max 1, :name "RAND"}
    :exp {:min 1, :max 1, :name "EXP"}
    :ln {:min 1, :max 1, :name "LN"}
    :log10 {:min 1, :max 1, :name "LOG10"}
    :log2 {:min 1, :max 1, :name "LOG2"}
    :log {:min 2, :max 2, :name "LOG"}
    :sqrt {:min 1, :max 1, :name "SQRT"}
    :bin {:min 1, :max 1, :name "BIN"}
    :hex {:min 1, :max 1, :name "HEX"}
    :unhex {:min 1, :max 1, :name "UNHEX"}
    :conv {:min 3, :max 3, :name "CONV"}
    :abs {:min 1, :max 1, :name "ABS"}
    :pmod {:min 2, :max 2, :name "PMOD"}
    :sin {:min 1, :max 1, :name "SIN"}
    :asin {:min 1, :max 1, :name "ASIN"}
    :cos {:min 1, :max 1, :name "COS"}
    :acos {:min 1, :max 1, :name "ACOS"}
    :tan {:min 1, :max 1, :name "TAN"}
    :atan {:min 1, :max 1, :name "ATAN"}
    :degrees {:min 1, :max 1, :name "DEGREES"}
    :radians {:min 1, :max 1, :name "RADIANS"}
    :positive {:min 1, :max 1, :name "POSITIVE"}
    :negative {:min 1, :max 1, :name "NEGATIVE"}
    :sign {:min 1, :max 1, :name "SIGN"}
    :e {:max 0, :name "E"}
    :pi {:max 0, :name "PI"}
    :binary {:min 1, :max 1, :name "BINARY"}
    :from_unixtime {:min 1, :max 2, :name "FROM_UNIXTIME"}
    :unix_timestamp {:max 2, :name "UNIX_TIMESTAMP"}
    :to_date {:min 1, :max 1, :name "TO_DATE"}
    :year {:min 1, :max 1, :name "YEAR"}
    :month {:min 1, :max 1, :name "MONTH"}
    :day {:min 1, :max 1, :name "DAY/DAYOFMONTH"}
    :hour {:min 1, :max 1, :name "HOUR"}
    :minute {:min 1, :max 1, :name "MINUTE"}
    :second {:min 1, :max 1, :name "SECOND"}
    :weekofyear {:min 1, :max 1, :name "WEEKOFYEAR"}
    :datediff {:min 2, :max 2, :name "DATEDIFF"}
    :date_add {:min 2, :max 2, :name "DATE_ADD"}
    :date_sub {:min 2, :max 2, :name "DATE_SUB"}
    :from_utc_timestamp {:min 2, :max 2, :name "FROM_UTC_TIMESTAMP"}
    :to_utc_timestamp {:min 2, :max 2, :name "TO_UTC_TIMESTAMP"}
    :if {:min 3, :max 3, :name "IF"}
    :ascii {:min 1, :max 1, :name "ASCII"}
    :base64 {:min 1, :max 1, :name "BASE64"}
    :concat {:name "CONCAT"}
    :concat_ws {:min 1, :name "CONCAT_WS"}
    :decode {:min 2, :max 2, :name "DECODE"}
    :encode {:min 2, :max 2, :name "ENCODE"}
    :find_in_set {:min 2, :max 2, :name "FIND_IN_SET"}
    :format_number {:min 2, :max 2, :name "FORMAT_NUMBER"}
    :get_json_object {:min 2, :max 2, :name "GET_JSON_OBJECT"}
    :in_file {:min 2, :max 2, :name "IN_FILE"}
    :instr {:min 2, :max 2, :name "INSTR"}
    :length {:min 1, :max 1, :name "LENGTH"}
    :locate {:min 2, :max 3, :name "LOCATE"}
    :lower {:min 1, :max 1, :name "LOWER/LCASE"}
    :lpad {:min 3, :max 3, :name "LPAD"}
    :ltrim {:min 1, :max 1, :name "LTRIM"}
    :parse_url {:min 2, :max 3, :name "PARSE_URL"}
    :printf {:min 1, :name "PRINTF"}
    :regexp_extract {:min 3, :max 3, :name "REGEXP_EXTRACT"}
    :regexp_replace {:min 3, :max 3, :name "REGEXP_REPLACE"}
    :repeat {:min 2, :max 2, :name "REPEAT"}
    :reverse {:min 1, :max 1, :name "REVERSE"}
    :rpad {:min 3, :max 3, :name "RPAD"}
    :rtrim {:min 1, :max 1, :name "RTRIM"}
    :space {:min 1, :max 1, :name "SPACE"}
    :substring {:min 2, :max 3, :name "SUBSTR/SUBSTRING"}
    :translate {:min 3, :max 3, :name "TRANSLATE"}
    :trim {:min 1, :max 1, :name "TRIM"}
    :unbase64 {:min 1, :max 1, :name "UNBASE64"}
    :upper {:min 1, :max 1, :name "UPPER/UCASE"}
    :count {:min 1, :max 1, :name "COUNT"}
    :sum {:min 1, :max 1, :name "SUM"}
    :avg {:min 1, :max 1, :name "AVG"}
    :min {:min 1, :max 1, :name "MIN"}
    :max {:min 1, :max 1, :name "MAX"}
    :var_pop {:min 1, :max 1, :name "VARIANCE/VAR_POP"}
    :var_samp {:min 1, :max 1, :name "VAR_SAMP"}
    :stddev_pop {:min 1, :max 1, :name "STDDEV_POP"}
    :stddev_samp {:min 1, :max 1, :name "STDDEV_SAMP"}
    :covar_pop {:min 2, :max 2, :name "COVAR_POP"}
    :covar_samp {:min 2, :max 2, :name "COVAR_SAMP"}
    :corr {:min 2, :max 2, :name "CORR"}
    :percentile {:min 2, :max 2, :name "PERCENTILE"}
    :percentile_approx {:min 2, :max 2, :name "PERCENTILE_APPROX"}
})

(defn- function-call [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (prs/choice*
                    (constantly :ascii) (expect-string-ignore-case "ASCII")
                    (constantly :acos) (expect-string-ignore-case "ACOS")
                    (constantly :asin) (expect-string-ignore-case "ASIN")
                    (constantly :atan) (expect-string-ignore-case "ATAN")
                    (constantly :abs) (expect-string-ignore-case "ABS")
                    (constantly :avg) (expect-string-ignore-case "AVG")

                    (constantly :base64) (expect-string-ignore-case "BASE64")
                    (constantly :binary) (expect-string-ignore-case "BINARY")
                    (constantly :bin) (expect-string-ignore-case "BIN")

                    (constantly :covar_samp) (expect-string-ignore-case "COVAR_SAMP")
                    (constantly :concat_ws) (expect-string-ignore-case "CONCAT_WS")
                    (constantly :covar_pop) (expect-string-ignore-case "COVAR_POP")
                    (constantly :ceil) (expect-string-ignore-case "CEILING")
                    (constantly :concat) (expect-string-ignore-case "CONCAT")
                    (constantly :count) (expect-string-ignore-case "COUNT")
                    (constantly :ceil) (expect-string-ignore-case "CEIL")
                    (constantly :conv) (expect-string-ignore-case "CONV")
                    (constantly :corr) (expect-string-ignore-case "CORR")
                    (constantly :cos) (expect-string-ignore-case "COS")

                    (constantly :day) (expect-string-ignore-case "DAYOFMONTH")
                    (constantly :date_add) (expect-string-ignore-case "DATE_ADD")
                    (constantly :date_sub) (expect-string-ignore-case "DATE_SUB")
                    (constantly :datediff) (expect-string-ignore-case "DATEDIFF")
                    (constantly :degrees) (expect-string-ignore-case "DEGREES")
                    (constantly :decode) (expect-string-ignore-case "DECODE")
                    (constantly :day) (expect-string-ignore-case "DAY")

                    (constantly :encode) (expect-string-ignore-case "ENCODE")
                    (constantly :exp) (expect-string-ignore-case "EXP")
                    (constantly :e) (expect-string-ignore-case "E")

                    (constantly :from_utc_timestamp) (expect-string-ignore-case "FROM_UTC_TIMESTAMP")
                    (constantly :format_number) (expect-string-ignore-case "FORMAT_NUMBER")
                    (constantly :from_unixtime) (expect-string-ignore-case "FROM_UNIXTIME")
                    (constantly :find_in_set) (expect-string-ignore-case "FIND_IN_SET")
                    (constantly :floor) (expect-string-ignore-case "FLOOR")

                    (constantly :get_json_object) (expect-string-ignore-case "GET_JSON_OBJECT")

                    (constantly :hour) (expect-string-ignore-case "HOUR")
                    (constantly :hex) (expect-string-ignore-case "HEX")

                    (constantly :in_file) (expect-string-ignore-case "IN_FILE")
                    (constantly :instr) (expect-string-ignore-case "INSTR")
                    (constantly :if) (expect-string-ignore-case "IF")

                    (constantly :length) (expect-string-ignore-case "LENGTH")
                    (constantly :locate) (expect-string-ignore-case "LOCATE")
                    (constantly :lower) (expect-string-ignore-case "LCASE")
                    (constantly :log10) (expect-string-ignore-case "LOG10")
                    (constantly :lower) (expect-string-ignore-case "LOWER")
                    (constantly :ltrim) (expect-string-ignore-case "LTRIM")
                    (constantly :lpad) (expect-string-ignore-case "LPAD")
                    (constantly :log2) (expect-string-ignore-case "LOG2")
                    (constantly :log) (expect-string-ignore-case "LOG")
                    (constantly :ln) (expect-string-ignore-case "LN")

                    (constantly :minute) (expect-string-ignore-case "MINUTE")
                    (constantly :month) (expect-string-ignore-case "MONTH")
                    (constantly :max) (expect-string-ignore-case "MAX")
                    (constantly :min) (expect-string-ignore-case "MIN")

                    (constantly :negative) (expect-string-ignore-case "NEGATIVE")

                    (constantly :percentile_approx) (expect-string-ignore-case "PERCENTILE_APPROX")
                    (constantly :percentile) (expect-string-ignore-case "PERCENTILE")
                    (constantly :parse_url) (expect-string-ignore-case "PARSE_URL")
                    (constantly :positive) (expect-string-ignore-case "POSITIVE")
                    (constantly :printf) (expect-string-ignore-case "PRINTF")
                    (constantly :power) (expect-string-ignore-case "POWER")
                    (constantly :pmod) (expect-string-ignore-case "PMOD")
                    (constantly :power) (expect-string-ignore-case "POW")
                    (constantly :pi) (expect-string-ignore-case "PI")

                    (constantly :regexp_extract) (expect-string-ignore-case "REGEXP_EXTRACT")
                    (constantly :regexp_replace) (expect-string-ignore-case "REGEXP_REPLACE")
                    (constantly :radians) (expect-string-ignore-case "RADIANS")
                    (constantly :reverse) (expect-string-ignore-case "REVERSE")
                    (constantly :repeat) (expect-string-ignore-case "REPEAT")
                    (constantly :round) (expect-string-ignore-case "ROUND")
                    (constantly :rtrim) (expect-string-ignore-case "RTRIM")
                    (constantly :rand) (expect-string-ignore-case "RAND")
                    (constantly :rpad) (expect-string-ignore-case "RPAD")

                    (constantly :stddev_samp) (expect-string-ignore-case "STDDEV_SAMP")
                    (constantly :stddev_pop) (expect-string-ignore-case "STDDEV_POP")
                    (constantly :substring) (expect-string-ignore-case "SUBSTRING")
                    (constantly :second) (expect-string-ignore-case "SECOND")
                    (constantly :substring) (expect-string-ignore-case "SUBSTR")
                    (constantly :space) (expect-string-ignore-case "SPACE")
                    (constantly :sign) (expect-string-ignore-case "SIGN")
                    (constantly :sqrt) (expect-string-ignore-case "SQRT")
                    (constantly :sin) (expect-string-ignore-case "SIN")
                    (constantly :sum) (expect-string-ignore-case "SUM")

                    (constantly :to_utc_timestamp) (expect-string-ignore-case "TO_UTC_TIMESTAMP")
                    (constantly :translate) (expect-string-ignore-case "TRANSLATE")
                    (constantly :to_date) (expect-string-ignore-case "TO_DATE")
                    (constantly :trim) (expect-string-ignore-case "TRIM")
                    (constantly :tan) (expect-string-ignore-case "TAN")

                    (constantly :unix_timestamp) (expect-string-ignore-case "UNIX_TIMESTAMP")
                    (constantly :unbase64) (expect-string-ignore-case "UNBASE64")
                    (constantly :upper) (expect-string-ignore-case "UCASE")
                    (constantly :unhex) (expect-string-ignore-case "UNHEX")
                    (constantly :upper) (expect-string-ignore-case "UPPER")

                    (constantly :var_samp) (expect-string-ignore-case "VAR_SAMP")
                    (constantly :var_pop) (expect-string-ignore-case "VAR_POP")
                    (constantly :var_pop) (expect-string-ignore-case "VARIANCE")

                    (constantly :weekofyear) (expect-string-ignore-case "WEEKOFYEAR")

                    (constantly :year) (expect-string-ignore-case "YEAR")
                )
                blank*
                (paren
                    (prs/choice*
                        identity (prs/separated-list
                            value-expr
                            (prs/chain
                                blank*
                                (prs/expect-char \,)
                                blank*
                            )
                        )
                        (constantly []) (prs/chain
                            blank*
                            (prs/foresee (prs/expect-char \)))
                        )
                    )
                )
            ))
        )
        [f _ args] prsd
        ]
        (check-arity (build-in-functions f) args stream)
        [strm {:type :func-call, :func f, :args args}]
    )
)

(defn- simple-expr:term [stream]
    (->> stream
        ((prs/choice
            (paren query)
            exists-operator
            cast-operator
            case-operator
            distinct-count-sum-avg
            function-call
            binary-operator
            literal
            asterisked-identifier
            dotted-identifier
        ))
    )
)

(defn- simple-expr [stream]
    (simple-expr:term stream)
)

(defn- bit-expr:term [stream]
    (->> stream
        ((prs/choice*
            (fn [[t _ x]] {:type t, :value x}) (prs/chain
                (prs/choice*
                    (constantly :unary+) (prs/expect-char \+)
                    (constantly :unary-) (prs/expect-char \-)
                    (constantly :unary-tilde) (prs/expect-char \~)
                )
                blank+
                (prs/choice
                    simple-expr
                    (paren bit-expr)
                )
            )
            identity (paren bit-expr)
            identity simple-expr
        ))
    )
)

(defn- bit-expr:recur [left stream]
    (let [
        [strm prsd] (->> stream
            ((prs/optional
                (prs/chain
                    (prs/choice*
                        second (prs/chain
                            blank*
                            (prs/choice*
                                (constantly :|) (prs/expect-string "|")
                                (constantly :&) (prs/expect-string "&")
                                (constantly :<<) (prs/expect-string "<<")
                                (constantly :>>) (prs/expect-string ">>")

                                (constantly :+) (prs/expect-string "+")
                                (constantly :-) (prs/expect-string "-")
                                (constantly :*) (prs/expect-string "*")
                                (constantly :div) (prs/expect-string "/")
                                (constantly :mod) (prs/expect-string "%")
                                (constantly :caret) (prs/expect-string "^")
                            )
                            blank*
                        )
                    )
                    bit-expr:term
                )
            ))
        )
        ]
        (if-not prsd
            [stream left]
            (let [
                [connective right] prsd
                right (remove-paren-if-possible right)
                nxt (case (:type left)
                    :paren
                    {:type :bit-expr, :value [(:value left) connective right]}
                    :bit-expr
                    (assoc left :value (concat (:value left) [connective right]))
                    {:type :bit-expr, :value [left connective right]}
                )
                ]
                (recur nxt strm)
            )
        )
    )
)

(defn- bit-expr [stream]
    (let [
        [strm1 prsd1] (bit-expr:term stream)
        [strm2 prsd2] (bit-expr:recur prsd1 strm1)
        ]
        [strm2 prsd2]
    )
)

(defn- extend-predicate [left stream]
    (let [
        [strm prsd] (->> stream
            ((prs/optional
                (prs/chain
                    (prs/choice*
                        (constantly true) (prs/chain
                            blank+
                            (expect-string-ignore-case "NOT")
                        )
                        false
                    )
                    (prs/choice*
                        (fn [x] {:type :in-array, :right (last x)}) (prs/chain
                            blank+
                            (expect-string-ignore-case "IN")
                            blank+
                            (prs/choice*
                                #(map remove-paren-if-possible %)
                                (paren value-expr-list)
                            )
                        )
                        (fn [x] {:type :between, :middle (nth x 3), :right (last x)})
                        (prs/chain
                            blank+
                            (expect-string-ignore-case "BETWEEN")
                            blank+
                            (prs/choice*
                                #(remove-paren-if-possible %) bit-expr
                            )
                            blank+
                            (expect-string-ignore-case "AND")
                            blank+
                            (prs/choice*
                                #(remove-paren-if-possible %) predicate
                            )
                        )
                        (fn [x] {:type :like, :right (last x)}) (prs/chain
                            blank+
                            (expect-string-ignore-case "LIKE")
                            blank+
                            (prs/choice*
                                #(remove-paren-if-possible %)
                                simple-expr
                            )
                        )
                        (fn [x] {:type :reglike, :right (last x)}) (prs/chain
                            blank+
                            (expect-string-ignore-case "REGEXP")
                            blank+
                            (prs/choice*
                                #(remove-paren-if-possible %)
                                bit-expr
                            )
                        )
                    )
                )
            ))
        )
        ]
        (if-not prsd
            [stream left]
            (let [
                [not? res] prsd
                res (assoc res :left (remove-paren-if-possible left))
                res (if-not not? res
                    (assoc res :type
                        (case (:type res)
                            :in-array :not-in-array
                            :between :not-between
                            :like :not-like
                            :reglike :not-reglike
                        )
                    )
                )
                ]
                [strm res]
            )
        )
    )
)

(defn- predicate [stream]
    (let [
        [strm1 prsd1] (bit-expr stream)
        [strm2 prsd2] (extend-predicate prsd1 strm1)
        ]
        [strm2 prsd2]
    )
)

(defn- boolean-primary:recur [left stream]
    (let [
        [strm prsd] (->> stream
            ((prs/choice*
                (fn [x] {:type :is-not, :right (last x)}) (prs/chain
                    blank+
                    (expect-string-ignore-case "IS")
                    blank+
                    (expect-string-ignore-case "NOT")
                    blank+
                    null-literal
                )
                (fn [x] {:type :is, :right (last x)}) (prs/chain
                    blank+
                    (expect-string-ignore-case "IS")
                    blank+
                    null-literal
                )
                (fn [[_ c _ r]] {:type c, :right r}) (prs/chain
                    blank*
                    (prs/choice*
                        (constantly :<=>) (prs/expect-string "<=>")
                        (constantly :=) (prs/expect-string "=")
                        (constantly :<>) (prs/expect-string "<>")
                        (constantly :<>) (prs/expect-string "!=")
                        (constantly :<=) (prs/expect-string "<=")
                        (constantly :>=) (prs/expect-string ">=")
                        (constantly :<) (prs/expect-string "<")
                        (constantly :>) (prs/expect-string ">")
                    )
                    blank*
                    predicate
                )
                nil
            ))
        )
        ]
        (if-not prsd
            [stream left]
            (let [nxt (assoc prsd :left left)]
                (recur nxt strm)
            )
        )
    )
)

(defn- boolean-primary [stream]
    (let [
        [strm1 prsd1] (->> stream
            ((prs/choice
                predicate
                (paren boolean-primary)
            ))
        )
        prsd1 (remove-paren-if-possible prsd1)
        [strm2 prsd2] (boolean-primary:recur prsd1 strm1)
        ]
        [strm2 prsd2]
    )
)

(defn- boolean-negation [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (prs/choice*
                    (constantly :boolean-negation) (prs/chain
                        (expect-string-ignore-case "NOT")
                        blank+
                    )
                    (constantly :boolean-negation) (prs/chain
                        (prs/expect-char \!)
                        blank*
                    )
                )
                value-expr:term
            ))
        )
        [ty sub] prsd
        res {:type ty, :value (remove-paren-if-possible sub)}
        ]
        [strm res]
    )
)

(defn boolean:test [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                boolean-primary
                (prs/choice*
                    (constantly :is-not) (prs/chain
                        blank+
                        (expect-string-ignore-case "IS")
                        blank+
                        (expect-string-ignore-case "NOT")
                        blank+
                    )
                    (constantly :is) (prs/chain
                        blank+
                        (expect-string-ignore-case "IS")
                        blank+
                    )
                )
                boolean-literal
            ))
        )
        [l c r] prsd
        res {
            :type c
            :left (remove-paren-if-possible l)
            :right r
        }
        ]
        [strm res]
    )
)

(defn value-expr:term [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/choice
                boolean:test
                boolean-negation
                (partial paren:no-transparent value-expr)
                boolean-primary
            ))
        )
        ]
        [strm prsd]
    )
)

(defn value-expr:recur [left stream]
    (let [
        [strm prsd] (->> stream
            ((prs/optional
                (prs/chain
                    (prs/choice*
                        second (prs/chain
                            blank+
                            (prs/choice*
                                (constantly :and) (expect-string-ignore-case "AND")
                                (constantly :or) (expect-string-ignore-case "OR")
                            )
                            blank+
                        )
                        second (prs/chain
                            blank*
                            (prs/choice*
                                (constantly :and) (prs/expect-string "&&")
                                (constantly :or) (prs/expect-string "||")
                            )
                            blank*
                        )
                    )
                    value-expr:term
                )
            ))
        )
        ]
        (if-not prsd
            [stream left]
            (let [
                [connective right] prsd
                right (remove-paren-if-possible right)
                nxt (case (:type left)
                    :paren
                    {:type :value-expr, :value [(:value left) connective right]}
                    :value-expr
                    (assoc left :value (concat (:value left) [connective right]))
                    {:type :value-expr, :value [left connective right]}
                )
                ]
                (recur nxt strm)
            )
        )
    )
)

(defn value-expr [stream]
    ; This follows MySQL-5.5 syntax
    (let [
        [strm1 prsd1] (->> stream
            (value-expr:term)
        )
        [strm2 prsd2] (->> strm1
            (value-expr:recur prsd1)
        )
        ]
        [strm2 prsd2]
    )
)
