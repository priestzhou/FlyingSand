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

(defn- one-escapable-char []
    (prs/choice
        (prs/chain (prs/expect-char \') (prs/expect-char \'))
        (prs/expect-char-if #(and (not= % :eof) (not= % \')))
    )
)

(defliteral national-string-literal :national-string-literal
    (string-literal
        (expect-char-ignore-case \N)
        (prs/many (one-escapable-char))
    )
)

(defliteral national-string-literal :national-string-literal
    (string-literal
        (expect-char-ignore-case \N)
        (prs/many (one-escapable-char))
    )
)

; this is not completely compliant to SQL-92 for lack of leading charactor set spec
(defliteral character-string-literal :character-string-literal
    (string-literal
        (prs/foresee (prs/expect-char \'))
        (prs/many (one-escapable-char))
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
    "ADD" "ALL" "ALLOCATE" "ALTER" "AND" "ANY" "ARE" "ARRAY" "AS"
    "ASENSITIVE" "ASYMMETRIC" "AT" "ATOMIC" "AUTHORIZATION"

    "BEGIN" "BETWEEN" "BIGINT" "BINARY" "BLOB" "BOOLEAN" "BOTH" "BY"

    "CALL" "CALLED" "CASCADED" "CASE" "CAST" "CHAR" "CHARACTER" "CHECK" "CLOB"
    "CLOSE" "COLLATE" "COLUMN" "COMMIT" "CONNECT" "CONSTRAINT" "CONTINUE"
    "CORRESPONDING" "CREATE" "CROSS" "CUBE" "CURRENT" "CURRENT_DATE"
    "CURRENT_DEFAULT_TRANSFORM_GROUP" "CURRENT_PATH" "CURRENT_ROLE" "CURRENT_TIME"
    "CURRENT_TIMESTAMP" "CURRENT_TRANSFORM_GROUP_FOR_TYPE" "CURRENT_USER"
    "CURSOR" "CYCLE"

    "DATE" "DAY" "DEALLOCATE" "DEC" "DECIMAL" "DECLARE" "DEFAULT" "DELETE"
    "DEREF" "DESCRIBE" "DETERMINISTIC" "DISCONNECT" "DISTINCT" "DOUBLE" "DROP"
    "DYNAMIC"

    "EACH" "ELEMENT" "ELSE" "END" "END-EXEC" "ESCAPE" "EXCEPT" "EXEC" "EXECUTE"
    "EXISTS" "EXTERNAL"

    "FALSE" "FETCH" "FILTER" "FLOAT" "FOR" "FOREIGN" "FREE" "FROM" "FULL" "FUNCTION"

    "GET" "GLOBAL" "GRANT" "GROUP" "GROUPING"

    "HAVING" "HOLD" "HOUR"

    "IDENTITY" "IMMEDIATE" "IN" "INDICATOR" "INNER" "INOUT" "INPUT" "INSENSITIVE"
    "INSERT" "INT" "INTEGER" "INTERSECT" "INTERVAL" "INTO" "IS" "ISOLATION"

    "JOIN"

    "LANGUAGE" "LARGE" "LATERAL" "LEADING" "LEFT" "LIKE" "LOCAL" "LOCALTIME"
    "LOCALTIMESTAMP"
    "LIMIT" ; MySQL & Hive extension; not in SQL standards

    "MATCH" "MEMBER" "MERGE" "METHOD" "MINUTE" "MODIFIES" "MODULE" "MONTH"
    "MULTISET"

    "NATIONAL" "NATURAL" "NCHAR" "NCLOB" "NEW" "NO" "NONE" "NOT" "NULL" "NUMERIC"

    "OF" "OLD" "ON" "ONLY" "OPEN" "OR" "ORDER" "OUT" "OUTER" "OUTPUT" "OVER" 
    "OVERLAPS"

    "PARAMETER" "PARTITION" "PRECISION" "PREPARE" "PRIMARY" "PROCEDURE"

    "RANGE" "READS" "REAL" "RECURSIVE" "REF" "REFERENCES" "REFERENCING"
    "REGR_AVGX" "REGR_AVGY" "REGR_COUNT" "REGR_INTERCEPT" "REGR_R2" "REGR_SLOPE"
    "REGR_SXX" "REGR_SXY" "REGR_SYY" "RELEASE" "RESULT" "RETURN" "RETURNS"
    "REVOKE" "RIGHT" "ROLLBACK" "ROLLUP" "ROW" "ROWS"

    "SAVEPOINT" "SCROLL" "SEARCH" "SECOND" "SELECT" "SENSITIVE" "SESSION_USER"
    "SET" "SIMILAR" "SMALLINT" "SOME" "SPECIFIC" "SPECIFICTYPE" "SQL" "SQLEXCEPTION"
    "SQLSTATE" "SQLWARNING" "START" "STATIC" "SUBMULTISET" "SYMMETRIC" "SYSTEM"
    "SYSTEM_USER"

    "TABLE" "THEN" "TIME" "TIMESTAMP" "TIMEZONE_HOUR" "TIMEZONE_MINUTE" "TO"
    "TRAILING" "TRANSLATION" "TREAT" "TRIGGER" "TRUE"

    "UESCAPE" "UNION" "UNIQUE" "UNKNOWN" "UNNEST" "UPDATE" "UPPER" "USER" "USING"

    "VALUE" "VALUES" "VAR_POP" "VAR_SAMP" "VARCHAR" "VARYING"

    "WHEN" "WHENEVER" "WHERE" "WIDTH_BUCKET" "WINDOW" "WITH" "WITHIN" "WITHOUT"

    "YEAR"
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
                (partial quoted-identifier \")
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

(defn table-correlation [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                correlation
                (prs/choice*
                    second (prs/chain
                        blank*
                        paren-column-list
                    )
                    nil
                )
            ))
        )
        [cor col] prsd
        res (assoc cor :type :table-correlation)
        res (if-not col res (assoc res :column-list (:value col)))
        ]
        [strm res]
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
                        table-correlation
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
                table-correlation
            ))
        )
        [subquery _ as] prsd
        ]
        [strm (-> as (dissoc :type) (assoc :query subquery))]
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
                    identifier
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

(defn- distinct-count [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (expect-string-ignore-case "COUNT")
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
        args (->> prsd (last) (last))
        ]
        (if-not (<= 1 (count args) 2)
            (prs/gen-ISE stream "COUNT(DISTINCT accepts 1 or 2 args")
        )
        [strm {:type :distinct-count, :args args}]
    )
)

(defn- function-call [stream]
    (let [
        [strm prsd] (->> stream
            ((prs/chain
                (prs/choice*
                    (constantly :power) (expect-string-ignore-case "POW")
                )
                blank*
                (paren
                    (prs/separated-list
                        value-expr
                        (prs/chain
                            blank*
                            (prs/expect-char \,)
                            blank*
                        )
                    )
                )
            ))
        )
        [f _ args] prsd
        ]
        (if-not (= 2 (count args))
            (prs/gen-ISE stream "POW requires 2 arguments")
        )
        [strm {:type :func-call, :func f, :args args}]
    )
)

(defn- simple-expr:term [stream]
    (->> stream
        ((prs/choice
            (paren query)
            binary-operator
            exists-operator
            cast-operator
            case-operator
            distinct-count
            function-call
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
