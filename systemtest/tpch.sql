 CREATE TABLE region (r_regionkey BIGINT,r_name STRING,r_comment STRING ) 
                ROW FORMAT DELIMITED FIELDS TERMINATED BY "|"; 

CREATE TABLE nation (n_nationkey  BIGINT,
                         n_name STRING,
                         n_regionkey BIGINT,
                         n_comment STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "|";

CREATE TABLE supplier (S_SUPPKEY BIGINT,
                           S_NAME STRING,
                           S_ADDRESS STRING,
                           S_NATIONKEY BIGINT,
                           S_PHONE STRING,
                           S_ACCTBAL DOUBLE,
                           S_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "|";

CREATE TABLE customer (C_CUSTKEY BIGINT,
                           C_NAME STRING,
                           C_ADDRESS STRING,
                           C_NATIONKEY BIGINT,
                           C_PHONE STRING,
                           C_ACCTBAL INT,
                           C_MKTSEGMENT STRING,
                           C_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "|";

CREATE TABLE orders (O_ORDERKEY BIGINT,
                        O_CUSTKEY BIGINT,
                        O_ORDERSTATUS STRING,
                        O_TOTALPRICE DOUBLE:,
                        O_ORDERDATE STRING,
                        O_ORDERPRIORITY STRING,
                        O_CLERK STRING,
                        O_SHIPPRIORITY BIGINT,
                        O_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "|";

CREATE TABLE part (P_PARTKEY BIGINT,
                       P_NAME STRING,
                       P_MFGR STRING,
                       P_BRAND STRING,
                       P_TYPE STRING,
                       P_SIZE BIGINT,
                       P_CONTAINER STRING,
                       P_RETAILPRICE DOUBLE,
                       P_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "|";

CREATE TABLE partsupp (PS_PARTKEY BIGINT,
                           PS_SUPPKEY BIGINT,
                           PS_AVAILQTY BIGINT,
                           PS_SUPPLYCOST DOUBLE,
                           PS_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "|";

CREATE TABLE lineitem (L_ORDERKEY  BIGINT,
                           L_PARTKEY BIGINT,
                           L_SUPPKEY BIGINT,
                           L_LINEINT  BIGINT,
                           L_QUANTITY INT,
                           L_EXTENDEDPRICE   DOUBLE,
                           L_DISCOUNT DOUBLE,
                           L_TAX  DOUBLE,
                           L_RETURNFLAG  STRING,
                           L_LINESTATUS STRING,
                           L_SHIPDATE  STRING,
                           L_COMMITDATE STRING,
                           L_RECEIPTDATE  STRING,
                           L_SHIPINSTRUCT  STRING,
                           L_SHIPMODE STRING,
                           L_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "|";
