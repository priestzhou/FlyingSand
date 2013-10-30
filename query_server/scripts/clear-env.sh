#!/usr/bin

export shark=/home/admin/shark-0.8.0-bin-cdh4/shark-0.8.0/bin/shark
export stage_spark_master=192.168.9.100
export stage_spark_port=10000
export stage_mysql_port=3306
export stage_mysql_host=192.168.9.101
export stage_mysql_db=meta_stage
export stage_mysql_user=root
export stage_mysql_pwd=fs123
export stage_sql=./9sea_stage.sql

# clear mysql env

sh clear-mysql.sh

# clear up hive env

sh clear-hive.sh
