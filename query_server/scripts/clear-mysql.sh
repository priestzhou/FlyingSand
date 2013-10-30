#!/usr/bin

# clean mysql env
mysql -P$stage_mysql_port -h $stage_mysql_host -u$stage_mysql_user -p$stage_mysql_pwd $stage_mysql_db < $stage_sql 

if [ $? -eq 0 ];then
        echo "mysql db is re-initialized!"
else
        echo "mysql db initialize failed!"
        exit 1
fi
