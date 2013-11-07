#!/usr/bin

# clean hive env

$shark -h $stage_spark_master -p $stage_spark_port -e 'show tables'>table.output

grep 'tn_' table.output > table.list

while read line
do
tbl="drop table $line";
echo $tbl;
$shark -h $stage_spark_master -p $stage_spark_port -e "$tbl";
done < ./table.list

rm table.list table.output

if [ $? -eq 0 ];then
	echo "clear up hive tables!"
else
	echo "can't clear up hive tables!"
fi 
