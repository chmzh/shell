#bin/sh
source /etc/profile
source /opt/cndw/shell/constant.sh
#echo $tody
for game in ${games[@]}
do
for type1 in ${logType[@]}
do

#for dir in `hdfs dfs -ls /user/flume/Games/${game}/${type1} | egrep '([0-9]+)-([0-9]+)-([0-9]+)'`
for dir in `hdfs dfs -ls /user/flume/Games/${game}/${type1} | grep ${yestoday}`
do
echo $dir
f=${dir:0:1}
if [ $f == $path ]; then
#echo $dir
datedir="${dir##*/}"
pdate="${datedir##*/}"
logfile="$pdate.log"
localDir=/opt/Games/${game}/${type1}
mkdir -p $localDir
hdfs dfs -getmerge $dir $localDir/$logfile
hdfs dfs -mkdir -p /user/impala/Games/${game}/${type1}
hdfs dfs -rm -r /user/impala/Games/${game}/${type1}/${logfile}

hdfs dfs -put $localDir/$logfile /user/impala/Games/${game}/${type1}
#修改文件权限
hdfs dfs -chown -R impala:impala /user/impala/Games/${game}/${type1}

hdfs_file=/user/impala/Games/${game}/${type1}/${logfile}
#load 到 impala  TODO 需要判断 load 到哪个数据库，哪张表
tmp_table=${game}.zh_${type1}_tmp
table=${game}.zh_${type1}
`impala-shell -i slave1 --query="alter table ${tmp_table} add partition (pdate='${pdate}')"`
`impala-shell -i slave1 --query="load data inpath '${hdfs_file}' into table ${tmp_table} PARTITION (pdate='${pdate}')"`
`impala-shell -i slave1 --query="INSERT INTO ${table} partition(pdate) SELECT * FROM ${tmp_table}"`
`impala-shell -i slave1 --query="TRUNCATE TABLE ${tmp_table}"`
#移除文件
hdfs dfs -rm -r $dir
#if [ $?==0 ]; then
#	echo rmfile ok
#hdfs dfs -rm -r $file
#fi
fi
done
done
done