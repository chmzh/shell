#/bin/sh

games=(game1 game2)
logType=(log1 log2)
path=/
today=`date "+%Y-%m-%d"`
#执行80分钟前建立的文件
#pre_hour=`date -d "- 65 minutes" "+%H"`
yestoday=`date -d "- 180 minutes" "+%Y-%m-%d"`