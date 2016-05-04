#!/bin/python
#coding=utf-8
import common
import sys
import os
from os import popen
import subprocess
#import pydoop.hdfs as hdfs

#print common.games
#print common.today()
#print common.yestoday()

subprocess.call(["source /etc/profile"],shell=True)
yestoday = common.yestoday()
#yestoday = "2016-04-21"
def mergeFiles():
    for game in common.games:
        for logType in common.logTypes:
            cmd = ""
            logTypeDir = "/user/flume/Games/"+game+"/"+logType
            impalaDir = "/user/impala/Games/"+game+"/"+logType
            fileName = yestoday+".log"
            
            localDir = "/opt/Games/"+game+"/"+logType
            localFile = localDir+"/"+fileName
            #print localDir
            cmd = "mkdir -p "+localDir
            code = subprocess.call(cmd,shell=True)
            if code!=0:
                logErr(game,logType,yestoday,0,cmd)
                continue
            #print logTypeDir
            p1 = subprocess.Popen(["hdfs", "dfs", "-ls", logTypeDir], stdout=subprocess.PIPE)
            p2 = subprocess.Popen(["grep",yestoday],stdin=p1.stdout,stdout=subprocess.PIPE)
            p3 = subprocess.Popen(["awk '{print $8}'"],stdin=p2.stdout,stdout=subprocess.PIPE,shell=True)
            for dir1 in p3.stdout:
                dir1 = dir1.strip('\n')
                cmd = "hdfs dfs -getmerge "+dir1+" "+localFile
                #cmd1 = "merge cmd :"+cmd
                #print cmd
                code = subprocess.call(cmd,shell=True)
                if code !=0:
                    logErr(game,logType,yestoday,1,cmd)
                    continue
                cmd = "hdfs dfs -mkdir -p "+impalaDir
                code = subprocess.call(cmd,shell=True)
                if code!=0:
                    logErr(game,logType,yestoday,2,cmd)
                    continue
                cmd = "hdfs dfs -rm -r "+impalaDir+"/"+fileName
                #if code!=0:
                #    logErr(localDir,yestoday,3,cmd)
                #    continue
                cmd = "hdfs dfs -put "+localFile+" "+impalaDir
                code = subprocess.call(cmd,shell=True)
                if code!=0:
                    logErr(game,logType,yestoday,4,cmd)
                    continue
                cmd = "hdfs dfs -chown -R impala:impala "+impalaDir
                if code!=0:
                    logErr(game,logType,yestoday,5,cmd)
                    continue
                tmpTable = game+".zh_"+logType+"_tmp"
                table = game+".zh_"+logType
                cmd = "impala-shell -i slave1 --query=\"alter table "+tmpTable+" add partition (pdate='"+yestoday+"')\""
                code = subprocess.call(cmd,shell=True)
                if code !=0:
                    logErr(game,logType,yestoday,6,cmd)
                    continue
                cmd = "impala-shell -i slave1 --query=\"load data inpath '"+impalaDir+"/"+fileName+"' into table "+tmpTable+" PARTITION (pdate='"+yestoday+"') \""
                code = subprocess.call(cmd,shell=True)
                if code !=0:
                    logErr(game,logType,yestoday,7,cmd)
                    continue
                cmd = "impala-shell -i slave1 --query=\"INSERT INTO "+table+" PARTITION(pdate) SELECT * FROM "+tmpTable+"\""
                code = subprocess.call(cmd,shell=True)
                if code !=0:
                    logErr(game,logType,yestoday,8,cmd)
                    continue
                cmd = "impala-shell -i slave1 --query=\"TRUNCATE TABLE "+tmpTable+"\""
                code = subprocess.call(cmd,shell=True)
                if code !=0:
                    logErr(game,logType,yestoday,9,cmd)
                    continue
                #cmd = "hdfs dfs -rm -r "+impalaDir+"/"+fileName
                #code = os.system(cmd)
                #if code !=0:
                #    logErr(game,logType,yestoday,10,cmd)
                #    continue
                cmd = "rm -rf "+localDir+"/*"
                code = subprocess.call(cmd,shell=True)
                if code !=0:
                    logErr(game,logType,yestoday,11,cmd)
                    continue
                
                cmd = "hdfs dfs -rm -r "+logTypeDir+"/"+yestoday
                code = subprocess.call(cmd,shell=True)
                if code !=0:
                    logErr(game,logType,yestoday,12,cmd)
                    continue




def logErr(game,logType,yestoday,step,cmd):
    errLogDir = "/opt/cndw/shell/errs"
    os.system("mkdir -p "+errLogDir)
    fileName = errLogDir+"/err_"+yestoday+".log"
    output = open(fileName, 'a')
    output.write("game:%s,logType:%s,process:%s,cmd:%s\r\n" %(game,logType,step,cmd))
    output.close()
mergeFiles()
#logErr("game1","log1","2016-05-04",1,"cmd")