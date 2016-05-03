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
            code = os.system(cmd)
            if code!=0:
                logErr(localDir,yestoday,0,cmd)
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
                code = os.system(cmd)
                if code !=0:
                    logErr(localDir,yestoday,1,cmd)
                    continue
                cmd = "hdfs dfs -mkdir -p "+impalaDir
                code = os.system(cmd)
                if code!=0:
                    logErr(localDir,yestoday,2,cmd)
                    continue
                cmd = "hdfs dfs -rm -r "+impalaDir+"/"+fileName
                #if code!=0:
                #    logErr(localDir,yestoday,3,cmd)
                #    continue
                cmd = "hdfs dfs -put "+localFile+" "+impalaDir
                code = os.system(cmd)
                if code!=0:
                    logErr(localDir,yestoday,4,cmd)
                    continue
                cmd = "hdfs dfs -chown -R impala:impala "+impalaDir
                if code!=0:
                    logErr(localDir,yestoday,5,cmd)
                    continue
                tmpTable = game+".zh_"+logType+"_tmp"
                table = game+".zh_"+logType
                cmd = "impala-shell -i slave1 --query=\"alter table "+tmpTable+" add partition (pdate='"+yestoday+"')\""
                code = os.system(cmd)
                if code !=0:
                    logErr(localDir,yestoday,6,cmd)
                    continue
                cmd = "impala-shell -i slave1 --query=\"load data inpath '"+impalaDir+"/"+fileName+"' into table "+tmpTable+" PARTITION (pdate='"+yestoday+"') \""
                code = os.system(cmd)
                if code !=0:
                    logErr(localDir,yestoday,7,cmd)
                    continue
                cmd = "impala-shell -i slave1 --query=\"INSERT INTO "+table+" PARTITION(pdate) SELECT * FROM "+tmpTable+"\""
                code = os.system(cmd)
                if code !=0:
                    logErr(localDir,yestoday,8,cmd)
                    continue
                cmd = "impala-shell -i slave1 --query=\"TRUNCATE TABLE "+tmpTable+"\""
                code = os.system(cmd)
                if code !=0:
                    logErr(localDir,yestoday,9,cmd)
                    continue
                #cmd = "hdfs dfs -rm -r "+impalaDir+"/"+fileName
                #code = os.system(cmd)
                #if code !=0:
                #    logErr(localDir,yestoday,10,cmd)
                #    continue
                cmd = "hdfs dfs -rm -r "+logTypeDir+"/"+yestoday
                code = os.system(cmd)
                if code !=0:
                    logErr(localDir,yestoday,11,cmd)
                    continue



def logErr(logDir,fileName,step,cmd):
    fileName = logDir+"/err_"+fileName+".log"
    output = open(fileName, 'w+')
    output.write("process:%s,cmd:%s" %(step,cmd))
#with hdfs.open(logTypeDir) as f:
#    for line in f:
#        print line


#logTypeDir = "`hdfs dfs -ls /user/flume/Games/%s/%s`" %(game,logType)
#dirs = popen(logTypeDir,'r').read()
#print dirs


#cat = subprocess.Popen(["hadoop", "fs", "-cat", "/path/to/myfile"], stdout=subprocess.PIPE)
#for line in cat.stdout:
#    print line

#files=os.popen('ls /','r').read()
#print files

#os.system('echo $JAVA_HOME')
mergeFiles()
#a=os.system('`impala-shell -i slave1 --query="load data inpath /a into table a"`')
#    print a

#user=os.popen(cmd,'r').read()