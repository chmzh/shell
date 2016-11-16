#!/bin/python
#coding=utf-8
import datetime
games = {'log_test1':['360','qq']}
logTypes = ['userreginfo','userlogininfo','userlogoutinfo','serverselinfo','rolecreateinfo','rolelogininfo','rolelvupinfo','rolecancelinfo','payrequestinfo','paysucinfo','rewardinfo','consumeinfo','missioninfo']

def today():
    now = datetime.date.today()
    return timeFormat(now,"%Y-%m-%d")

def yestoday():
    now = datetime.datetime.now()
    yestoday = now + datetime.timedelta(days=-1)
    return timeFormat(yestoday,"%Y-%m-%d")


def timeFormat(date,format1):
    return date.strftime(format1)
