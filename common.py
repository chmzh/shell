#coding=utf-8
import datetime
games = ['game1', 'game2']
logTypes = ['log1','log2']

def today():
    now = datetime.date.today()
    return timeFormat(now,"%Y-%m-%d")

def yestoday():
    now = datetime.datetime.now()
    yestoday = now + datetime.timedelta(days=-1)
    return timeFormat(yestoday,"%Y-%m-%d")


def timeFormat(date,format1):
    return date.strftime(format1)