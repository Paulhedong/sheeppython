#!/usr/bin/python
# -*- coding: utf-8 -*-
"""cache devices from db to memcache 

Overview
========

This python is to read devices info from db and put into memcache.
1. read from mad_device and put to memcache <appkey>-<utdid>: String(create_time)
2. read from mad_device_msg and put to memcache pulledmsg-<utdid>:string(msgids)

This python is runned every 2 hours repeatly

"""
import torndb
import time
from tornado.options import define, options
import memcache
import sys
define("mysql_host", default="127.0.0.1:3306", help="blog database host")
define("mysql_database", default="mad", help="blog database name")
define("mysql_user", default="mobilead", help="blog database user")
define("mysql_password", default="Mobad2016!", help="blog database password")


print "[%s] start " %time.ctime()
db = torndb.Connection(
            host=options.mysql_host, database=options.mysql_database,
            user=options.mysql_user, password=options.mysql_password)

mc = memcache.Client(['127.0.0.1:11211'])
start = 0
offset = 10000
sql = "SELECT id,utdid,appkey,ctime from mad_device order by id asc limit %s offset %s " 
count = 10000

while count >0 :
    device_list = db.query(sql,offset,start)
    count = len(device_list) 
    for dev in device_list:
        key = str(dev['appkey']+'-'+dev['utdid'])
        val = str(dev['ctime'])
        try:
       	    ret = mc.set(key,val,0)
        except Exception, e:
            print key
            raise e

    start = start + offset

print "mad_device end: [%s] - %s " %(time.ctime(),start)


start = 0
offset = 10000
sql = "SELECT id,utdid,msgids,last_interval_time from mad_device_msg order by id asc limit %s offset %s " 
count = 10000

while count >0 :
    device_list = db.query(sql,offset,start)
    count = len(device_list) 
    for dev in device_list:
        key = "pulledmsg-" + str(dev['utdid'])
        val = str(dev['msgids'])
        if dev.has_key('last_interval_time') and dev['last_interval_time'] is not None:
            val = val+"|"+str(dev['last_interval_time'])
        try:
       	    ret = mc.set(key,val,0)
        except Exception, e:
            print key
            raise e

    start = start + offset

print "mad_device_msg end: [%s] - %s " %(time.ctime(),start)

