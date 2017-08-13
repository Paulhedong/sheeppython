#!/usr/bin/python
# -*- coding: utf-8 -*-
#Usage: back the max_num and cur_num from mt_job_num_log
"""backup the max_num and cur_num from mad_msg to mad_msg_num_log 
   backup mad_msg_multi_pull_count to mad_msg_multi_pull_count_log

Overview
========

This python is to read msg max_num and cur_num from mad_msg and record to mad_msg_num_log.
This python is to read msg from mad_msg_multi_pull_count and record to mad_msg_multi_pull_count_log.

This python is runned at 00:01 every day repeatly

"""
import torndb
import json
import datetime
import time
from tornado import httpclient
from tornado.options import define, options
define("mysql_host", default="127.0.0.1:3306", help="blog database host")
define("mysql_database", default="mad", help="blog database name")
define("mysql_user", default="mobilead", help="blog database user")
define("mysql_password", default="Mobad2016!", help="blog database password")
db = torndb.Connection(
            host=options.mysql_host, database=options.mysql_database,
            user=options.mysql_user, password=options.mysql_password)
now = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
yesterday = datetime.date.today() - datetime.timedelta(days=1)
day_before_yesterday = yesterday - datetime.timedelta(days=1)


# mad_msg -> mad_msg_num_log
sql_msg = "select id, max_num, cur_num, status from mad_msg order by id"
sql_msg_num = 'select msg_id, log_date, max_num, cur_num from mad_msg_num_log where log_date = "%s" order by msg_id'
sql_insert = 'insert into mad_msg_num_log (msg_id, log_date, max_num, cur_num, max_delta, cur_delta, status, ctime, utime) \
             values (%s, "%s", %s, %s, %s, %s, %s, "%s", "%s")'


msg_list = db.query(sql_msg)
sql_exe = sql_msg_num % str(day_before_yesterday)
msg_num_list = db.query(sql_exe)
last_maxnum = dict()
last_curnum = dict()
for msg_num in msg_num_list:
    last_maxnum[msg_num['msg_id']] = msg_num['max_num']
    last_curnum[msg_num['msg_id']] = msg_num['cur_num']

for msg in msg_list:
    sql_exe = sql_insert % (msg['id'], str(yesterday), msg['max_num'], msg['cur_num'], \
              msg['max_num']-last_maxnum.get(msg['id'], 0), msg['cur_num']-last_curnum.get(msg['id'], 0), \
              msg['status'], now, now)
    db.execute(sql_exe)

print "mad_msg_num_log finished at:" + now

# mad_msg_multi_pull_count -> mad_msg_multi_pull_count_log
sql_msg_multi = "select msg_id, after_days, pull_count from mad_msg_multi_pull_count order by msg_id asc"
sql_msg_multi_log = 'select msg_id, log_date, after_days, pull_count from mad_msg_multi_pull_count_log where log_date = "%s" order by msg_id asc'
sql_insert = 'insert into mad_msg_multi_pull_count_log (ctime, utime, msg_id, log_date, after_days, pull_count, pull_count_delta) \
              values ("%s","%s",%s, "%s", %s, %s, %s)'

sql_exe = sql_msg_multi_log % str(day_before_yesterday)
msg_num_list = db.query(sql_exe)
last_pull_count = dict()
for msg_num in msg_num_list:
    msg_day = "%s-%s" % (msg_num['msg_id'], msg_num['after_days'])
    last_pull_count[msg_day] = msg_num['pull_count']

msg_list = db.query(sql_msg_multi)
for msg in msg_list:
    msg_day = "%s-%s" % (msg['msg_id'], msg['after_days'])
    sql_exe = sql_insert % (now, now, msg['msg_id'], str(yesterday), msg['after_days'], msg['pull_count'], \
              msg['pull_count']-last_pull_count.get(msg_day, 0))
    db.execute(sql_exe)

print "mad_msg_num_log finished at:" + now


