# -*- coding: utf-8 -*-
import os
import sys
import re
import datetime
import time
import shutil
import json
import torndb
from tornado.options import define, options
import pinyin
import hashlib
from random import Random

define("mysql_host_gcd", default="172.16.50.150:5506", help="blog database host")
define("mysql_database_gcd", default="sheepshead", help="blog database name")
define("mysql_user_gcd", default="gyf", help="blog database user")
define("mysql_password_gcd", default="vFVUTYjt5apY65EL", help="blog database password")

define("mysql_host_crd", default="172.16.50.150:5507", help="blog database host")
define("mysql_database_crd", default="sheepshead", help="blog database name")
define("mysql_user_crd", default="gyf", help="blog database user")
define("mysql_password_crd", default="B4oZsRfbf43DLFMh", help="blog database password")

define("mysql_host_local", default="127.0.0.1:3306", help="blog database host")
define("mysql_database_local", default="sheepshead", help="blog database name")
define("mysql_user_local", default="root", help="blog database user")
define("mysql_password_local", default="root", help="blog database password")

db_gcd = torndb.Connection(
        host=options.mysql_host_gcd, database=options.mysql_database_gcd,
        user=options.mysql_user_gcd, password=options.mysql_password_gcd)

db_crd = torndb.Connection(
        host=options.mysql_host_crd, database=options.mysql_database_crd,
        user=options.mysql_user_crd, password=options.mysql_password_crd)

db_local = torndb.Connection(
        host=options.mysql_host_local, database=options.mysql_database_local,
        user=options.mysql_user_local, password=options.mysql_password_local)

now = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
nowdate = str(time.strftime('%Y-%m-%d', time.localtime()))


def initDb():
    # reload(sys)
    # sys.setdefaultencoding('utf-8')
    
    db_local.execute('truncate table config_task_type_price')
    db_local.execute('truncate table config_task_type')

    db_local.execute('truncate table sys_organization')
    db_local.execute('truncate table sys_role')
    db_local.execute('truncate table sys_user')
    db_local.execute('truncate table sys_user_role')
    db_local.execute('truncate table sys_role_resource')
    db_local.execute('truncate table sys_user_bd')
    db_local.execute('truncate table sys_user_appinfo')

    db_local.execute('truncate table app_info')
    db_local.execute('truncate table app_info_task_type')
    db_local.execute('truncate table app_info_task_type_price')

    db_local.execute('truncate table app_info_task_statis_cycle')
    db_local.execute('truncate table app_info_task_statis_day')
    db_local.execute('truncate table app_info_task_statis_month')
    db_local.execute('truncate table taskno_record')


    # sys_organization
    db_local.execute('insert into sys_organization(org_name, org_code, ctime, utime, create_user_id, modify_user_id) \
                    values("云蜂科技","IbeeSaas","%s", "%s", 0, 0)' % (now,now))
    db_local.execute('insert into sys_organization(org_name, org_code, ctime, utime, create_user_id, modify_user_id) \
                    values("南京测试","NjTest","%s", "%s", 0, 0)' % (now,now))

    # sys_role
    db_local.execute('insert into sys_role(id, role_name, role_code, create_user_id, modify_user_id) values(1, "管理员", "admin", 0, 0)')
    db_local.execute('insert into sys_role(id, role_name, role_code, create_user_id, modify_user_id) values(2, "销售", "sales", 0, 0)')
    db_local.execute('insert into sys_role(id, role_name, role_code, create_user_id, modify_user_id) values(3, "客户", "customer", 0, 0)')

    # sys_role_resource
    for i in range(1,22):
        db_local.execute('insert into sys_role_resource(role_id, resource_id, create_user_id) values(1, %s, 0)' % i)

    for i in range(1,14):
        db_local.execute('insert into sys_role_resource(role_id, resource_id, create_user_id) values(2, %s, 0)' % i)

    for i in range(1,4):
        db_local.execute('insert into sys_role_resource(role_id, resource_id, create_user_id) values(3, %s, 0)' % i)
    for i in range(5,7):
        db_local.execute('insert into sys_role_resource(role_id, resource_id, create_user_id) values(3, %s, 0)' % i)
    for i in range(11,13):
        db_local.execute('insert into sys_role_resource(role_id, resource_id, create_user_id) values(3, %s, 0)' % i)

    # sys_user
    originpwd,puresalt,pwd = genMd5Pwd('bjadmin')
    print 'bjadmin:'+originpwd+':'+puresalt+':'+pwd
    db_local.execute('insert into sys_user(login_name, passwd, org_id, user_name, create_user_id,  modify_user_id, salt) \
                     values("bjadmin", "%s", 1, "北京管理员",0, 0, "%s")' % (pwd, puresalt))

    originpwd,puresalt,pwd = genMd5Pwd('njadmin')
    print 'njadmin:'+originpwd+':'+puresalt+':'+pwd
    db_local.execute('insert into sys_user(login_name, passwd, org_id, user_name, create_user_id,  modify_user_id, salt) \
                     values("njadmin", "%s", 1, "南京管理员",0, 0, "%s")' % (pwd, puresalt))

    # sys_user_role
    db_local.execute('insert into sys_user_role(user_id, role_id, create_user_id) values(1, 1, 0)')
    db_local.execute('insert into sys_user_role(user_id, role_id, create_user_id) values(2, 1, 0)')


def start():
    reload(sys)
    sys.setdefaultencoding('utf-8')

    # config_task_type,config_task_type_price
    sql_gcd_tasktypes = 'select * from task_types order by id asc'
    sql_config_task_type = 'insert into config_task_type(ctime, utime, task_type, free_count, task_type_desc, usable)\
                            values("%s", "%s", "%s", %s, "%s", %s)'
    sql_config_task_type_price = 'insert into config_task_type_price(ctime, utime, task_type, upper_limit, unit_price)\
                            values("%s", "%s", "%s", %s, %s)'
    task_types = db_gcd.query(sql_gcd_tasktypes)
    for task_type in task_types:
    	db_local.execute(sql_config_task_type % (now, now, task_type['task_type'], 100, task_type['task_name'], task_type['usable']))
    	db_local.execute(sql_config_task_type_price % (now, now, task_type['task_type'], -1, 100))

    # sys_organization
    sql_gcd_appinfo = 'select app_key, merchants_name from app_info order by id asc'
    sql_sys_org = 'insert into sys_organization(org_name, org_code, ctime, utime, create_user_id, modify_user_id) \
                    values("%s","%s","%s","%s", 0, 0)'

    app_keys = set()

    orgs = db_crd.query(sql_gcd_appinfo)
    for org in orgs:
        if org['app_key'] not in app_keys and ("测试" not in org['merchants_name'] and \
            'demo' not in org['merchants_name'] and "专用" not in org['merchants_name']):
            db_local.execute(sql_sys_org % (org['merchants_name'], pinyin.get(org['merchants_name'], format="strip"), now,now))
            app_keys.add(org['app_key'])

    orgs = db_gcd.query(sql_gcd_appinfo)
    for org in orgs:
        if org['app_key'] not in app_keys and ("测试" not in org['merchants_name'] and \
            'demo' not in org['merchants_name'] and "专用" not in org['merchants_name']):
            db_local.execute(sql_sys_org % (org['merchants_name'], pinyin.get(org['merchants_name'], format="strip"), now,now))
            app_keys.add(org['app_key'])

    # app_info, app_info_task_type, app_info_task_type_price
    app_keys.clear()
    sql_old_appinfo = 'select * from app_info order by id asc'
    sql_old_apptasks = 'select * from app_task where app_key = "%s"'
    sql_new_insertapp = 'insert into app_info(app_key,ak,sk,merchants_name,channel,biz_type,use_cache,cycle,start_month,create_user_id,user_id,usable) \
                        values("%s","%s","%s","%s","%s","%s",%s,1,"%s",0,1,%s)'
    sql_new_insertapptask = 'insert into app_info_task_type(app_key,task_type,cache_charge,free_count_remaining,cur_status,create_user_id,usable) \
                            values("%s","%s",%s,%s,%s,0,%s)'                        
    sql_new_insertapptaskprice = 'insert into app_info_task_type_price(app_key,task_type,upper_limit,unit_price,create_user_id) \
                            values("%s","%s",%s,%s,0)'                        
    sql_old_appkeys = 'select distinct app_key from app_info'
    sql_crd_7_appkeys = 'select distinct app_key from charge_order where create_time >= "2017-7-1" and create_time<"2017-8-1"'
    sql_crd_8_appkeys = 'select distinct app_key from charge_order where create_time >= "2017-8-1" and create_time<"2017-9-1"'

    # get all appkeys info from crd
    charged_appkeys = set()
    charged_db_appkeys = db_crd.query(sql_old_appkeys)
    for charged_db_appkey in charged_db_appkeys:
        charged_appkeys.add(charged_db_appkey['app_key'])

    # get all app_keys from charge_order which called in 2017.7
    charged_7_appkeys = set()
    charged_db_7_appkeys = db_crd.query(sql_crd_7_appkeys)
    for charged_db_7_appkey in charged_db_7_appkeys:
        charged_7_appkeys.add(charged_db_7_appkey['app_key'])

    # get all app_keys from charge_order which called in 2017.8
    charged_8_appkeys = set()
    charged_db_8_appkeys = db_crd.query(sql_crd_8_appkeys)
    for charged_db_8_appkey in charged_db_8_appkeys:
        charged_8_appkeys.add(charged_db_8_appkey['app_key'])     

    # migrate appinfo from crd to local, all appinfo in crd are considered as charged appkeys, should have start_month and price.
    # the price has one type: ￥1
    appinfos = db_crd.query(sql_old_appinfo)
    for appinfo in appinfos:
        start_month = "2017-8-1"
        if appinfo['app_key'] in charged_7_appkeys:
            start_month = "2017-7-1"
        db_local.execute(sql_new_insertapp % (appinfo['app_key'],appinfo['ak'],appinfo['sk'],appinfo['merchants_name'],appinfo['channel'],\
                        appinfo['biz_type'],appinfo['use_cache'],start_month,appinfo['usable']))
        app_keys.add(appinfo['app_key'])
        apptasks = db_crd.query(sql_old_apptasks % appinfo['app_key'])
        for apptask in apptasks:
            db_local.execute(sql_new_insertapptask % (appinfo['app_key'],apptask['task_type'],apptask['cache_charge'],apptask['max_size'],\
                            1,apptask['usable']))
            db_local.execute(sql_new_insertapptaskprice % (appinfo['app_key'],apptask['task_type'],-1,100))

    # migrate appinfo from gcd to local, all appinfo in gcd but not in crd are considered as testing appkeys, should have no start_month.
    appinfos = db_gcd.query(sql_old_appinfo)
    for appinfo in appinfos:
        start_month = "2017-1-1"
        if appinfo['app_key'] not in app_keys:
            db_local.execute(sql_new_insertapp % (appinfo['app_key'],appinfo['ak'],appinfo['sk'],appinfo['merchants_name'],appinfo['channel'],\
                        appinfo['biz_type'],appinfo['use_cache'],start_month,appinfo['usable']))
            app_keys.add(appinfo['app_key'])
            apptasks = db_gcd.query(sql_old_apptasks % appinfo['app_key'])
            for apptask in apptasks:
                db_local.execute(sql_new_insertapptask % (appinfo['app_key'],apptask['task_type'],apptask['cache_charge'],apptask['max_size'],\
                                0,apptask['usable']))
                db_local.execute(sql_new_insertapptaskprice % (appinfo['app_key'],apptask['task_type'],-1,100))

    # migrate three statis tables:app_info_task_statis_cycle,app_info_task_statis_day,app_info_task_statis_month
    # migrate taskno_record

    statis_day = dict() # {"appkey|tasktype|date":{"appkey":"", "tasktype":"", "date":"", "ctime":"", "utime":"", "call_count":""}}
    statis_month = dict() # {"appkey|tasktype|month":{"appkey":"", "tasktype":"", "date":"", "ctime":"", "utime":"", "call_count":""}}}
    statis_cycle = dict() # {"appkey|tasktype|startmonth":{"appkey":"", "tasktype":"", "date":"", "ctime":"", "utime":"", "call_count":""}}}

    # create org and user account based on 

    # start = 0
    # limit = 10000
    # sql_old_charge_orders = "select * from charge_order order by create_time_stamp asc limit %s offset %s " 
    # count = 10000

    # while count >0 :
    #     charge_orders = db_gcd.query(sql_old_charge_orders,limit,start)
    #     count = len(charge_orders) 
    #     for charge_order in charge_orders:
            

    #     start = start + limit

def genMd5Pwd(username=''):
    puresalt = create_salt(32)
    salt = username+puresalt
    pwd = create_salt(6)

    m2 = hashlib.md5()   
    m2.update(salt)
    m2.update(pwd)
    m3 = hashlib.md5()   
    m3.update(m2.digest())
    finalpwd = m3.hexdigest() 
    return pwd,puresalt,finalpwd  

def create_salt(length = 6):  
    salt = ''  
    chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'  
    len_chars = len(chars) - 1  
    random = Random()  
    for i in xrange(length):  
        # 每次从chars中随机取一位  
        salt += chars[random.randint(0, len_chars)]  
    return salt 

if __name__=='__main__':
    initDb()
    start()

