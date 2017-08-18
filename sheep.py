# -*- coding: utf-8 -*-
import hashlib
import sys
import time
from random import Random
import datetime
import calendar
import pinyin
import torndb
from tornado.options import define, options

define("mysql_host_gcd", default="172.16.50.150:5506", help="blog database host")
define("mysql_database_gcd", default="sheepshead", help="blog database name")
define("mysql_user_gcd", default="gyf", help="blog database user")
define("mysql_password_gcd", default="vFVUTYjt5apY65EL", help="blog database password")

define("mysql_host_crd", default="172.16.50.150:5507", help="blog database host")
define("mysql_database_crd", default="sheepshead", help="blog database name")
define("mysql_user_crd", default="gyf", help="blog database user")
define("mysql_password_crd", default="B4oZsRfbf43DLFMh", help="blog database password")

define("mysql_host_local", default="127.0.0.1:3306", help="blog database host")
define("mysql_database_local", default="sheepshead_new", help="blog database name")
define("mysql_user_local", default="root", help="blog database user")
define("mysql_password_local", default="root", help="blog database password")

db_gcd = torndb.Connection(
    host=options.mysql_host_gcd, database=options.mysql_database_gcd,
    user=options.mysql_user_gcd, password=options.mysql_password_gcd, time_zone="+8:00")

db_crd = torndb.Connection(
    host=options.mysql_host_crd, database=options.mysql_database_crd,
    user=options.mysql_user_crd, password=options.mysql_password_crd, time_zone="+8:00")

db_local = torndb.Connection(
    host=options.mysql_host_local, database=options.mysql_database_local,
    user=options.mysql_user_local, password=options.mysql_password_local, time_zone="+8:00")

now = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
nowdate = str(time.strftime('%Y-%m-%d', time.localtime()))


def initDb():
    # reload(sys)
    # sys.setdefaultencoding('utf-8')

    # db_local.execute('truncate table config_task_type_price')
    # db_local.execute('truncate table config_task_type')

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
    # db_local.execute('truncate table taskno_record')

    # sys_organization
    db_local.execute('insert into sys_organization(org_name, org_code, ctime, utime, create_user_id, modify_user_id) \
                    values("云蜂科技","IbeeSaas","%s", "%s", 0, 0)' % (now, now))
    db_local.execute('insert into sys_organization(org_name, org_code, ctime, utime, create_user_id, modify_user_id) \
                    values("南京测试","NjTest","%s", "%s", 0, 0)' % (now, now))

    # sys_role
    db_local.execute(
        'insert into sys_role(id, role_name, role_code, create_user_id, modify_user_id,ctime,utime) values(1, "管理员", "admin", 1, 1,"%s", "%s")' % (
            now, now))
    db_local.execute(
        'insert into sys_role(id, role_name, role_code, create_user_id, modify_user_id,ctime,utime) values(2, "销售", "sale", 1, 1,"%s", "%s")' % (
            now, now))
    db_local.execute(
        'insert into sys_role(id, role_name, role_code, create_user_id, modify_user_id,ctime,utime) values(3, "客户", "customer", 1, 1,"%s", "%s")' % (
            now, now))

    # sys_role_resource
    for i in range(1, 22):
        db_local.execute('insert into sys_role_resource(role_id, resource_id, create_user_id) values(1, %s, 0)' % i)

    for i in range(1, 14):
        db_local.execute('insert into sys_role_resource(role_id, resource_id, create_user_id) values(2, %s, 0)' % i)

    for i in range(1, 4):
        db_local.execute('insert into sys_role_resource(role_id, resource_id, create_user_id) values(3, %s, 0)' % i)
    for i in range(5, 7):
        db_local.execute('insert into sys_role_resource(role_id, resource_id, create_user_id) values(3, %s, 0)' % i)
    for i in range(11, 13):
        db_local.execute('insert into sys_role_resource(role_id, resource_id, create_user_id) values(3, %s, 0)' % i)

    # sys_user
    originpwd, puresalt, pwd = gen_md5_pwd('bjadmin')
    print 'bjadmin的密码:' + originpwd
    db_local.execute('insert into sys_user(login_name, passwd, org_id, user_name, create_user_id,  modify_user_id, salt,ctime,utime) \
                     values("bjadmin", "%s", 1, "北京管理员",0, 0, "%s","%s","%s")' % (pwd, puresalt, now, now))

    originpwd, puresalt, pwd = gen_md5_pwd('njadmin')
    print 'njadmin的密码:' + originpwd
    db_local.execute('insert into sys_user(login_name, passwd, org_id, user_name, create_user_id,  modify_user_id, salt,ctime,utime) \
                     values("njadmin", "%s", 2, "南京管理员",0, 0, "%s","%s","%s")' % (pwd, puresalt, now, now))

    # sys_user_role
    db_local.execute(
        'insert into sys_user_role(user_id, role_id, create_user_id,ctime) values(1, 1, 0,"%s")' % (
            now))
    db_local.execute(
        'insert into sys_user_role(user_id, role_id, create_user_id,ctime) values(2, 1, 0,"%s")' % (
            now))


def start():
    reload(sys)
    sys.setdefaultencoding('utf-8')
    app_keys = set()
    sql_old_appinfo = 'select * from app_info order by id asc'
    sql_old_apptasks = 'select * from app_task where app_key = "%s"'
    sql_new_insertorg = 'insert into sys_organization(org_name, org_code, ctime, utime, create_user_id, modify_user_id) \
                    values("%s","%s","%s","%s", 0, 0)'
    sql_new_insertsysuser = 'insert into sys_user(login_name, passwd, org_id, user_name, create_user_id, modify_user_id, salt,ctime,utime) \
                       values("%s","%s",%s,"%s", 1, 1,"%s","%s","%s")'

    sql_new_insertsysuserrole = 'insert into sys_user_role(user_id,role_id,ctime,create_user_id) values(%s,3,"%s",1)'
    sql_new_insertsysuserappinfo = 'insert into sys_user_appinfo(user_id,app_key,ctime,create_user_id) values(%s,"%s","%s",1)'

    sql_new_insertapp = 'insert into app_info(app_key,ak,sk,merchants_name,channel,biz_type,use_cache,cycle,start_month,create_user_id,user_id,usable,ctime,utime) \
                        values("%s","%s","%s","%s","%s","%s",%s,1,"%s",1,%s,%s,"%s","%s")'
    sql_new_insertapptask = 'insert into app_info_task_type(app_key,task_type,cache_charge,free_count_remaining,cur_status,create_user_id,usable,charge_time,ctime,utime) \
                            values("%s","%s",%s,%s,%s,1,%s,"%s","%s","%s")'
    sql_new_insertapptaskprice = 'insert into app_info_task_type_price(app_key,task_type,upper_limit,unit_price,create_user_id,ctime,utime) \
                            values("%s","%s",%s,%s,1,"%s","%s")'
    sql_old_chargeorder_firsttime = 'SELECT create_time FROM `charge_order` where app_key="%s" order by create_time limit 1';

    sql_old_chargeorder_type_firsttime = 'SELECT create_time FROM `charge_order` where app_key="%s" and task_type="%s" order by create_time limit 1';

    # the price has one type: ￥1
    # 先处理crd环境的用户，公司，appinfo，taskType，price
    appinfos = db_crd.query(sql_old_appinfo)
    for appinfo in appinfos:
        merchants_name = appinfo['merchants_name']
        pinyin_merchants_name = pinyin.get(appinfo['merchants_name'], format="strip")
        app_key = appinfo['app_key']
        start_month = db_crd.query(sql_old_chargeorder_firsttime % (app_key));
        # 获取charger_order，这个appKey 最早请求的date
        start_time = "1970-01-01"
        for i in start_month:
            start_time = getMonthStartDate(i['create_time'])
            # crd处理快加，蓝莓钱包的用户
        if app_key not in app_keys and ("快加" in merchants_name or '蓝莓' in merchants_name):
            # 插入机构表（排除云蜂，南京测试）
            org_id = db_local.execute_lastrowid(
                sql_new_insertorg % (merchants_name, pinyin_merchants_name, \
                                     now, now))
            # 获取随机密码
            originpwd, puresalt, pwd = gen_md5_pwd(pinyin_merchants_name)
            print merchants_name + '的登录名：' + pinyin_merchants_name + ',密码:' + originpwd
            # 插入用户表（排除云蜂，南京测试）
            user_id = db_local.execute_lastrowid(
                sql_new_insertsysuser % (pinyin_merchants_name, pwd, org_id, merchants_name, puresalt, now, now))
            # 插入用户角色表（排除云蜂，南京测试）
            db_local.execute(sql_new_insertsysuserrole % (user_id, now))
            db_local.execute(sql_new_insertsysuserappinfo % (user_id, app_key, now))
            # 插入app_info表（排除云蜂，南京测试）
            db_local.execute(sql_new_insertapp % (
                app_key, appinfo['ak'], appinfo['sk'], merchants_name, appinfo['channel'], \
                appinfo['biz_type'], appinfo['use_cache'], start_time, user_id, appinfo['usable'], now, now));

        # 处理云蜂的账号
        if app_key not in app_keys and ("北京测试" in merchants_name or 'demo' in merchants_name or "导流" in merchants_name):
            # 插入sys_user_appinfo表
            db_local.execute(sql_new_insertsysuserappinfo % (1, app_key, now))
            # 插入app_info表（云蜂数据）
            db_local.execute(sql_new_insertapp % (
                app_key, appinfo['ak'], appinfo['sk'], merchants_name, appinfo['channel'], \
                appinfo['biz_type'], appinfo['use_cache'], start_time, 1, appinfo['usable'], now, now));
        # 处理南京的账号
        if app_key not in app_keys and (
                            "南京测试" in merchants_name or '2C' in merchants_name or "autotest" in merchants_name):
            # 插入sys_user_appinfo表
            db_local.execute(sql_new_insertsysuserappinfo % (2, app_key, now))
            # 插入app_info表（南京数据）
            db_local.execute(sql_new_insertapp % (
                app_key, appinfo['ak'], appinfo['sk'], merchants_name, appinfo['channel'], \
                appinfo['biz_type'], appinfo['use_cache'], start_time, 2, appinfo['usable'], now, now));
        app_keys.add(app_key)
        # 根据appkey获取所有taskType
        apptasks = db_crd.query(sql_old_apptasks % (app_key))
        for apptask in apptasks:
            task_type = apptask['task_type']
            start_month = db_crd.query(sql_old_chargeorder_type_firsttime % (app_key, task_type))
            # 获取charger_order，这个appKey 最早请求的datetime
            start_time = "1972-01-01 00:00:00"
            curstatus = 0
            for i in start_month:
                start_time = i['create_time'].strftime('%Y-%m-%d %H:%M:%S')
                curstatus = 1
            db_local.execute(sql_new_insertapptask % (
                app_key, task_type, apptask['cache_charge'], apptask['max_size'], \
                curstatus, apptask['usable'], start_time, now, now))
            db_local.execute(sql_new_insertapptaskprice % (app_key, apptask['task_type'], -1, 100, now, now))

    # 开始处理gcd的用户，公司，appinfo，taskType，price
    appinfos = db_gcd.query(sql_old_appinfo)
    for appinfo in appinfos:
        merchants_name = appinfo['merchants_name']
        pinyin_merchants_name = pinyin.get(appinfo['merchants_name'], format="strip")
        app_key = appinfo['app_key']
        start_month = db_gcd.query(sql_old_chargeorder_firsttime % (app_key));
        # 获取charger_order，这个appKey 最早请求的date
        start_time = "1970-01-01"
        for i in start_month:
            start_time = getMonthStartDate(i['create_time'])
        # 处理gcd的用户（排除crd已经有的用户）
        if app_key not in app_keys and (
                            merchants_name != 'rrx' and '蓝莓' not in merchants_name and '快加' not in merchants_name):
            # 插入机构表（排除云蜂，南京测试）
            org_id = db_local.execute_lastrowid(
                sql_new_insertorg % (merchants_name, pinyin_merchants_name, \
                                     now, now))
            # 获取随机密码
            originpwd, puresalt, pwd = gen_md5_pwd(pinyin_merchants_name)
            print merchants_name + '的登录名：' + pinyin_merchants_name + ',密码:' + originpwd
            # 插入用户表（排除云蜂，南京测试）
            user_id = db_local.execute_lastrowid(
                sql_new_insertsysuser % (pinyin_merchants_name, pwd, org_id, merchants_name, puresalt, now, now))

            # 插入用户角色表（排除云蜂，南京测试）
            db_local.execute(sql_new_insertsysuserrole % (user_id, now))
            # 插入用户appinfo表（排除云蜂，南京测试）
            db_local.execute(sql_new_insertsysuserappinfo % (user_id, app_key, now))
            # 插入app_info表（排除云蜂，南京测试）
            db_local.execute(sql_new_insertapp % (
                app_key, appinfo['ak'], appinfo['sk'], merchants_name, appinfo['channel'], \
                appinfo['biz_type'], appinfo['use_cache'], start_time, user_id, appinfo['usable'], now, now));
            app_keys.add(app_key)
            # 根据appkey获取所有taskType
            apptasks = db_gcd.query(sql_old_apptasks % (app_key))
            for apptask in apptasks:
                task_type = apptask['task_type']
                start_month = db_gcd.query(sql_old_chargeorder_type_firsttime % (app_key, task_type))
                # 获取charger_order，这个appKey 最早请求的datetime
                start_time = "1970-01-02 00:00:01"
                for i in start_month:
                    start_time = i['create_time']
                db_local.execute(sql_new_insertapptask % (
                    app_key, task_type, apptask['cache_charge'], apptask['max_size'], \
                    0, apptask['usable'], start_time, now, now))
                db_local.execute(sql_new_insertapptaskprice % (app_key, apptask['task_type'], -1, 100, now, now))


# 将crd，gcd的环境charge_order 记录放到taskno_record表
def migrateRecords():
    sql_old_charge_orders = "select * from charge_order order by create_time_stamp asc limit %s offset %s "
    sql_new_insert_records = 'insert into taskno_record(ctime, utime, task_no, app_key, task_type, status, cycle_start, fee, is_free) \
                              values("%s","%s","%s","%s","%s",%s,"%s",%s,%s)'
    sql_new_exists_taskno = 'select count(*) as count from taskno_record where task_no="%s"'

    print "crd taskNo開始"
    start = 0
    limit = 10000
    count = 10000
    while count > 0:
        charge_orders = db_crd.query(sql_old_charge_orders, limit, start)
        count = len(charge_orders)
        for chargeOrder in charge_orders:
            status = convertStatus(chargeOrder['status'], chargeOrder['finish_flag'])
            fee = 0
            task_no = chargeOrder['task_no']
            # 判斷taskNo是否存在
            exists = db_local.query(sql_new_exists_taskno % (task_no))
            if exists[0]['count'] > 0:
                continue
            if status == 2 or status == 3:
                fee = 100
            cycle_start = getMonthStartDate(chargeOrder['create_time'])
            if chargeOrder['finish_time'] is None:
                chargeOrder['finish_time'] = '2017-01-01 00:00:00'
            db_local.execute(sql_new_insert_records % (
                chargeOrder['create_time'], chargeOrder['finish_time'], task_no, chargeOrder['app_key'], \
                chargeOrder['task_type'], status, cycle_start, fee, 0))
        start = start + count
        db_crd.reconnect()

    print "gcd taskNo開始"
    startgcd = 0
    limitgcd = 10000
    countgcd = 10000
    while countgcd > 0:
        charge_orders = db_gcd.query(sql_old_charge_orders, limitgcd, startgcd)
        countgcd = len(charge_orders)
        for chargeOrder in charge_orders:
            app_key = chargeOrder['app_key']
            task_no = chargeOrder['task_no']
            if app_key == 'y8x2huy9i2':  # 人人信
                continue
            # 判斷taskNo是否存在
            exists = db_local.query(sql_new_exists_taskno % (task_no))
            if exists[0]['count'] > 0:
                continue
            if app_key == 'u9z6ncrde7':  # 蓝莓
                app_key = 'fd9mnm1m6v'
            if app_key == 'mur1eur6wf':  # 快加
                app_key = 'hilw7pgpzs'
            status = convertStatus(chargeOrder['status'], chargeOrder['finish_flag'])
            cycle_start = getMonthStartDate(chargeOrder['create_time'])
            if chargeOrder['finish_time'] is None:
                chargeOrder['finish_time'] = '2017-01-01 00:00:00'
            db_local.execute(sql_new_insert_records % (
                chargeOrder['create_time'], chargeOrder['finish_time'], task_no, app_key, \
                chargeOrder['task_type'], status, cycle_start, 0, 1))
        startgcd = startgcd + countgcd
        print startgcd
        db_gcd.reconnect()

def calc_statis():
    day_statis()
    month_statis()
    cycel_statis()
    update_balance()

# ---------------------每天统计start---------------------
def day_statis():
    # 天统计表的次数
    sql_new_day_statis_callcount = 'SELECT app_key,task_type,DATE_FORMAT(ctime, "%%Y-%%m-%%d") as datetime,count(task_no) as tcount FROM `taskno_record` where is_free=0 GROUP BY app_key,task_type, DATE_FORMAT(ctime, "%%Y-%%m-%%d")'
    sql_new_day_statis_sucesscount = 'SELECT app_key,task_type,DATE_FORMAT(ctime, "%%Y-%%m-%%d") as datetime,count(task_no) as tcount,sum(fee) as feesum  FROM `taskno_record` where is_free=0 and (status=2 or status=3) GROUP BY app_key,task_type, DATE_FORMAT(ctime, "%%Y-%%m-%%d")'
    sql_new_day_statis_failcount = 'SELECT app_key,task_type,DATE_FORMAT(ctime, "%%Y-%%m-%%d") as datetime,count(task_no) as tcount  FROM `taskno_record` where is_free=0 and (status=4 or status=5) GROUP BY app_key,task_type, DATE_FORMAT(ctime, "%%Y-%%m-%%d")'
    sql_new_day_statis_callcountfree = 'SELECT app_key,task_type,DATE_FORMAT(ctime, "%%Y-%%m-%%d") as datetime,count(task_no)  as tcount  FROM `taskno_record` where is_free=1 GROUP BY app_key,task_type, DATE_FORMAT(ctime, "%%Y-%%m-%%d")'
    sql_new_day_statis_sucesscountfree = 'SELECT app_key,task_type,DATE_FORMAT(ctime, "%%Y-%%m-%%d") as datetime,count(task_no) as tcount  FROM `taskno_record` where is_free=1 and (status=2 or status=3) GROUP BY app_key,task_type, DATE_FORMAT(ctime, "%%Y-%%m-%%d")'
    sql_new_day_statis_failcountfree = 'SELECT app_key,task_type,DATE_FORMAT(ctime, "%%Y-%%m-%%d") as datetime,count(task_no) as tcount  FROM `taskno_record` where is_free=1 and (status=4 or status=5) GROUP BY app_key,task_type, DATE_FORMAT(ctime, "%%Y-%%m-%%d")'
    sql_new_insertday = 'insert into app_info_task_statis_day (ctime,utime,app_key,task_type,day,call_count,success_count,fail_count,call_count_free,success_count_free,fail_count_free,current_cost) values ("%s","%s","%s","%s","%s",%s,%s,%s,%s,%s,%s,%s)'

    # 每日调用次数
    daydict = {}
    callcounts = db_local.query(sql_new_day_statis_callcount)
    for i in callcounts:
        dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['datetime']
        dict = {}
        dict['call_count'] = i['tcount']
        if daydict.has_key(dictKey):
            dict = daydict.get(dictKey)
        daydict[dictKey] = dict
    # 每日成功调用次数
    sucesscounts = db_local.query(sql_new_day_statis_sucesscount)
    for i in sucesscounts:
        dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['datetime']
        dict = {}
        if daydict.has_key(dictKey):
            dict = daydict.get(dictKey)
        dict['success_count'] = i['tcount']
        # 只有成功计费的才有当前花费
        dict['current_cost'] = i['feesum']
        daydict[dictKey] = dict
    # 每日失败调用次数
    failcounts = db_local.query(sql_new_day_statis_failcount)
    for i in failcounts:
        dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['datetime']
        dict = {}
        if daydict.has_key(dictKey):
            dict = daydict.get(dictKey)
        dict['fail_count'] = i['tcount']
        daydict[dictKey] = dict
    # 每日免费调用次数
    callcountsfree = db_local.query(sql_new_day_statis_callcountfree)
    for i in callcountsfree:
        dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['datetime']
        dict = {}
        dict['call_count_free'] = i['tcount']
        if daydict.has_key(dictKey):
            dict = daydict.get(dictKey)
        daydict[dictKey] = dict
    # 每日成功调用次数
    sucesscountsfree = db_local.query(sql_new_day_statis_sucesscountfree)
    for i in sucesscountsfree:
        dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['datetime']
        dict = {}
        if daydict.has_key(dictKey):
            dict = daydict.get(dictKey)
        dict['success_count_free'] = i['tcount']
        daydict[dictKey] = dict
    # 每日失败调用次数
    failcountsfree = db_local.query(sql_new_day_statis_failcountfree)
    for i in failcountsfree:
        dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['datetime']
        dict = {}
        if daydict.has_key(dictKey):
            dict = daydict.get(dictKey)
        dict['fail_count_free'] = i['tcount']
        daydict[dictKey] = dict

    for day in daydict:
        call_count = 0
        success_count = 0
        fail_count = 0
        call_count_free = 0
        success_count_free = 0
        fail_count_free = 0
        current_cost = 0
        j = daydict.get(day)
        for i in j:
            if i == 'call_count':
                call_count = j[i]
            if i == 'success_count':
                success_count = j[i]
            if i == 'fail_count':
                fail_count = j[i]
            if i == 'call_count_free':
                call_count_free = j[i]
            if i == 'success_count_free':
                success_count_free = j[i]
            if i == 'fail_count_free':
                fail_count_free = j[i]
            if i == 'current_cost':
                current_cost = j[i]
        db_local.execute(sql_new_insertday % (
            now, now, day.split('|')[0], day.split('|')[1], day.split('|')[2], call_count, success_count, fail_count,
            call_count_free, success_count_free, fail_count_free, current_cost))


# ---------------------每月统计start---------------------
def month_statis():
    # 月统计表的次数
    sql_new_month_statis_callcount = 'SELECT app_key,task_type,DATE_FORMAT(ctime, "%%Y-%%m") as datetime,count(task_no) as tcount  FROM `taskno_record` where is_free=0 GROUP BY app_key,task_type, DATE_FORMAT(ctime, "%%Y-%%m")'
    sql_new_month_statis_sucesscount = 'SELECT app_key,task_type,DATE_FORMAT(ctime, "%%Y-%%m") as datetime,count(task_no) as tcount,sum(fee) as feesum  FROM `taskno_record` where is_free=0 and (status=2 or status=3) GROUP BY app_key,task_type, DATE_FORMAT(ctime, "%%Y-%%m")'
    sql_new_month_statis_failcount = 'SELECT app_key,task_type,DATE_FORMAT(ctime, "%%Y-%%m") as datetime,count(task_no) as tcount  FROM `taskno_record` where is_free=0 and (status=4 or status=5) GROUP BY app_key,task_type, DATE_FORMAT(ctime, "%%Y-%%m")'
    sql_new_month_statis_callcountfree = 'SELECT app_key,task_type,DATE_FORMAT(ctime, "%%Y-%%m") as datetime,count(task_no) as tcount  FROM `taskno_record` where is_free=1 GROUP BY app_key,task_type, DATE_FORMAT(ctime, "%%Y-%%m")'
    sql_new_month_statis_sucesscountfree = 'SELECT app_key,task_type,DATE_FORMAT(ctime, "%%Y-%%m") as datetime,count(task_no) as tcount  FROM `taskno_record` where is_free=1 and (status=2 or status=3) GROUP BY app_key,task_type, DATE_FORMAT(ctime, "%%Y-%%m")'
    sql_new_month_statis_failcountfree = 'SELECT app_key,task_type,DATE_FORMAT(ctime, "%%Y-%%m") as datetime,count(task_no) as tcount  FROM `taskno_record` where is_free=1 and (status=4 or status=5) GROUP BY app_key,task_type, DATE_FORMAT(ctime, "%%Y-%%m")'
    sql_new_insertmonth = 'insert into app_info_task_statis_month (ctime,utime,app_key,task_type,month,call_count,success_count,fail_count,call_count_free,success_count_free,fail_count_free,current_cost) values ("%s","%s","%s","%s","%s",%s,%s,%s,%s,%s,%s,%s)'

    # 每月调用次数
    monthdict = {}
    callcounts = db_local.query(sql_new_month_statis_callcount)
    for i in callcounts:
        dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['datetime']
        dict = {}
        dict['call_count'] = i['tcount']
        if monthdict.has_key(dictKey):
            dict = monthdict.get(dictKey)
        monthdict[dictKey] = dict
    # 每月成功调用次数
    sucesscounts = db_local.query(sql_new_month_statis_sucesscount)
    for i in sucesscounts:
        dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['datetime']
        dict = {}
        if monthdict.has_key(dictKey):
            dict = monthdict.get(dictKey)
        dict['success_count'] = i['tcount']
        dict['current_cost'] = i['feesum']
        monthdict[dictKey] = dict
    # 每月失败调用次数
    failcounts = db_local.query(sql_new_month_statis_failcount)
    for i in failcounts:
        dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['datetime']
        dict = {}
        if monthdict.has_key(dictKey):
            dict = monthdict.get(dictKey)
        dict['fail_count'] = i['tcount']
        monthdict[dictKey] = dict
    # 每月免费调用次数
    callcountsfree = db_local.query(sql_new_month_statis_callcountfree)
    for i in callcountsfree:
        dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['datetime']
        dict = {}
        dict['call_count_free'] = i['tcount']
        if monthdict.has_key(dictKey):
            dict = monthdict.get(dictKey)
        monthdict[dictKey] = dict
    # 每月成功调用次数
    sucesscountsfree = db_local.query(sql_new_month_statis_sucesscountfree)
    for i in sucesscountsfree:
        dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['datetime']
        dict = {}
        if monthdict.has_key(dictKey):
            dict = monthdict.get(dictKey)
        dict['success_count_free'] = i['tcount']
        monthdict[dictKey] = dict
    # 每月失败调用次数
    failcountsfree = db_local.query(sql_new_month_statis_failcountfree)
    for i in failcountsfree:
        dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['datetime']
        dict = {}
        if monthdict.has_key(dictKey):
            dict = monthdict.get(dictKey)
        dict['fail_count_free'] = i['tcount']
        monthdict[dictKey] = dict

    for month in monthdict:
        call_count = 0
        success_count = 0
        fail_count = 0
        call_count_free = 0
        success_count_free = 0
        fail_count_free = 0
        current_cost = 0
        j = monthdict.get(month)
        for i in j:
            if i == 'call_count':
                call_count = j[i]
            if i == 'success_count':
                success_count = j[i]
            if i == 'fail_count':
                fail_count = j[i]
            if i == 'call_count_free':
                call_count_free = j[i]
            if i == 'success_count_free':
                success_count_free = j[i]
            if i == 'fail_count_free':
                fail_count_free = j[i]
            if i == 'current_cost':
                current_cost = j[i]
        db_local.execute(sql_new_insertmonth % (
            now, now, month.split('|')[0], month.split('|')[1], getMonthStartDate(month.split('|')[2]), call_count,
            success_count, fail_count,
            call_count_free, success_count_free, fail_count_free, current_cost))


# ---------------------周期统计start---------------------
def cycel_statis():
    # 周期统计表的次数
    sql_new_cycle_statis_callcount = 'SELECT app_key,task_type,cycle_start ,count(task_no) as tcount  FROM `taskno_record` where is_free=0 GROUP BY app_key,task_type, cycle_start'
    sql_new_cycle_statis_sucesscount = 'SELECT app_key,task_type,cycle_start ,count(task_no) as tcount,sum(fee) as feesum  FROM `taskno_record` where is_free=0 and (status=2 or status=3) GROUP BY app_key,task_type, cycle_start'
    sql_new_cycle_statis_failcount = 'SELECT app_key,task_type,cycle_start ,count(task_no) as tcount  FROM `taskno_record` where is_free=0 and (status=4 or status=5) GROUP BY app_key,task_type, cycle_start'
    # sql_new_cycle_statis_callcountfree = 'SELECT app_key,task_type,cycle_start ,count(task_no) as tcount  FROM `taskno_record` where is_free=1 GROUP BY app_key,task_type, cycle_start'
    # sql_new_cycle_statis_sucesscountfree = 'SELECT app_key,task_type,cycle_start ,count(task_no) as tcount  FROM `taskno_record` where is_free=1 and (status=2 or status=3) GROUP BY app_key,task_type, cycle_start'
    # sql_new_cycle_statis_failcountfree = 'SELECT app_key,task_type,cycle_start ,count(task_no)  as tcount FROM `taskno_record` where is_free=1 and (status=4 or status=5) GROUP BY app_key,task_type, cycle_start'
    sql_new_insertcycle = 'insert into app_info_task_statis_cycle (ctime,utime,app_key,task_type,cycle_start,cycle_next_start,call_count,success_count,fail_count,call_count_free,success_count_free,fail_count_free,current_cost) values ("%s","%s","%s","%s","%s","%s",%s,%s,%s,%s,%s,%s,%s)'

    # 周期调用次数
    cycledict = {}
    callcounts = db_local.query(sql_new_cycle_statis_callcount)
    for i in callcounts:
        dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['cycle_start'].strftime('%Y-%m-%d')
        dict = {}
        dict['call_count'] = i['tcount']
        if cycledict.has_key(dictKey):
            dict = cycledict.get(dictKey)
        cycledict[dictKey] = dict
    # 周期成功调用次数
    sucesscounts = db_local.query(sql_new_cycle_statis_sucesscount)
    for i in sucesscounts:
        dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['cycle_start'].strftime('%Y-%m-%d')
        dict = {}
        if cycledict.has_key(dictKey):
            dict = cycledict.get(dictKey)
        dict['success_count'] = i['tcount']
        dict['current_cost'] = i['feesum']
        cycledict[dictKey] = dict
    # 周期失败调用次数
    failcounts = db_local.query(sql_new_cycle_statis_failcount)
    for i in failcounts:
        dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['cycle_start'].strftime('%Y-%m-%d')
        dict = {}
        if cycledict.has_key(dictKey):
            dict = cycledict.get(dictKey)
        dict['fail_count'] = i['tcount']
        cycledict[dictKey] = dict
    # # 周期免费调用次数
    # callcountsfree = db_local.query(sql_new_cycle_statis_callcountfree)
    # for i in callcountsfree:
    #     dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['datetime']
    #     dict = {}
    #     dict['call_count_free'] = i['tcount']
    #     if cycledict.has_key(dictKey):
    #         dict = cycledict.get(dictKey)
    #     cycledict[dictKey] = dict
    # # 周期成功调用次数
    # sucesscountsfree = db_local.query(sql_new_cycle_statis_sucesscountfree)
    # for i in sucesscountsfree:
    #     dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['datetime']
    #     dict = {}
    #     if cycledict.has_key(dictKey):
    #         dict = cycledict.get(dictKey)
    #     dict['success_count_free'] = i['tcount']
    #     cycledict[dictKey] = dict
    # # 周期失败调用次数
    # failcountsfree = db_local.query(sql_new_cycle_statis_failcountfree)
    # for i in failcountsfree:
    #     dictKey = i['app_key'] + "|" + i['task_type'] + "|" + i['datetime']
    #     dict = {}
    #     if cycledict.has_key(dictKey):
    #         dict = cycledict.get(dictKey)
    #     dict['fail_count_free'] = i['tcount']
    #     cycledict[dictKey] = dict

    for cycle in cycledict:
        call_count = 0
        success_count = 0
        fail_count = 0
        call_count_free = 0
        success_count_free = 0
        fail_count_free = 0
        current_cost = 0
        j = cycledict.get(cycle)
        for i in j:
            if i == 'call_count':
                call_count = j[i]
            if i == 'success_count':
                success_count = j[i]
            if i == 'fail_count':
                fail_count = j[i]
            if i == 'call_count_free':
                call_count_free = j[i]
            if i == 'success_count_free':
                success_count_free = j[i]
            if i == 'fail_count_free':
                fail_count_free = j[i]
            if i == 'current_cost':
                current_cost = j[i]
        db_local.execute(sql_new_insertcycle % (
            now, now, cycle.split('|')[0], cycle.split('|')[1], cycle.split('|')[2],
            getNextMonthStartDate(cycle.split('|')[2]), call_count, success_count,
            fail_count,
            call_count_free, success_count_free, fail_count_free, current_cost))


# 更新用户余额,和appinfo_task_type的剩余免费次数
def update_balance():
    sql_new_appKey_cost = 'SELECT app_key,sum(current_cost) as costsum FROM `app_info_task_statis_month` GROUP BY app_key'

    sql_new_update_max_free_count = 'UPDATE app_info_task_type set free_count_remaining=0 where free_count_remaining=-1'

    sql_new_month_statis = 'SELECT app_key,task_type,sum(call_count_free) as freecount FROM `app_info_task_statis_month` GROUP BY app_key,task_type'

    sql_new_update_free_count = 'UPDATE app_info_task_type set free_count_remaining=free_count_remaining-%s where app_key="%s" and task_type="%s"'

    sql_new_update_balance = 'UPDATE sys_user set balance=balance-%s where id=(SELECT user_id from app_info where app_key="%s") '
    # 将之前的免费次数为-1的改成10000000
    db_local.execute(sql_new_update_max_free_count)
    # 更新免费次数，从月统计表里面找call_count_free
    appkeytasks = db_local.query(sql_new_month_statis)
    for tasks in appkeytasks:
        db_local.execute(sql_new_update_free_count % (tasks['freecount'], tasks['app_key'], tasks['task_type']))
    # 更新用户余额
    appkeysCost = db_local.query(sql_new_appKey_cost)
    for appkey in appkeysCost:
        db_local.execute(sql_new_update_balance % (appkey['costsum'], appkey['app_key']))


def convertStatus(originStatus=2, finish_flag=1):
    status = 1
    if originStatus == 1:
        status = 1
    elif originStatus == 2:
        if finish_flag == 1:
            status = 3
        elif finish_flag == 2:
            status = 2
        else:
            status = 3
    elif originStatus == 3:
        if finish_flag == -1:
            status = 4
        else:
            status = 5
    else:
        status = 1
    return status


def getMonthStartDate(current_date="1970-01-01 00:00:00"):
    # print current_date
    current_date = str(current_date)
    strArray = current_date.split('-')
    return strArray[0] + '-' + strArray[1] + '-01'


def getNextMonthStartDate(current_date="1970-01-01"):
    cur = time.strptime(current_date, "%Y-%m-%d")
    first_day = datetime.date(cur.tm_year, cur.tm_mon, 1)
    days_num = calendar.monthrange(first_day.year, first_day.month)[1]  # 获取一个月有多少天
    first_day_of_next_month = first_day + datetime.timedelta(days=days_num)  # 当月的最后一天只需要days_num-1即可
    return first_day_of_next_month.strftime('%Y-%m-%d')


def gen_md5_pwd(username=''):
    puresalt = create_salt(32)
    salt = username + puresalt
    pwd = create_salt(6)

    m2 = hashlib.md5()
    m2.update(salt)
    m2.update(pwd)
    m3 = hashlib.md5()
    m3.update(m2.digest())
    finalpwd = m3.hexdigest()
    return pwd, puresalt, finalpwd


def create_salt(length=6):
    salt = ''
    chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
    len_chars = len(chars) - 1
    random = Random()
    for i in xrange(length):
        # 每次从chars中随机取一位
        salt += chars[random.randint(0, len_chars)]
    return salt


if __name__ == '__main__':
    initDb()
    start()
    migrateRecords()
    calc_statis()
