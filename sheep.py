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

    # sys_organization
    db_local.execute('insert into sys_organization(org_name, org_code, ctime, utime, create_user_id, modify_user_id) \
                    values("云蜂科技","IbeeSaas","%s", "%s", 0, 0)' % (now,now))
    db_local.execute('insert into sys_organization(org_name, org_code, ctime, utime, create_user_id, modify_user_id) \
                    values("内部测试","ForTest","%s", "%s", 0, 0)' % (now,now))

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
    db_local.execute('insert into sys_user(login_name, passwd, org_id, user_name, create_user_id,  modify_user_id, salt) \
                     values("admin", "d3c59d25033dbf980d29554025c23a75", 1, "管理员",0, 0, "8d78869f470951332959580424d4bf4f")')

    # sys_user_role
    db_local.execute('insert into sys_user_role(user_id, role_id, create_user_id) values(1, 1, 0)')


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
    orgs = db_crd.query(sql_gcd_appinfo)
    app_keys = set()

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
    

if __name__=='__main__':
    initDb()
    start()
