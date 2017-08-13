#!/usr/bin/python
# -*- coding: utf-8 -*-
"""pull module

Overview
========

web engine listening specified port, returning one matched msg according to
incoming received device information

Usage summary
=============

python mad.py --port=8084

"""
import MySQLdb
import re
import time
import datetime
import json
import torndb
from tornado import gen
import urllib
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado import httpclient
from tornado.options import define, options
import logging
import base64
import requests

import memcache
import os
import collections
from distutils.version import LooseVersion 

import redis

from ipip import IPX

define('version', default='1.1.1', help='version')

define('port', default='8084', help='run port')

define("mysql_host", default="127.0.0.1:3306", help="blog database host")
define("mysql_database", default="mad", help="blog database name")
define("mysql_user", default="mobilead", help="blog database user")
define("mysql_password", default="Mobad2016!", help="blog database password")

define("session1", default="APP-REQ", help="")
define("session1_rsp", default="APP-RSP")
define("session2", default="NOTIFY-APP-REQ", help="")
define("session2_rsp", default="NOTIFY-APP-RSP")

define("test_device_expiretime", default=15)
define("cycle_expiretime", default=15)
define("mad_settings_expiretime", default=15)
define("test_online_msgs_expiretime", default=60)
define("api_msgs_expiretime", default=300)
define("silent_duration_expiretime", default=3600)
define("appkey_expiretime", default=15)


mc = memcache.Client(['127.0.0.1:11211'])
rds = redis.Redis(host='127.0.0.1',port=6379,db=10)

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", BaseHandler),
            (r"/version", VersionHandler),
        ]
        super(Application, self).__init__(handlers)
        self.db = torndb.Connection(
            host=options.mysql_host, database=options.mysql_database,
            user=options.mysql_user, password=options.mysql_password)

        #populate the country_id, country_name to countrydict
        sql_country = "select country_id, iso_code from mad_rs_country"
        countrylist = self.db.query(sql_country)
        self.countrydict = dict()
        if len(countrylist) > 0:
            for country in countrylist:
                self.countrydict[country['iso_code']] = country['country_id']

        #populate the city_code, city_name to citydict
        sql_city = "select city_code, city_areacode from mad_rs_china_city"
        citylist = self.db.query(sql_city)
        self.citydict = dict()
        if len(citylist) > 0:
            for city in citylist:
                self.citydict[city['city_areacode']] = city['city_code']
        
        IPX.load(os.path.abspath(os.path.join(os.path.split(os.path.realpath(__file__))[0], 'ipdata.datx')))

class VersionHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def post(self):
        self.write(options.version)
        self.finish()

    @gen.coroutine
    def get(self):
        self.write(options.version)
        self.finish()

class BaseHandler(tornado.web.RequestHandler):

    @property
    def db(self):
        return self.application.db

    @property
    def countrydict(self):
        return self.application.countrydict

    @property
    def citydict(self):
        return self.application.citydict

    @gen.coroutine
    def post(self):
        """post handler to process incoming request.
        this method is asynchronous

        """
        self.multiple_msg = 1 
        self.response = None
        self.debug_model = False
        self.now = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        self.timestamp = int(time.time())
        report = self._decode()
        
        if report is None or type(report) != type(dict()) or report.has_key('version') == False:
            print "[%s] %s " % (self.now, json.dumps(report))
            self.version = '1.0'
            self.response = {'code': '1502', 'version': self.version}
            self._response_report()
            return
        if report.has_key("debug_model"):
                self.debug_model = report['debug_model']

        self.version = report['version']

        if report['session'] == options.session1:     
            
            self.ip = self.request.headers.get('X-Real-Ip') if self.request.headers.get('X-Real-Ip', None) is not None else self.request.remote_ip
            if report.has_key("debug_ip"):
                self.ip = report['debug_ip']

            country_id, location, city = self._get_location(self.ip)
            report['ip'] = self.ip
            report['country_id'] = country_id
            report['location'] = location
            report['city'] = city
            print "[%s] %s " % (self.now, json.dumps(report))
            # for meteor version, 'loc' could be null
            if not report.has_key('loc'):
                report['loc'] = ''
            # for meteor version, 'capability' could be null. if json version is not '1.0', support '01|02|03|05' <05: open url>
            if not report['carrier'].has_key('capability'):
                if report['version'] == '1.0':
                    report['carrier']['capability'] = '01|02|03'
                else:
                    report['carrier']['capability'] = '01|02|03|05'
            if not report['carrier'].has_key('stub_version'):
                report['carrier']['stub_version'] = ''
            self.report = {
                "devid": report['devid'],
                "utdid": report['utdid'],
                "man": report['man'],
                "model": report["mod"],
                "os_version": report['osv'],
                "lang": report['lang'],
                "operator": report['operator'],
                "loc": report['loc'],
                "appid": report['carrier']['appid'],
                "pkgname": report['carrier']['pkgname'],
                "channel": report['carrier']['channel'],
                "silent": report['carrier']['silent'],
                "app_version": report['carrier']['version'],
                "stub_version": report['carrier']['stub_version'],
                "capability": report['carrier']['capability'],
                "ip": self.ip,
                "country_id": country_id,
                "location": location,
                "city": city
            }

            self.pull_report = report
            self._pull_status()

            if report.has_key('iccid'):
                self.report["iccid"] = report['iccid']

            self.cycle = self._get_cycle(self.report['model'], self.report['appid'], self.report['channel'])
            # self.cycle = 8
            strCycle = str(self.cycle)

            #原从memcache查询，改为从redis查询，并支持查询IMEI，按|分隔
            dev_value = self._get_device_imei(self.report['utdid'])       
            if dev_value is None:
                dev_key = str( "utdid_" + self.report['utdid'])
                self.report['createtime'] = self.now
                dev_value = str(self.report['createtime']) + "|" + str(self.report['devid']).replace("|","")
                rds.set(dev_key, dev_value)                
                self._reg_dev()
            else:
                self.report['createtime'] = dev_value

            #按照IMEI保存缓存
            imei_val = str(self.report['appid']) + '-' + str(self.report['channel']) + '-' + str(self.report['devid'])
            if rds.sismember('appkey-channel-imei',imei_val) == False:
                rds.sadd('appkey-channel-imei',imei_val)
                self._reg_dev('mad_imei_device_today')

            #process the incoming request and send response
            self.response = {"version": self.version, "session": options.session1_rsp, "code": "1500", "cycle": strCycle}
            if (report['carrier']['appid'] == "1557b43e3bda40e8ce8a70b5" and report['carrier']['channel'] == "wingtech") or (report['carrier']['appid'] == "1557b43e3bda40e8ce8a70b5" and report['carrier']['channel'] == "hipad") or report is None:
                 
                if self._is_test_device() or self._get_temp_by_redis('country',country_id) == 1 or self._get_temp_by_redis('province',location) == 1 or self._get_temp_by_redis('city',city) == 1:
                    if self.cycle != 0:
                        self._process()
                else:
                    self.response = {"version": self.version, "session": options.session1_rsp,"cycle": strCycle}
            else:
                if self.cycle != 0:
                    if country_id != 0 :
                        self._process()
            self._response_report()
            return
        elif report['session'] == options.session2:
            print "[%s] %s " % (self.now, json.dumps(report))
            log_dir = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'log')
            self.statis_msg_dir = os.path.join(log_dir, 'statis_msg')
            self.statis_msg_api_dir = os.path.join(self.statis_msg_dir, 'api_log')
            if not os.path.exists(self.statis_msg_api_dir):
                os.makedirs(self.statis_msg_api_dir , 755)

            self.ip = self.request.headers.get('X-Real-Ip') if self.request.headers.get('X-Real-Ip', None) is not None else self.request.remote_ip
            if report.has_key("debug_ip"):
                self.ip = report['debug_ip']
            country_id, location, city = self._get_location(self.ip)
            report['status']['ip'] = self.ip
            report['status']['country_id'] = country_id
            report['status']['location'] = location
            report['status']['city'] = city
            run_log = json.dumps(report['status'])
            _status = report['status']
            self.report = {"run_log": run_log, "recode": _status['code'], "run_id": _status['correlator'], "job_id": _status['taskid']}
            self.response = {"version": self.version, "session": options.session2_rsp, "code": "1500"}
            self._process_status()
            self._response_report()
            return
        else:
            print "[%s] %s " % (self.now, json.dumps(report))
            self.response = {'code': '1502', 'version': self.version}
            self._response_report()
            return

    def _decode(self):
        """decode incoming json to dict
        if json version is bigger than 1.0, base64 decode

        @return: decoded dict
        """
        body = self.request.body
        report = None
        try:
            report = json.loads(body)
            # print "[%s] %s " % (self.now, body)
        except Exception, e:
            try:
                _body = base64.b64decode(body)
                codes = {"d0": "version", "d1": "session", "d2": "devid", "d3": "utdid", "d4": "man", "d5": "mod", "d6": "osv", 
                         "d8": "lang", "d9": "operator", "da":"loc", "c0": "carrier", "c1": "appid", "c2": "pkgname", 
                         "c3": "channel", "c4": "version", "c5": "silent", "c6": "capability", "c7": "stub_version", "r1": "code", "r0": "status", 
                         "a1": "correlator", "a2": "taskid", "s20": "APP-REQ", "s22": "NOTIFY-APP-REQ", "s30": "APP-SELFUPDATE-REQ", "db": "msisdn",
                         "dc": "iccid", "dd": "imsi"}
                for code, key in codes.items():
                    _body = _body.replace('"' + code + '"', '"' + key + '"')
                report = json.loads(_body)
                # print "[%s] %s " % (self.now, _body)
            except Exception, e:
                pass
        return report

    def _encode(self):
        """encode down json key, and convert to base64

        this method is to convert self.response to encoded self.response.
        and then convert self.response to base64 encoded

        @return: base64 encoded response

        """
        code_list = {"a1": "correlator", "a2": "taskid", "a3": "pkgname", "a4": "appname", "a5": "version", "a6": "brief", 
                     "a7": "objecturi", "a8": "objectsize", "a9": "icon", "a10": "start", "a11": "type", "a12": "action", 
                     "a13": "class", "a14": "extra", "a15": "operation", "a16":"pic", "a17": "strategy",
                     "a18": "targetapps", "a19":"servicepkg", "a20": "versionCode", "a22":"title", "a23":"md5"}
        if "applist" in self.response.keys():
            applist = json.dumps(self.response['applist'])
            for code, key in code_list.items():
                applist = applist.replace('"' + key + '"', '"' + code + '"')
            self.response['applist'] = json.loads(applist)
            if type(self.response['applist']) == list :
                for app_item in self.response['applist']:
                    if app_item.has_key("a10") and app_item['a10'].has_key("a14") and not app_item['a10'].has_key("extra"):
                        app_item['a10']['extra'] = app_item['a10']['a14']

        if "caplist" in self.response.keys():
            caplist = json.dumps(self.response['caplist'])
            for code, key in code_list.items():
                caplist = caplist.replace('"' + key + '"', '"' + code + '"')
            self.response['caplist'] = json.loads(caplist)

        code_body = {"d0": "version", "d1": "session", "r1": "code", "r2": "cycle", "l0":"link", "a1": "correlator", 
                     "a2": "taskid", "a15": "operation", "a7": "objecturi", "a0": "applist", "a21": "caplist", 
                     "s21": "APP-RSP", "s23": "NOTIFY-APP-RSP", "s31": "APP-SELFUPDATE-RSP"}
        response = json.dumps(self.response)
        for code, key in code_body.items():
            response = response.replace('"' + key + '"', '"' + code + '"')
        return base64.b64encode(response)

    def _response_report(self):
        """generata down json according to self.response dict

        this method will send the json down to client
        """
        response = json.dumps(self.response)
        print "[%s] %s " % (self.now, response)
        if self.version != '1.0':
            response = self._encode()

        self.write(response)
        self.finish()        

    #通过utdid获取设备的IMEI和注册时间
    #@return_type  create_time :获取设备的注册时间，imei:获取设备的IMEI
    def _get_device_imei(self, utdid , return_type = 'create_time'):
        dev_key = str( "utdid_" + utdid)
        dev_value = rds.get(dev_key)
        if not dev_value is None :
            dev_value_arr = dev_value.split("|")
            if len(dev_value_arr)>1:
                if return_type =='create_time':
                    return dev_value_arr[0]
                if return_type =='imei':
                    return dev_value_arr[1]
        return None

    def _get_location(self, ip):
        """get country_id prov_code and city_id by ip

        @param ip: the ip to resolve

        @return: the country_id and prov_code
            country_id is from mad_rs_country
            prov_code is from mad_rs_china_prov
            city_id is from mad_rs_china_city
            for foreign country, the prov_code is '00'
            for unresolved ip, country_id is 0, prov_code is '00', city_id=0
        """
        """
        @2017年6月9日14:29:06 切换为ipip.net IP数据库（收费版）
        解析后为unicode的字符串，需要按\t进行切割，含义如下：
        0"中国",                // 国家
        1"天津",                // 省会或直辖市（国内）
        2"天津",                // 地区或城市 （国内）
        3"",                   // 学校或单位 （国内）
        4"鹏博士",              // 运营商字段（只有购买了带有运营商版本的数据库才会有）
        5"39.128399",          // 纬度     （每日版本提供）
        6"117.185112",         // 经度     （每日版本提供）
        7"Asia/Shanghai",      // 时区一, 可能不存在  （每日版本提供）
        8"UTC+8",              // 时区二, 可能不存在  （每日版本提供）
        9"120000",             // 中国行政区划代码    （每日版本提供）
        10"86",                 // 国际电话代码        （每日版本提供）
        11"CN",                 // 国家二位代码        （每日版本提供）
        12"AP"                  // 世界大洲代码        （每日版本提供）
        """
        country_id = 0
        prov_code = '00'
        city_id = 0
        try:            
            ip_info = IPX.find(ip) #解析数组
            ip_arr  = ip_info.split("\t")
            #通过国家二位编码，获取country_id
            if self.countrydict.has_key(ip_arr[11]):
                country_id = self.countrydict[ip_arr[11]]

            #通过中国行政编码获取省份
            if ip_arr[11] == 'CN' or (ip_arr[9] is not None and len(ip_arr[9])>0): #China
                country_id = self.countrydict['CN']
                prov_code = ip_arr[9][:2]
                city_code = ip_arr[9][:4]
                if self.citydict.has_key(city_code):
                    city_id = self.citydict[city_code]
            elif ip_arr[11] == 'MO':
                prov_code = '82'
            elif ip_arr[11] == 'HK':
                prov_code = '81'
            elif ip_arr[11] == 'TW':
                prov_code = '71'
        except Exception, e:
            pass
        return country_id, prov_code, city_id

    @gen.coroutine
    def _reg_dev(self,tableName='mad_device'):
        """insert the device into mad_device table
           this method is asynchronous
        """
        try:
            sql = """INSERT INTO %s (utdid,appkey,device_id,ip,operator,os_version,channel,
                model,man,silent,location,sdk_version,pkgname,country_id,capability,ctime,utime) 
                VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',%s,'%s','%s','%s')
                """ % (tableName, self.report['utdid'], self.report['appid'], self.report['devid'], \
                self.report['ip'], self.report['operator'], self.report['os_version'], \
                self.report['channel'], self.report['model'], self.report['man'], \
                self.report['silent'], self.report['location'], self.report['app_version'], \
                self.report['pkgname'], self.report['country_id'], self.report['capability'],\
                self.now, self.now)
            reg_dev_id = self.db.execute_lastrowid(sql)
        except Exception, e:
            pass
    
    def _get_msg_by_api(self ):
        """
        获取所有包含API信息的03策略，用于上报时调用接口
        """
        api_msg = mc.get('api-msg')
        if api_msg is None:
            sql = """ select a.id , b.api from mad_msg as a 
                inner join mad_res_startapp as b 
                on a.res_id = b.id and b.api> 0
                where a.res_type = '03' and `status` in (1,3) 
                union all
                select a.id , b.api from mad_msg as a 
                inner join mad_res_openurl as b 
                on a.res_id = b.id and b.api> 0
                where a.res_type = '05' and `status` in (1,3)
                """
            msgs = self.db.query(sql)
            api_msg = {}
            for item in msgs:
                api_msg[item['id']] = item['api']
            mc.set('api-msg' ,api_msg , options.api_msgs_expiretime)

        return api_msg

    def _get_msg_by_type(self, type='online'):
        """get test or online messages list

        @param type: 'test' to get all test state messages
                     'online' to get all online messages

        @return dict contains messages list, key is msgid.
                if no online msgs, return empty dict

        this method first get from memcache, otherwise get from database
        """
        test_online_messages = mc.get('test-online-msgs')
        if test_online_messages is None:
            sql = """SELECT 
                id, 
                limit_num,
                status,
                res_id,
                res_type,
                cur_num,
                max_num,
                need_interval,
                multi_pull,
                infinite_pull,
                silent_period
            FROM
                mad_msg
            WHERE
                status = 1
            OR (
                status = 3
                AND (
                    (
                        limit_num > cur_num
                        OR infinite_pull = 2
                        OR multi_pull <> ''
                    )
                    OR (
                        (
                            res_type = '08'
                            AND (
                                max_num > cur_num
                                OR infinite_pull = 2
                            )
                        )
                    )
                )
                AND stime <= '%s'
            ) 
            AND
                is_delete = 0
                
            order by priority desc """ % self.now

            msgs = self.db.query(sql)
            test_online_messages = dict()
            for msg in msgs:
                if msg['status'] == 1: # test msg
                    if test_online_messages.has_key('test'):
                        test_online_messages['test'][msg['id']] = msg
                    else:
                        test_online_messages['test'] = collections.OrderedDict()
                        test_online_messages['test'][msg['id']] = msg
                else: # online msg
                    if test_online_messages.has_key('online'):
                        test_online_messages['online'][msg['id']] = msg
                    else:
                        test_online_messages['online'] = collections.OrderedDict()
                        test_online_messages['online'][msg['id']] = msg
            mc.set('test-online-msgs', test_online_messages, options.test_online_msgs_expiretime)

        self.cachedmsgs = test_online_messages # put the test_online_messages to global variable
        ret_msgs = dict()
        if type == 'online':
            if test_online_messages.has_key('online'):
                ret_msgs = test_online_messages['online']
        else:
            if test_online_messages.has_key('test'):
                ret_msgs = test_online_messages['test']

        return ret_msgs

    def _get_msg_role_by_id(self, msgid):
        """get msg role by msgid

        @param type: msgid 

        @return dict contains role_key and role_value

        this method first get from memcache, otherwise get from database
        """
        msg_role_key = 'msg-role-%s' % msgid
        msg_roles = mc.get(msg_role_key)
        if msg_roles is None:
            sql = 'select role_key, role_value from mad_msg_role where msg_id = %s' % msgid
            db_msg_roles = self.db.query(sql)
            msg_roles = dict()
            for db_msg_role in db_msg_roles:
                if not msg_roles.has_key(db_msg_role['role_key']):
                    msg_roles[db_msg_role['role_key']] = list()
                msg_roles[db_msg_role['role_key']].append(db_msg_role['role_value'])
            mc.set(msg_role_key, msg_roles, options.test_online_msgs_expiretime)

        return msg_roles

    def _is_test_device(self):
        """
        验证是否为测试设备
        """
        test_devices = self._get_test_devices()

        coming_device_str = self.report['man'].lower()+'-'+self.report['model'].lower()+'-'+self.report['devid'].lower()
        # incoming device is test device in appkey, get the test-state message

        if test_devices.has_key(self.report['appid']) and coming_device_str in test_devices[self.report['appid']]:
            return True
        return False

    def _get_test_devices(self):
        """
        获取测试设备列入，如缓存不存在，则从数据库读取
        """
        test_devices = mc.get("testdev")
        if test_devices is None:
            query_result = self.db.query("select appkey, device_id, man, model from mad_test_device")
            test_devices = dict()
            for device_item in query_result:
                device_item_str = device_item['man'].lower()+'-'+device_item['model'].lower()+'-'+device_item['device_id'].lower()
                if test_devices.has_key(device_item['appkey']):
                    test_devices[device_item['appkey']].add(device_item_str)
                else:
                    device_item_str_set = set()
                    device_item_str_set.add(device_item_str)
                    test_devices[device_item['appkey']] = device_item_str_set
            mc.set('testdev', test_devices, options.test_device_expiretime)

        return test_devices

    def _try_to_get_test_msg(self):
        """if the device is test device, return the matched test state msg id

        1. check if the device is test device for the its appkey
        2. if so, then query all test state mesages, find the all matched msgid
        Match Rule: test-state, appkey,capability. For test device, don't check silent duration,
                    max_count, channel, man|model, stime and so on. 
        """
        
        test_devices = self._get_test_devices()

        coming_device_str = self.report['man'].lower()+'-'+self.report['model'].lower()+'-'+self.report['devid'].lower()
        # incoming device is test device in appkey, get the test-state message

        matched_msg = list()
        matched_first_normal_msg = list()
        if test_devices.has_key(self.report['appid']) and coming_device_str in test_devices[self.report['appid']]:
            # test_msgs is dict(), key is the msgid
            test_msgs = self._get_msg_by_type('test')
            if test_msgs is not None: # test state msgs exist
                for msgid, msg in test_msgs.items(): 
                    if test_msgs[msgid]['res_type'] in self.report['capability']: #incoming device support the msg capability
                        the_msg_role = self._get_msg_role_by_id(msgid) # the_msg_role is dict
                        for role_key, role_value in the_msg_role.items(): # appkey matched (role_value is a list)
                            if role_key == 'appkey' and self.report['appid'] in role_value:
                                is_matched = False
                                # if self.multiple_msg == 2 and test_msgs[msgid]['res_type'] in "01|02|03" :
                                if self.multiple_msg == 2 :
                                    is_matched = True
                                elif test_msgs[msgid].has_key('infinite_pull') and test_msgs[msgid]['infinite_pull'] == 2:
                                    is_matched = True
                                elif test_msgs[msgid].has_key('multi_pull') and len(test_msgs[msgid]['multi_pull']) > 0 :
                                    is_matched = True
                                elif len(matched_first_normal_msg) == 0 :
                                    matched_first_normal_msg.append(msg) 
                                if is_matched : matched_msg.append(msg)
        return matched_msg + matched_first_normal_msg if len(matched_msg + matched_first_normal_msg)>0 else None 
        #return None

    def _get_cycle(self, model, appkey, channel):
        """get the cycle by given model, appkey, and channel
        get Cycle column from memcache,then mad_cycle table.
        the priority is as: model > channel > appkey

        @param model
        @param appkey
        @param channel
        @return the cycle. if not matched, return the default 8
        """
        cycle = 8 #default cycle is 8
        cycle_key = 'cycles'
        cycle_value = mc.get(cycle_key)
        if cycle_value is None:
            sql = 'select type, value, cycle from mad_cycle order by type asc'
            sql_result = self.db.query(sql)
            cycle_value = []
            for sql_result_item in sql_result:
                cycle_item = {'type':sql_result_item['type'], 'value':sql_result_item['value'], 'cycle':sql_result_item['cycle']}
                cycle_value.append(cycle_item)
            mc.set(cycle_key, cycle_value, options.cycle_expiretime)

        match_channel = '%s-%s' % (appkey, channel)
        for cycle_item in cycle_value:
            if cycle_item['type'] == 1 and cycle_item['value'] == model.lower(): # model
                cycle = cycle_item['cycle']
                break
            if cycle_item['type'] == 2 and cycle_item['value'] == match_channel: # appkey-channel
                cycle = cycle_item['cycle']
                break
            if cycle_item['type'] == 3 and cycle_item['value'] == appkey: # appkey
                cycle = cycle_item['cycle']
                break

        return cycle

    def _is_silent_duration(self,silent_period, createtime, appkey, channel):
        """check the device is in silent duration
        compare the device createtime and now

        @param createtime: the device first pull time
        @param appkey: the device appkey
        @param channel: the device channel

        @return: True: if in silent duration, otherwise False
        """
        #检测策略是否处于静默期
        #silent_period:静默期
        #stime:策略起作用时间 
        if silent_period >= 0: 
            date = datetime.datetime.strptime(createtime, '%Y-%m-%d %H:%M:%S')
            datenow = datetime.datetime.strptime(self.now, '%Y-%m-%d %H:%M:%S')
            diffday = (datenow - date).days
            if int(diffday) < int(silent_period):
                return True
            else:
                return False
        #first check appkey channel silent days
        silent_channel_key = 'silent-%s-%s' %(appkey, channel)
        silent_channel_value = mc.get(silent_channel_key)
        if silent_channel_value is None:
            sql = 'select appkey, channel, silent_day from mad_silent where appkey="%s" and channel="%s" limit 1' % (appkey, channel)
            sql_result = self.db.query(sql)
            if sql_result is None or sql_result == []: # not find silent day
                silent_channel_value = -1
            else:
                silent_channel_value = sql_result[0]['silent_day']
            mc.set(silent_channel_key, silent_channel_value, options.silent_duration_expiretime)
        silent_days = silent_channel_value

        #if appkey channel not set silent days, check appkey
        if silent_days == -1: # not find appkey channel silent
            silent_appkey_key = 'silent-%s' %(appkey)
            silent_appkey_value = mc.get(silent_appkey_key)
            if silent_appkey_value is None:
                sql = 'select appkey, silent_day from mad_silent where appkey="%s" and channel is null limit 1' % (appkey)
                sql_result = self.db.query(sql)
                if sql_result is None or sql_result == []: # not find silent day
                    silent_appkey_value = -1
                else:
                    silent_appkey_value = sql_result[0]['silent_day']
                mc.set(silent_appkey_key, silent_appkey_value, options.silent_duration_expiretime)
            silent_days = silent_appkey_value

        #finally compare the createtime and now with silent_days
        if silent_days == -1:
            return False
        else:
            create_time = datetime.datetime.strptime(createtime, '%Y-%m-%d %H:%M:%S')
            now = datetime.datetime.strptime(self.now, '%Y-%m-%d %H:%M:%S')
            diff_day = (now - create_time).days
            if int(diff_day) < int(silent_days):
                return True
            else:
                return False            

    def _convert_dict_to_pullmsg(self, pull_msg_dict):
        """convert dict to pull msg content
        @param pull_msg_dict:{'1': {'multi': 0}, '2': {'multi': 1, 'first_time': 1469073538, 'bitmask': 1}, '3': {'multi': 0}}
        @return ",1,2:1469073538:1,3,"
        """
        retvalue = ','
        for (k,v) in pull_msg_dict.items():
            one_msg_value = ''
            if v['multi'] == 0:
                one_msg_value = str(k) + ","
            else:
                one_msg_value = str(k)+":"+str(v['first_time'])+":"+str(v['bitmask'])+","
            retvalue = retvalue + one_msg_value
        return retvalue

    def _convert_pullmsg_to_dict(self, msgids_content):
        """convert msg ids content to dict
        @param msgids_content: ",1,2:1469073538:1,3,"
        @return a dict: {'1': {'multi': 0}, '2': {'multi': 1, 'first_time': 1469073538, 'bitmask': 1}, '3': {'multi': 0}}
        """
        pull_msg_dict = {}
        pull_msg_ids = str(msgids_content)
        pull_msg_set = set(pull_msg_ids.split(','))
        for pull_msg in pull_msg_set:
            if len(pull_msg)>0:
                if ':' in pull_msg:
                    multi_pull_msg_arr = pull_msg.split(':')
                    pull_msg_id = multi_pull_msg_arr[0]
                    pull_msg_value = {'multi':1, 'first_time':int(multi_pull_msg_arr[1]), 'bitmask':int(multi_pull_msg_arr[2])}
                    pull_msg_dict[pull_msg_id] = pull_msg_value
                else:
                    pull_msg_value = {'multi':0}
                    pull_msg_dict[pull_msg] = pull_msg_value
        return pull_msg_dict

    def _check_pull_cond(self, target_msg_id, need_interval, multi_pull, infinite_pull):
        """check whether the target_msg can be pulled or not, based on following condition:
        1. for normal msg, if this msg or relate msg has not been pulled, return true.
        2. for need_interval msg, if relate msg not been pulled and exceeds the interval duration, return true.
        3. for multi_pull msg, if relate msg not been pulled or this time matched multi-pull condition, return true. set self.multi_count
        4. for infinite_pull msg, if msg is infinite_pull, return true

        @param target_msg_id: the target msg id to be checked
        @param need_interval: the target msg is need_interval or not.
        @param multi_pull: format is as '1,3,7,10' indicate the msg can be pulled multi times. if empty, this msg is normal.
        @param infinite_pull: the target msg is infinite_pull or not.
        @return True: pass. False: can't be pulled.
        """
        target_msg_id = str(target_msg_id)
        now = datetime.datetime.strptime(self.now, '%Y-%m-%d %H:%M:%S')
        #注掉，需恢复
        #获取所有定义的策略类型每天拉取次数和拉取时间间隔
        #order_msg_time_num = mc.get('mad-settings')
        #if order_msg_time_num is None:
        order_msg_time_num = self._get_order_msg_time_num()
        
        self.multi_days_dict[target_msg_id] = None
        self.multi_bit_mask_dict[target_msg_id] = None

        #get pull_msg_ids from memcache
        dev_key = 'pulledmsg-%s' % self.report['utdid']
        pullmsg_value = str(mc.get(dev_key))

        if pullmsg_value is None: # it's the 1st time for this device to pull msg
            if len(multi_pull) > 0:
                self.multi_days_dict[target_msg_id] = 0
                self.multi_bit_mask_dict[target_msg_id] = 0
            return True
        #注意：只有低频类型的时间间隔可以超过一天
        if infinite_pull == 2 and int(order_msg_time_num['msg_unlimited_push_time']) > 0:
            #判断今天该设备拉取无限拉取策略的次数是否达到最大值
            if int(self.infinite_num) < int(order_msg_time_num['msg_unlimited_push_time']):
                if self.last_infinite_time:
                    lpulltime = datetime.datetime.strptime(self.last_infinite_time, '%Y-%m-%d %H:%M:%S')
                    diff_day = (now - lpulltime).days
                    #最后一次拉取时间为今天
                    if int(diff_day) < 1:
                        #判断是否大于定义的拉取时间间隔
                        diff_hour = (now - lpulltime).seconds/3600
                        if int(diff_hour) < int(order_msg_time_num['msg_unlimited_push_interval']):
                            return False
            else:
                return False
        elif len(multi_pull) > 0 and int(order_msg_time_num['msg_multiple_push_time']) > 0:
            #判断今天该设备多次激活策略的次数是否达到最大值
            if int(self.multi_num) < int(order_msg_time_num['msg_multiple_push_time']):
                if self.last_multi_time:
                    lpulltime = datetime.datetime.strptime(self.last_multi_time, '%Y-%m-%d %H:%M:%S')
                    diff_day = (now - lpulltime).days
                    #最后一次拉取时间为今天
                    if int(diff_day) < 1:
                        #判断是否大于定义的拉取时间间隔
                        diff_hour = (now - lpulltime).seconds/3600
                        if int(diff_hour) < int(order_msg_time_num['msg_multiple_push_interval']):
                            return False
            else:
                return False
        elif need_interval == 1 and int(order_msg_time_num['msg_low_frequency_push_time']) > 0:
            #判断今天该设备低频策略的次数是否达到最大值
            if int(self.interval_num) < int(order_msg_time_num['msg_low_frequency_push_time']):
                if self.last_interval_time:
                    lpulltime = datetime.datetime.strptime(self.last_interval_time, '%Y-%m-%d %H:%M:%S')
                    diff_day = (now - lpulltime).days
                    #判断是否大于定义的拉取时间间隔
                    diff_hour = (now - lpulltime).seconds/3600
                    if (int(diff_hour)+int(diff_day)*24) < int(order_msg_time_num['msg_low_frequency_push_interval']):
                        return False
            else:
                return False
        elif int(order_msg_time_num['msg_ordinary_push_time']) > 0:
            #判断今天该设备普通策略的次数是否达到最大值
            if int(self.normal_num) < int(order_msg_time_num['msg_ordinary_push_time']):
                if self.last_normal_time:
                    lpulltime = datetime.datetime.strptime(self.last_normal_time, '%Y-%m-%d %H:%M:%S')
                    diff_day = (now - lpulltime).days
                    #最后一次拉取时间超过一天
                    if int(diff_day) < 1:
                        #判断是否大于定义的拉取时间间隔
                        diff_hour = (now - lpulltime).seconds/3600
                        if int(diff_hour) < int(order_msg_time_num['msg_ordinary_push_interval']):
                            return False
            else:
                return False
        retvalue = False

        #get pull_msg_dict {'1': {'multi': 0}, '2': {'multi': 1, 'first_time': 13223432, 'bitmask': 15}}
        pull_msg_dict = self.pull_msg_dict
        pull_msg_set = self.pull_msg_set
        if target_msg_id in pull_msg_set: # the msg has been pulled by the device
            if pull_msg_dict[target_msg_id]['multi'] == 1 and len(multi_pull) > 0: #multi-pull msg
                first_pull_time = pull_msg_dict[target_msg_id]['first_time']
                bit_mask = pull_msg_dict[target_msg_id]['bitmask']
                inter_days = (self.timestamp-first_pull_time)//(24*60*60)

                if str(inter_days) in multi_pull.split(','): # today is in mutli-pull days (1,3,7,X)
                    if inter_days == 1:
                        if bit_mask & int('0001',2) == 0: # 1 day not pulled yet
                            retvalue = True
                            self.multi_days_dict[target_msg_id] = 1
                            self.multi_bit_mask_dict[target_msg_id] = bit_mask|int('0001',2)
                    elif inter_days == 3:
                        if bit_mask & int('0010',2) == 0: # 3 day not pulled
                            retvalue = True
                            self.multi_days_dict[target_msg_id] = 3
                            self.multi_bit_mask_dict[target_msg_id] = bit_mask|int('0010',2)
                    elif inter_days == 7:
                        if bit_mask & int('0100',2) == 0: # 7 day not pulled
                            retvalue = True
                            self.multi_days_dict[target_msg_id] = 7
                            self.multi_bit_mask_dict[target_msg_id] = bit_mask|int('0100',2)
                    else:
                        day_num = '0'
                        multi_pull_list = multi_pull.split(',')
                        multi_pull_defaule = ['1','3','7']
                        multi_pull_msg = []
                        for multi_pull_field in multi_pull_list:
                            if multi_pull_field not in multi_pull_defaule:
                                multi_pull_msg.append(multi_pull_field)
                        multi_pull_msg.sort()
                        multi_pull_len = multi_pull_msg.index(str(inter_days)) + 4 
                        msg_multi_day_num = '1000'
                        if multi_pull_len > 4:
                            msg_multi_day_num = '1' + day_num*(multi_pull_len-1)
                        if bit_mask & int(msg_multi_day_num,2) == 0: # X day not pulled
                            retvalue = True
                            self.multi_days_dict[target_msg_id] = inter_days
                            self.multi_bit_mask_dict[target_msg_id] =  bit_mask|int(msg_multi_day_num,2)
                else: # today is not in multi-pull days (1,3,7,X)
                    retvalue = False
            elif infinite_pull == 2:# is infinite_pull msg
                return True
            else: # not multi-pull msg and infinite_pull msg
                retvalue = False
        else: # this msg has not been pulled by the device before
            #check target relate msg ids
            target_relate_msg_set = set()
            #first try to get relate_msgs from memcache
            relate_msgs = mc.get('msg-relate')
            if relate_msgs is None:
                relate_msgs = list()
                sql = 'select relate_msg from mad_msg_relate'
                sql_result = self.db.query(sql)
                for sql_result_item in sql_result:
                    relate_msgs.append(sql_result_item['relate_msg'])
                mc.set('msg-relate', relate_msgs, options.test_online_msgs_expiretime)

            #query relate_msgs one by one, if contains target_msg_id, union to a set
            for item in relate_msgs:
                item_set = set(item.split(','))
                if target_msg_id in item_set:
                    target_relate_msg_set = target_relate_msg_set | item_set

            has_pulled = True
            #intersect pull_msg_set with target_relate_msg_set, if have common msg, return True
            common_set = target_relate_msg_set & pull_msg_set
            if common_set is None or len(common_set)==0 or (len(common_set)==1 and '' in common_set):
                has_pulled = False

            if not has_pulled: # relate msg not been pulled by the device
                #check need_interval msg time duration
                if need_interval == 1:
                    retvalue = True
                else: # this msg is not need_interval msg
                    if len(multi_pull) > 0: #multi-pull msg
                        self.multi_days_dict[target_msg_id] = 0
                        self.multi_bit_mask_dict[target_msg_id] =  0
                    retvalue = True
            else: # relate msg has been pulled by the device
                retvalue = False
        return retvalue
                
    def _get_operator(self, operator):
        """get operator according to mncmcc

        @param operator: mncmcc as 46000
        @return carrier id.  0:China Mobile; 1: China Unicom; 2: China Telecom; -1: unknown
        """
        if operator == '46000' or operator == '46002' or operator == '46007':
            return 0
        elif operator == '46001':
            return 1
        elif operator == '46003':
            return 2
        else:
            return -1

    def _try_to_get_online_msg(self):
        """return the matched online msg id list

        query all online state mesages, find the all matched msgid
        Match Rule: silent duration, max_count, appkey, channel, man|model, stime, capability and so on.

        @return: the all matched online msg id list
        """
        #check silent duration
        #if self._is_silent_duration(self.report['createtime'], self.report['appid'], self.report['channel']):
        #    return None

        #get all online, check one by one
        online_msgs = self._get_msg_by_type('online')
        matched_msg = list()
        matched_first_normal_msg = list()
        now_hour = time.strftime("%H", time.localtime())
        for msgid, msg in online_msgs.items():

            #check silent duration
            if self._is_silent_duration(msg['silent_period'],self.report['createtime'], self.report['appid'], self.report['channel']):
                continue

            # check if reach limit num
            if (msg['cur_num'] >= msg['limit_num'] and msg['res_type'] != '08') or msg['cur_num'] >= msg['max_num']:
            # infinite_pull msg or multi_pull msg before is pulled, return true
                if str(msgid) in self.pull_msg_set and (msg['infinite_pull'] == 2 or len(msg['multi_pull']) > 0 ):
                    pass
                else:
                    continue
            the_msg_role = self._get_msg_role_by_id(msgid) # the_msg_role is dict

            #判断当前时间是否为策略定义的拉取时间
            if the_msg_role.has_key('push_hour'):
                hour = ','+str(int(now_hour))+','
                if hour not in str(the_msg_role['push_hour']):
                    continue
            # check capability
            if msg['res_type'] not in self.report['capability']:
                continue
            # check pull condition
            if not self._check_pull_cond(msgid, msg['need_interval'], msg['multi_pull'], msg['infinite_pull']):
                continue
            #check msg_role
            matched = True
            operator = self._get_operator(self.report['operator'])
            no_match_info = ''
            for role_key, role_value in the_msg_role.items(): # role_value is a list
                
                if role_key == 'appkey': # can be multi-rows
                    if self.report['appid'] not in role_value:
                        no_match_info = 'Match appkey failure'
                        matched = False
                        break
                elif role_key == 'channel': # can be multi-rows.
                #channel value is format as appkey-channel
                #match rule: 如果appkey匹配上，但channel没有匹配上，则未匹配
                #如果appkey没有匹配上，则继续其他匹配
                    match_channel = '%s-%s' % (self.report['appid'], self.report['channel'])
                    is_match_appkey = False
                    is_match_channel = False
                    for key_channel in role_value:
                        if key_channel.startswith(self.report['appid']):
                            is_match_appkey = True
                        if key_channel == match_channel:
                            is_match_channel = True
                    if not is_match_channel and is_match_appkey:
                        no_match_info = 'Match appkey and channel failure'
                        matched = False
                        break
                elif role_key == 'country_id': # only one row, format: ',1,3,5,'
                    coming_country_id = ',%s,' % self.report['country_id']
                    if coming_country_id not in role_value[0]:
                        no_match_info = 'Match country failure'
                        matched = False
                        break
                elif role_key == 'country_id_b': # only one row, format: ',1,3,5,'
                    coming_country_id = ',%s,' % self.report['country_id']
                    if coming_country_id in role_value[0]:
                        no_match_info = 'Match country blacklist'
                        matched = False
                        break
                elif role_key == 'location': # province code, only one row, format: ',33,22,21,'
                    coming_location = ',%s,' % self.report['location']
                    if coming_location not in role_value[0]:
                        no_match_info = 'Match province failure'
                        matched = False
                        break
                elif role_key == 'location_b': # black list province code, only one row, format: ',33,22,21,'
                    coming_location = ',%s,' % self.report['location']
                    if coming_location in role_value[0]:
                        no_match_info = 'Match province blacklist'
                        matched = False
                        break
                elif role_key == 'city': # city code, only one row, format: ',1,2,21,'
                    coming_city = ',%s,' % self.report['city']
                    if coming_city not in role_value[0]:
                        no_match_info = 'Match city failure'
                        matched = False
                        break
                elif role_key == 'city_b': # black list city code, only one row, format: ',1,2,21,'
                    coming_city = ',%s,' % self.report['city']
                    no_match_info = 'Match city blacklist'
                    if coming_city in role_value[0]:
                        matched = False
                        break
                elif role_key == 'man_model': # can be multi-rows
                    coming_man_model = '%s-%s' % (self.report['man'].lower(), self.report['model'].lower())
                    if coming_man_model not in role_value:
                        no_match_info = 'Match man-model failure'
                        matched = False
                        break
                elif role_key == 'man_model_b': # black list man-model, can be multi-rows
                    coming_man_model = '%s-%s' % (self.report['man'].lower(), self.report['model'].lower())
                    if coming_man_model in role_value:
                        no_match_info = 'Match man-model blacklist'
                        matched = False
                        break
                # handle black list for each operator
                # PS: for one operator, ONLY ONE List(black or white) can be applied, not BOTH
                # PS: but for different operators, black and white can be applied at the same time.
                #     for example: ChinaMobile has black list and ChinaUnicom has white list
                elif role_key == 'cmb': # ChinaMobile blacklist
                    if operator == -1: # unknown
                        matched = False
                        no_match_info = 'Match CMCC blacklist'
                        break
                    elif operator == 0: # ChinaMobile
                        if str(','+self.report['location']+',') in role_value[0]:
                            no_match_info = 'Match CMCC province blacklist'
                            matched = False
                            break
                elif role_key == 'cub': # ChinaUnicom blacklist
                    if operator == -1: # unknown
                        matched = False
                        no_match_info = 'Match CUCC blacklist'
                        break
                    elif operator == 1: # ChinaUnicom
                        if str(','+self.report['location']+',') in role_value[0]:
                            matched = False
                            no_match_info = 'Match CUCC province blacklist'
                            break
                elif role_key == 'ctb': # ChinaTelecom blacklist
                    if operator == -1: # unknown
                        matched = False
                        no_match_info = 'Match CTCC blacklist'
                        break
                    elif operator == 2: # ChinaTelecom
                        if str(','+self.report['location']+',') in role_value[0]:
                            no_match_info = 'Match CTCC province blacklist'
                            matched = False
                            break
                #handle white list for each operator
                elif role_key == 'cmw': # ChinaMobile White List
                    if operator == -1: # unknown
                        matched = False
                        no_match_info = 'Match CMCC white failure'
                        break
                    elif operator == 0: # ChinaMobile
                        if str(','+self.report['location']+',') not in role_value[0]:
                            no_match_info = 'Match CMCC province white failure'
                            matched = False
                            break
                elif role_key == 'cuw': # ChinaUnicom White List
                    if operator == -1: # unknown
                        matched = False
                        no_match_info = 'Match CUCC white failure'
                        break
                    elif operator == 1: # ChinaUnicom
                        if str(','+self.report['location']+',') not in role_value[0]:
                            no_match_info = 'Match CUCC province white failure'
                            matched = False
                            break
                elif role_key == 'ctw': # ChinaTelecom White List
                    if operator == -1: # unknown
                        matched = False
                        no_match_info = 'Match CTCC white failure'
                        break
                    elif operator == 2: # ChinaTelecom
                        if str(','+self.report['location']+',') not in role_value[0]:
                            no_match_info = 'Match CTCC province white failure'
                            matched = False
                            break
                elif role_key == 'devlistid': # Target Device
                    devlistid_redis_key = 'devlistid_' + str(role_value[0])
                    cur_device_list = self.report['devid'].split(":",1)
                    cur_device = cur_device_list[-1]
                    cur_iccid = self.report['iccid'] if self.report.has_key('iccid') else ''
                    if rds.sismember(devlistid_redis_key,cur_device) or rds.sismember(devlistid_redis_key,cur_iccid):
                        pass
                    else:
                        matched = False
                        no_match_info = 'Match device list failure'
                        break
                else:
                    #should never reach here
                    pass

            if matched:

                is_matched = False
                # if self.multiple_msg == 2 and msg['res_type'] in "01|02|03" :
                if msg['res_type'] == '08':
                    res_value = mc.get('res-08')
                    if res_value is None:
                        res_value = self._get_res_from_db('08')
                        mc.set('res-08',res_value,options.test_online_msgs_expiretime)
                    if res_value.has_key(msg['res_id']):
                        if (LooseVersion(res_value[msg['res_id']]['version']) > LooseVersion(self.report["app_version"])) and (res_value[msg['res_id']]['stub_version'] == self.report["stub_version"]):
                            result = list()
                            result.append(msg)
                            return result
                elif self.multiple_msg == 2 :
                    is_matched = True
                elif msg.has_key('infinite_pull') and msg['infinite_pull'] == 2:
                    is_matched = True
                elif len(msg['multi_pull']) > 0:
                    is_matched = True
                elif (len(matched_first_normal_msg) == 0) or (len(matched_first_normal_msg) == 1) :
                    if len(matched_first_normal_msg) == 0:
                        matched_first_normal_msg.append(msg)
                    elif matched_first_normal_msg[0]['res_type'] == '05':
                        if msg['res_type'] in ['01','02','03','06','07']:
                            matched_first_normal_msg.append(msg)
                    elif matched_first_normal_msg[0]['res_type'] in ['01','02','03','06','07']:
                        if msg['res_type'] == '05':
                            matched_first_normal_msg.append(msg)
                if is_matched : matched_msg.append(msg)
            elif self.debug_model == True:
                debug_info = "Msgid:%s , Failure:%s" % (msgid , no_match_info)
                if not self.response.has_key('debug_info'):
                    self.response['debug_info'] = []
                self.response['debug_info'].append(debug_info)
                print debug_info

        return matched_msg + matched_first_normal_msg if len(matched_msg + matched_first_normal_msg)>0 else None 
        #return None


    def _get_res_tablename(self, operation):
        """get resource tablename based on operation
        01,02: mad_res_app
        03: mad_res_startapp
        04: mad_res_uninstallapp
        05: mad_res_openurl
        06: mad_res_dialog
        07: mad_res_noti
        08: mad_res_upgrade

        @param: operation
        @return: table name as string
        """
        tablename = None
        if operation == '01' or operation == '02':
            tablename = 'mad_res_app'
        elif operation == '03':
            tablename = 'mad_res_startapp'
        elif operation == '04':
            tablename = 'mad_res_uninstallapp'
        elif operation == '05':
            tablename = 'mad_res_openurl'
        elif operation == '06':
            tablename = 'mad_res_dialog'
        elif operation == '07':
            tablename = 'mad_res_noti'
        elif operation == '08':
            tablename = 'mad_res_upgrade'
        else: # should never reach here
            tablename = None

        return tablename

    def _get_res_from_db(self, operation):
        """get resouce from db, return a dict. key is res_id

        @param: operation: 0102, 03, 04, 05, 06, 07, 08
        @return a dict contains all resource items. 
                key is res_id. value is dict for this res
        """
        ret_dict = dict()
        if operation == '0102':
            sql = "select r.id, r.operation, a.pkgname, a.appname, a.version, a.versioncode, a.brief, a.brief_en, a.url, a.icon, a.size, a.start_json \
                from mad_res_app as r, mad_app as a, mad_msg as m \
                where r.app_id = a.id and a.is_delete = 0 and r.is_delete = 0 and \
                (m.res_type='01' or m.res_type='02') and (m.`status`=1 or m.`status` = 3) and m.res_id=r.id"
            sql_result = self.db.query(sql)
            for item in sql_result:
                ret_dict[item['id']] = item
        elif operation == '03':
            sql = "select r.id, r.pkgname, '1.0' as version, 1 as versioncode, r.start_json, r.operation \
                from mad_res_startapp as r, mad_msg as m \
                where r.is_delete = 0 and m.res_type='03' and (m.`status`=1 or m.`status` = 3) and m.res_id=r.id"
            sql_result = self.db.query(sql)
            for item in sql_result:
                ret_dict[item['id']] = item
        elif operation == '04':
            sql = "select r.id, r.pkgname, r.operation \
                from mad_res_uninstallapp as r, mad_msg as m \
                where r.is_delete = 0 and m.res_type='04' and (m.`status`=1 or m.`status` = 3) and m.res_id=r.id"
            sql_result = self.db.query(sql)
            for item in sql_result:
                ret_dict[item['id']] = item
        elif operation == '05':
            sql = "select r.id, r.url, r.operation \
                from mad_res_openurl as r, mad_msg as m \
                where r.is_delete = 0 and m.res_type='05' and (m.`status`=1 or m.`status` = 3) and m.res_id=r.id"
            sql_result = self.db.query(sql)
            for item in sql_result:
                ret_dict[item['id']] = item
        elif operation == '06':
            sql = "select r.id, r.pic_url, r.operation, r.should_start, r.title, r.action, r.open_url, r.brief, \
                a.pkgname, a.appname, a.version, a.versioncode, a.brief_en, a.url, a.icon, a.size, a.start_json \
                from mad_res_dialog as r LEFT JOIN mad_app as a ON r.app_id = a.id INNER JOIN mad_msg as m on r.id = m.res_id \
                where r.is_delete = 0 and m.res_type='06' and (m.`status`=1 or m.`status` = 3) and m.res_id=r.id"
            sql_result = self.db.query(sql)
            for item in sql_result:
                ret_dict[item['id']] = item
        elif operation == '07':
            sql = "select r.id, r.icon_url, r.operation, r.should_start, r.title, r.brief as brief, r.pic_url, r.action, r.open_url, \
                a.pkgname, a.appname, a.version, a.versioncode, a.brief as abrief, a.brief_en, a.url, a.icon, a.size, a.start_json \
                from mad_res_noti as r LEFT JOIN mad_app as a ON r.app_id = a.id INNER JOIN mad_msg as m on r.id = m.res_id \
                where r.is_delete = 0 and m.res_type='07' and (m.`status`=1 or m.`status` = 3) and m.res_id=r.id"
            sql_result = self.db.query(sql)
            for item in sql_result:
                ret_dict[item['id']] = item
        elif operation == '08':
            sql = "select r.id, r.objecturi, r.objectsize, r.version, r.stub_version, r.operation \
                from mad_res_upgrade as r, mad_msg as m \
                where r.is_delete = 0 and m.res_type='08' and (m.`status`=1 or m.`status` = 3) and m.res_id=r.id"
            sql_result = self.db.query(sql)
            for item in sql_result:
                ret_dict[item['id']] = item
        elif operation == '10':
            sql = "select r.id, r.objecturi, r.objectsize, r.operation, r.md5 \
                from mad_res_sendfile as r, mad_msg as m \
                where r.`status` = 2 and  m.res_type='10' and (m.`status`=1 or m.`status` = 3) and m.res_id=r.id"
            sql_result = self.db.query(sql)
            for item in sql_result:
                ret_dict[item['id']] = item
        else:
            # should never reach herr
            pass

        return ret_dict
    def _get_appkey_from_db(self,appkey):
        sql = "select * from mad_appkey where appkey='%s'" % (appkey)
        sql_result = self.db.query(sql)
        for sql_item in sql_result:
            return sql_item 
        return None

    def _strip_head_zero(self, operation):
        """ delete the head 0
        e.g. 01 -> 1

        @param operation, the operation to strp
        @return the striped string
        """
        if operation[0:1] == '0':
            return operation[1:2]
        else:
            return operation

    def _gen_resp_for_res(self, msgid, res_item):
        """convert resouce item to self.response

        @param: msgid
        @param: res_item, the resouce item from res table
        @return True if self.reponse is set; False: otherwise
        """
        result = True
        value_item = dict()
        #Suport batch msg , by ym , 2016-10-12
        value = []
        if self.response.has_key("applist") and len(self.response['applist']) > 0 and res_item['operation'] in ['01','02','03']: 
            value =  self.response['applist']
        elif self.response.has_key("caplist") and len(self.response['caplist']) > 0 and res_item['operation'] in ['04','06','07','08','10']: 
            value =  self.response['caplist']

        value_item['correlator'] = self.report['utdid']
        value_item['taskid'] = str(msgid)
        value_item['operation'] = res_item['operation']
        value_item['operation'] = self._strip_head_zero(value_item['operation'])
        if res_item['operation'] == '01':
            value_item['pkgname'] = res_item['pkgname']
            value_item['appname'] = res_item['appname']
            value_item['version'] = res_item['version']
            value_item['versionCode'] = str(res_item['versioncode'])
            value_item['objecturi'] = res_item['url']
            value_item['objectsize'] = str(res_item['size'])
            value_item['icon'] = res_item['icon']
            value_item['brief'] = res_item['brief']
            value.append(value_item)
            self.response['applist'] = value
        elif res_item['operation'] == '02':
            value_item['pkgname'] = res_item['pkgname']
            value_item['appname'] = res_item['appname']
            value_item['version'] = res_item['version']
            value_item['versionCode'] = str(res_item['versioncode'])
            value_item['objecturi'] = res_item['url']
            value_item['objectsize'] = str(res_item['size'])
            value_item['icon'] = res_item['icon']
            start_json = json.loads(res_item['start_json'])
            value_item['start'] = start_json
            value_item['brief'] = res_item['brief']
            value.append(value_item)
            self.response['applist'] = value
        elif res_item['operation'] == '03':
            value_item['pkgname'] = res_item['pkgname']
            value_item['version'] = res_item['version']
            value_item['versionCode'] = str(res_item['versioncode'])
            start_json = json.loads(res_item['start_json'])
            value_item['start'] = start_json
            value.append(value_item)
            self.response['applist'] = value
        elif res_item['operation'] == '04':
            value_item['pkgname'] = res_item['pkgname']
            value.append(value_item)
            self.response['caplist'] = value
        elif res_item['operation'] == '05':
            value_item['objecturi'] = res_item['url']
            value = value_item
            self.response['link'] = value
        elif res_item['operation'] == '06':
            # value = []
            value_item['icon'] = res_item['pic_url']
            value_item['brief'] = res_item['brief']
            value_item['title'] = res_item['title']
            value_item['action'] = res_item['action']
            if value_item['action'] == '01': # url
                value_item['objecturi'] = res_item['open_url']
            else:
                value_item['objectsize'] = str(res_item['size'])
                value_item['version'] = res_item['version']
                value_item['versionCode'] = str(res_item['versioncode'])
                value_item['pkgname'] = res_item['pkgname']
                value_item['objecturi'] = res_item['url']
                if res_item['should_start'] == 1:
                    start_json = json.loads(res_item['start_json'])
                    value_item['start'] = start_json
            value.append(value_item)
            self.response['caplist'] = value
        elif res_item['operation'] == '07':
            # value = []
            value_item['icon'] = res_item['icon_url']
            value_item['pic'] = res_item['pic_url']
            value_item['brief'] = res_item['brief']
            value_item['title'] = res_item['title']
            value_item['action'] = res_item['action']
            if value_item['action'] == '01': # url
                value_item['objecturi'] = res_item['open_url']
            else:
                value_item['objectsize'] = str(res_item['size'])
                value_item['version'] = res_item['version']
                value_item['versionCode'] = str(res_item['versioncode'])
                value_item['pkgname'] = res_item['pkgname']
                value_item['objecturi'] = res_item['url']
                if res_item['should_start'] == 1:
                    start_json = json.loads(res_item['start_json'])
                    value_item['start'] = start_json
            value.append(value_item)
            self.response['caplist'] = value
        elif res_item['operation'] == '08':
            # value = []
            value_item['version'] = res_item['version']
            value_item['objectsize'] = res_item['objectsize']
            value_item['objecturi'] = res_item['objecturi']
            value.append(value_item)
            self.response['caplist'] = value
        elif res_item['operation'] == '10':
            value_item['objectsize'] = res_item['objectsize']
            value_item['objecturi'] = res_item['objecturi']
            value_item['md5']       = res_item['md5']
            value.append(value_item)
            self.response['caplist'] = value
        else:
            # never reach here
            result = False
            pass
        return result

    def _generate_response(self, matched_msg):
        """generate self.response dict according to matched_msg

        1. get the resource content from memcache based on res_type and res_id
        2. if memcache is expired, read from db and populate to memcache
        3. generate self.response dict

        @param matched_msg: the message dict
        @return True: self.response is set; False:otherwise
        """
        result = False
        operation = '0102'
        # res memcache key is format as res-03, for 01,02, use the same key res-0102
        if matched_msg['res_type'] != '01' and matched_msg['res_type'] != '02':
            operation = matched_msg['res_type']
        res_key = 'res-%s' % operation
        res_value = mc.get(res_key)
        if res_value is None:
            res_value = self._get_res_from_db(operation)
            mc.set(res_key,res_value,options.test_online_msgs_expiretime)
        # convert res_value to self.response
        if res_value.has_key(matched_msg['res_id']):
            res_value_item = res_value[matched_msg['res_id']]
            result = self._gen_resp_for_res(matched_msg['id'], res_value_item)
        return result

    @gen.coroutine
    def _insert_mad_device_msg(self, utdid, need_interval):
       
        # sql = "INSERT INTO mad_device_msg (ctime, utime, utdid, msgids) \
        #     values ('%s', '%s', '%s', '%s')" % (self.now, self.now, utdid, pull_msg_ids)
        # if need_interval == 1:
        
        device_msg_list = []

        self.device_msg_key.append('ctime')
        self.device_msg_value.append(self.now)

        self.device_msg_key.append('utdid')
        self.device_msg_value.append(str(utdid))

        device_msg_list.append(tuple(self.device_msg_value))
        
        qmarks = ','.join(['%s']*len(self.device_msg_value))
        cols = ','.join(self.device_msg_key)
        sql = "INSERT INTO mad_device_msg (%s) VALUES (%s)" % (cols, qmarks)
        self.db.executemany(sql,device_msg_list)


    @gen.coroutine
    def _update_mad_device_msg(self, utdid, need_interval):
        device_msg_list = []
        device_msg_list.append(tuple(self.device_msg_value))

        set_str = "utime = %s, msgids = %s, interval_num = %s, normal_num = %s, multi_num = %s, infinite_num = %s"

        if str(self.last_interval_time) != '': 
            set_str = set_str + " ,last_interval_time = %s"

        if str(self.last_normal_time) != '':
            set_str = set_str + " ,last_normal_time = %s"

        if str(self.last_multi_time) != '':
            set_str = set_str + " ,last_multi_time = %s"

        if str(self.last_infinite_time) != '':
            set_str = set_str + " ,last_infinite_time = %s"
        
        sql = "UPDATE mad_device_msg set %s where utdid = '%s'" % (set_str,utdid)
        self.db.executemany(sql,device_msg_list)

    def _add_pulled_msg(self, msgid, need_interval, multi_pull):
        """record the pulled msgid to the device in both memcache and db table mad_device_msg
        """
        multi_bit_mask = self.multi_bit_mask_dict[str(msgid)] if self.multi_bit_mask_dict.has_key(str(msgid)) else None

        dev_key = 'pulledmsg-%s' % self.report['utdid']
        dev_value = mc.get(dev_key)

        if dev_value is None:
            msg_value = msgid
            if len(multi_pull) > 0 and multi_bit_mask is not None:
                msg_value = str(msgid)+":"+str(self.timestamp)+":"+str(multi_bit_mask)
            dev_value = ',%s,' % msg_value
            dev_value = dev_value + '|' + str(self.last_interval_time) + '|' + str(self.last_normal_time) + '|' + str(self.last_multi_time) + '|' + str(self.last_infinite_time) + '|' + str(self.interval_num) + '|' + str(self.normal_num) + '|' + str(self.multi_num) + '|' + str(self.infinite_num)
            mc.set(dev_key, dev_value, 0)
            pull_msg_ids = str(dev_value).split('|')[0]
            self._get_mad_device_msg_key_value(pull_msg_ids)
            self._insert_mad_device_msg(self.report['utdid'], need_interval)
        else:
            dev_value_arr = str(dev_value).split('|')
            pull_msg_dict = self._convert_pullmsg_to_dict(str(dev_value_arr[0]))
            if pull_msg_dict.has_key(str(msgid)): #the msg has been pulled before
                if len(multi_pull) > 0 and multi_bit_mask is not None: # the msg is multi_pull msg
                    pull_msg_dict[str(msgid)]['bitmask'] = multi_bit_mask
            else: # the msg is pulled firstly
                if len(multi_pull) > 0 and multi_bit_mask is not None: # the msg is multi_pull msg
                    pull_msg_dict[str(msgid)]={'multi':1, 'first_time':self.timestamp, 'bitmask':multi_bit_mask}
                else:
                    pull_msg_dict[str(msgid)]={'multi':0}
            pull_msg_content = self._convert_dict_to_pullmsg(pull_msg_dict)
            dev_value = pull_msg_content + '|' + str(self.last_interval_time) + '|' + str(self.last_normal_time) + '|' + str(self.last_multi_time) + '|' + str(self.last_infinite_time) + '|' + str(self.interval_num) + '|' + str(self.normal_num) + '|' + str(self.multi_num) + '|' + str(self.infinite_num)
            mc.set(dev_key, dev_value, 0)
            pull_msg_ids = str(dev_value).split('|')[0]
            self._get_mad_device_msg_key_value(pull_msg_ids)
            self._update_mad_device_msg(self.report['utdid'], need_interval)

    @gen.coroutine
    def _increase_msg_cur_num_db(self, msgid, cur_num):
        """Async method to increase mad_msg cur_num
        """
        self.db.execute("UPDATE mad_msg set cur_num = cur_num + 1  WHERE id = %s and cur_num < max_num ", msgid)
        rds.incr('cur_num_' + str(msgid))

    @gen.coroutine
    def _increase_msg_multi_num_db(self, msgid, interval_days):
        """Async method to increase mad_msg cur_num
        """
        sql = "UPDATE mad_msg_multi_pull_count set pull_count = pull_count+1, utime='%s'  WHERE msg_id = %s and after_days = %s " % (self.now, msgid, interval_days)
        count = self.db.execute_rowcount(sql)
        if count == 0:
            sql = "insert into mad_msg_multi_pull_count(ctime, utime, msg_id, after_days, pull_count) values('%s','%s', %s, %s, 1)" % (self.now, self.now, msgid, interval_days)
            self.db.execute(sql)
            
    def _process(self):
        """query matched message for incoming device
           return  matching message

        1. try to get test state message, if the incoming device is test dev for the appkey
        2. if test message is not getten, try to get normal message
        3. get the matched msgid, and corresponding res, generate the down json.
        """

        #get appkey
        appkey_key = 'appkey-%s' % self.report['appid']
        appkey_value = mc.get(appkey_key)
        if appkey_value is None:
            appkey_value = self._get_appkey_from_db(self.report['appid'])
            mc.set(appkey_key,appkey_value,options.appkey_expiretime)
        #get multiple_msg 
        if appkey_value is not None and appkey_value.has_key('multiple_msg') and appkey_value['multiple_msg'] == 2 :
            self.multiple_msg = 2 

        # try:
        self.multi_days_dict = dict()
        self.multi_bit_mask_dict = dict()
        #key is msg id,type is string
        self.pull_msg_set = set()
        self.pull_msg_dict = dict()
        #定义设备各类型策略最后拉取时间和拉取次数
        self.last_interval_time = ''
        self.last_normal_time = ''
        self.last_multi_time = ''
        self.last_infinite_time = ''
        self.interval_num = 0
        self.normal_num = 0
        self.multi_num = 0
        self.infinite_num = 0

        #get pulledmsg
        dev_key = 'pulledmsg-%s' % self.report['utdid']
        pullmsg_value = mc.get(dev_key)

        #注掉，需还原
        if pullmsg_value is None:
            pullmsg_value = self._get_pullmsg_value_by_utdid()
        if pullmsg_value is not None:
            dev_value_arr = str(pullmsg_value).split('|')
            self.pull_msg_dict = self._convert_pullmsg_to_dict(str(dev_value_arr[0]))
            self.pull_msg_set  = set(self.pull_msg_dict.keys())
            #获取该设备各类型策略拉取次数和最后拉取时间
            dev_value_arr_len = len(dev_value_arr)
            if dev_value_arr_len >=2:
                self.last_interval_time = str(dev_value_arr[1])
            if dev_value_arr_len >=3:
                self.last_normal_time = str(dev_value_arr[2])
            if dev_value_arr_len >=4:
                self.last_multi_time = str(dev_value_arr[3])
            if dev_value_arr_len >=5:
                self.last_infinite_time = str(dev_value_arr[4])

            now = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
            now = datetime.datetime.strptime(now, '%Y-%m-%d %H:%M:%S')

            if self.last_interval_time != '':
                last_interval_time = datetime.datetime.strptime(self.last_interval_time, '%Y-%m-%d %H:%M:%S')
                diff_day_interval = (now - last_interval_time).days
                if diff_day_interval > 0:
                    self.interval_num = 0
                else:
                    self.interval_num = int(dev_value_arr[5])

            if self.last_normal_time != '':
                last_normal_time = datetime.datetime.strptime(self.last_normal_time, '%Y-%m-%d %H:%M:%S')
                diff_day_normal = (now - last_normal_time).days
                if diff_day_normal > 0:
                    self.normal_num = 0
                else:
                    self.normal_num = int(dev_value_arr[6])

            if self.last_multi_time != '':
                last_multi_time = datetime.datetime.strptime(self.last_multi_time, '%Y-%m-%d %H:%M:%S')
                diff_day_multi = (now - last_multi_time).days
                if diff_day_multi > 0:
                    self.multi_num = 0
                else:
                    self.multi_num = int(dev_value_arr[7])

            if self.last_infinite_time != '':
                last_infinite_time = datetime.datetime.strptime(self.last_infinite_time, '%Y-%m-%d %H:%M:%S')
                diff_day_infinite = (now - last_infinite_time).days
                if diff_day_infinite > 0:
                    self.infinite_num = 0
                else:
                    self.infinite_num = int(dev_value_arr[8])  
            
        is_test = False
        matched_msg_list = self._try_to_get_test_msg()
        if matched_msg_list is None:
            matched_msg_list = self._try_to_get_online_msg()
        else:
            is_test = True
        # matched_msg_list = self._try_to_filter_msgs(matched_msg_list);
       
        if matched_msg_list is not None:
            for matched_msg in matched_msg_list:

                if len(matched_msg['multi_pull']) > 0 :
                    self.last_multi_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    self.multi_num = self.multi_num + 1
                elif matched_msg['infinite_pull'] == 2 :
                    self.last_infinite_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    self.infinite_num = self.infinite_num + 1
                elif matched_msg['need_interval'] == 1 :
                    self.last_interval_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    self.interval_num = self.interval_num + 1
                else:
                    self.last_normal_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    self.normal_num = self.normal_num + 1

                ret = self._generate_response(matched_msg)
                if ret:
                    self._add_pulled_msg(matched_msg['id'], matched_msg['need_interval'], matched_msg['multi_pull'])
                    if not is_test: # if it's online msg, increase the msg count
                        if matched_msg.has_key('infinite_pull') and matched_msg['infinite_pull'] == 2 and str(matched_msg['id']) in self.pull_msg_set:
                        #this msg is infinite_pull type msg and before pull
                            continue
                        multi_days = self.multi_days_dict[str(matched_msg['id'])] if self.multi_days_dict.has_key(str(matched_msg['id'])) else None
                        if multi_days is None or multi_days == 0: # first pull
                            matched_msg['cur_num'] = matched_msg['cur_num'] + 1
                            #mc.set('test-online-msgs', self.cachedmsgs, options.test_online_msgs_expiretime)
                            self._increase_msg_cur_num_db(matched_msg['id'], matched_msg['cur_num'])
                        else:
                            self._increase_msg_multi_num_db(matched_msg['id'], multi_days)


    
        # except Exception, e:
        #     self.response['code'] = "1502"
        #     logging.error(str(e))
        #     self._response_report()
        #     return


    def _try_to_filter_msgs(self , msg_list):
        """ if 08 msg.version greater than client version return 08 msg
        """
        if msg_list is not None :
            msgs = {'first':[],'second':[],'third':[]}
            msg_type_filter = {'first':['01','02','03'],'second':['05'],'third':['04','06','07','08']}
            
            infinite_pull_list = list()
            for matched_msg in msg_list:
                # infinite_pull type msg first pull
                if matched_msg.has_key('infinite_pull') and matched_msg['infinite_pull'] == 2:
                    infinite_pull_list.append(matched_msg)
                else :
                    for seq , capabilitys in msg_type_filter.items():                
                        if matched_msg['res_type'] in capabilitys :
                            msgs[seq].append(matched_msg)
            
            result = list()
            if len(msgs['first']) > 0 :
                result = msgs['first']
            elif len(msgs['second']) > 0 :
                result = msgs['second']
            elif len(msgs['third']) > 0 :
                result = msgs['third']

            if len(infinite_pull_list + result) > 0 :
                return infinite_pull_list + result

        return None

    def _process_status(self):
        """process the status report from devices.
           just write the log to corresponding log file
        """

        #2017年6月14日16:08:05 新增定制需求
        #当上报策略ID 为03,05策略，且该策略API有值时，则调用定制接口
        if self.report['recode'] in (1415,1412) :
            task_id = int(self.report['job_id'])
            api_msgs = self._get_msg_by_api()
 
            if api_msgs.has_key(task_id):
                # 1: 定制需求1
                if api_msgs[task_id] == 1:
                    imei = self._get_device_imei(self.report['run_id'] , 'imei')
                    #接口url
                    api_url = "https://promotion-partner.gifshow.com/rest/n/promotion/p?adid=27&imei=%s&ip=%s" % (imei ,self.ip )
                    post_data = {'mac':'','netType':'','phoneModel':'','Isp':''}
                    api_response = requests.post(url = api_url ,data = post_data , timeout = 3 , verify=False)
                    api_log_file_name = "api_msgs_%s.log" % time.strftime("%Y-%m-%d")
                    api_target = file(os.path.join(self.statis_msg_api_dir,api_log_file_name),'a')                    
                    try:
                        logstr = '['+time.strftime("%Y-%m-%d %H:%M:%S")+'] '+ api_url.encode('UTF-8') + '\n'
                        api_target.write(logstr)

                        logstr = '['+time.strftime("%Y-%m-%d %H:%M:%S")+'] '+ api_response.text.encode('UTF-8') + '\n'
                        api_target.write(logstr)
                    except Exception as e:
                        logstr = '['+time.strftime("%Y-%m-%d %H:%M:%S")+'] Success'+ e + '\n'
                        api_target.write(logstr)
                        print e

        try:
            report = list()
            _runlog = self.report['run_log']
            logstr = '['+time.strftime("%Y-%m-%d %H:%M:%S")+'] '+_runlog + '\n'
            
            statis_msg_name = 'statis_msg_%s.log' % self.report['job_id']
            logfilename = os.path.join(self.statis_msg_dir, statis_msg_name) #'/mnt/mad/log/statis_msg/statis_msg_'+self.report['job_id']+'.log'
            target = file(logfilename, "a")
            target.write(logstr)

            log_dir = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'log')
            statis_msg_date_dir = os.path.join(log_dir, 'statis_msg_date')
            if not os.path.exists(statis_msg_date_dir):
                os.mkdir(statis_msg_date_dir)
            statis_msg_all = 'statis_msg_day_%s.log' % time.strftime("%Y%m%d")
            logfilename_all = os.path.join(statis_msg_date_dir, statis_msg_all) #'/mnt/mad/log/statis_msg_date/statis_msg_day_'+time.strftime("%Y-%m-%d")+'.log'
            target_all = file(logfilename_all, "a")
            target_all.write(logstr)

            return
        except Exception, e:
            return    

    def _pull_status(self):
        """process the status report from devices.
           just write the log to corresponding log file
        """
        try:
            runlog = json.dumps(self.pull_report)
            logstr = '['+time.strftime("%Y-%m-%d %H:%M:%S")+'] '+runlog + '\n'

            statis_pull = 'statis_pull_%s.log' % time.strftime("%Y%m%d")
            log_dir = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'log')
            statis_pull_dir = os.path.join(log_dir, 'statis_pull')
            if not os.path.exists(statis_pull_dir):
                os.mkdir(statis_pull_dir)
            logfilename = os.path.join(statis_pull_dir, statis_pull) #'/mnt/mad/log/statis_pull/statis_pull_'+time.strftime("%Y-%m-%d")+'.log'
            target = file(logfilename, "a")
            target.write(logstr)
            
            return
        except Exception, e:
            return  

    def _get_pullmsg_value_by_utdid(self):

        """
            根据utdid去数据库mad_device_msg表读取该策略拉取过哪些策略及策略类型当天拉取次数和该类型最后拉取时间
        """

        try:
            sql = "SELECT id,utdid,msgids,last_interval_time,last_normal_time,last_multi_time,last_infinite_time,interval_num,normal_num,multi_num,infinite_num from mad_device_msg where utdid = '%s' " % self.report['utdid']
            device_list = self.db.query(sql)
            for dev in device_list:
                key = "pulledmsg-" + str(dev['utdid'])
                val = str(dev['msgids'])

                if dev.has_key('last_interval_time'):
                    if dev['last_interval_time'] is None:
                        dev['last_interval_time'] = ''
                    val = val+"|"+str(dev['last_interval_time'])
                else:
                    val = val+"|"

                if dev.has_key('last_normal_time'):
                    if dev['last_normal_time'] is None:
                        dev['last_normal_time'] = ''
                    val = val+"|"+str(dev['last_normal_time'])
                else:
                    val = val+"|"

                if dev.has_key('last_multi_time'):
                    if dev['last_multi_time'] is None:
                        dev['last_multi_time'] = ''
                    val = val+"|"+str(dev['last_multi_time'])
                else:
                    val = val+"|"

                if dev.has_key('last_infinite_time'):
                    if dev['last_infinite_time'] is None:
                        dev['last_infinite_time'] = ''
                    val = val+"|"+str(dev['last_infinite_time'])
                else:
                    val = val+"|"

                if dev.has_key('interval_num'):
                    if dev['interval_num'] is None:
                        dev['interval_num'] = 0
                    val = val+"|"+str(dev['interval_num'])
                else:
                    val = val+"|0"

                if dev.has_key('normal_num'):
                    if dev['normal_num'] is None:
                        dev['normal_num'] = 0
                    val = val+"|"+str(dev['normal_num'])
                else:
                    val = val+"|0"

                if dev.has_key('multi_num'):
                    if dev['multi_num'] is None:
                        dev['multi_num'] = 0
                    val = val+"|"+str(dev['multi_num'])
                else:
                    val = val+"|0"

                if dev.has_key('infinite_num'):
                    if dev['infinite_num'] is None:
                        dev['infinite_num'] = 0
                    val = val+"|"+str(dev['infinite_num'])
                else:
                    val = val+"|0"
                    
                mc.set(key,val,0)
            return val

        except Exception, e:
            return 

    def _get_order_msg_time_num(self):

        """
            数据库mad_settings表读取定义的策略类型每天拉取次数和拉取时间间隔
        """

        ret = dict()
        sql = "SELECT name,value from mad_settings"
        setting_list = self.db.query(sql)
       
        for setting in setting_list:
         
            ret[setting['name']] = setting['value']

        return ret

    def _get_mad_device_msg_key_value(self,pull_msg_ids):

        """
            返回mad_device_mag表动态添加的字段和值
        """
        self.device_msg_key = []
        self.device_msg_value = []

        self.device_msg_key.append('utime')
        self.device_msg_value.append(self.now)

        self.device_msg_key.append('msgids')
        self.device_msg_value.append(pull_msg_ids)

        self.device_msg_key.append('interval_num')
        self.device_msg_value.append(int(self.interval_num))

        self.device_msg_key.append('normal_num')
        self.device_msg_value.append(int(self.normal_num))

        self.device_msg_key.append('multi_num')
        self.device_msg_value.append(int(self.multi_num))

        self.device_msg_key.append('infinite_num')
        self.device_msg_value.append(int(self.infinite_num))

        if str(self.last_interval_time) != '':
            self.device_msg_key.append('last_interval_time')
            self.device_msg_value.append(self.last_interval_time)

        if str(self.last_normal_time) != '':
            self.device_msg_key.append('last_normal_time')
            self.device_msg_value.append(self.last_normal_time)

        if str(self.last_multi_time) != '':
            self.device_msg_key.append('last_multi_time')
            self.device_msg_value.append(self.last_multi_time)

        if str(self.last_infinite_time) != '':
            self.device_msg_key.append('last_infinite_time')
            self.device_msg_value.append(self.last_infinite_time)
        
    def _get_temp_by_redis(self,temp_type,id_type):

        """
            判断设备是否在redis缓存允许下发的国家/省份/城市中
        """

        result = 0
        temp_key = "temp_%s" % temp_type
        temp_value = rds.get(temp_key)
        id_str = ",%s," % str(id_type)
        if (temp_value is not None) and (id_str in temp_value):
            result = 1

        return result
def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()        



if __name__ == "__main__":
    main()
