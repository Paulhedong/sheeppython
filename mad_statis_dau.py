#!/usr/bin/python
# -*- coding: utf-8 -*-
"""calculate dau based on log files
   write the result to file and db

Overview
========

this python is runned on 00:30 everyday as following command:
python mad_dau.py yesterday 1 1 

Usage:
python mad_statis_dau.py param1 param2 param3
param1: duration, can be 'all', 'yesterday', 'today', '20160501-20160514'
param2: 0: not write to file; 1: write to file
param3: 0: not write to db; 1: write to db 

"""
#usage: parse protocol-80xx.log* files and calculate the active users, insert to mad_z_static_active_by_channel

import os
import sys
import re
import datetime
import time
import shutil
import json
import torndb
from tornado.options import define, options

define("mysql_host", default="127.0.0.1:3306", help="blog database host")
define("mysql_database", default="mad", help="blog database name")
define("mysql_user", default="mobilead", help="blog database user")
define("mysql_password", default="Mobad2016!", help="blog database password")

script, duration, write_file, write_db = sys.argv # duration is to calculate the date duration. 'all' or '20160301-20160302'; write_file can be 0 or 1, which means wether to write the result log

MIN_DATETIME = datetime.datetime.strptime('19000101', '%Y%m%d')
MAX_DATETIME = datetime.datetime.strptime('20300101', '%Y%m%d')
if duration == 'all':
    start_date = MIN_DATETIME
    end_date = MAX_DATETIME
elif duration == 'yesterday':
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    yesterday_str = str(yesterday)
    yesterday_time = datetime.datetime.strptime(yesterday_str, '%Y-%m-%d')
    start_date = yesterday_time
    end_date = yesterday_time + datetime.timedelta(days=1)
elif duration == 'today':
    today = datetime.date.today()
    today_str = str(today)
    today_time = datetime.datetime.strptime(today_str, '%Y-%m-%d')
    start_date = today_time
    end_date = today_time + datetime.timedelta(days=1)
else:
    tmp = duration.split('-')
    if len(tmp) != 2:
        print '1st param must be format as yyyymmdd-yyyymmdd'
        exit(1)
    try:
        start_date = datetime.datetime.strptime(tmp[0], '%Y%m%d')
        end_date = datetime.datetime.strptime(tmp[1], '%Y%m%d') + datetime.timedelta(days=1)
        if start_date >= end_date:
            print 'start date is later than end date'
            exit(1)
    except:
        print '1st param must be format as yyyymmdd-yyyymmdd'
        exit (1)

if write_file != '0' and write_file != '1':
    print '2nd param(write_file) must be 0 or 1'
    exit(1)

if write_db != '0' and write_db != '1':
    print '3rd param(write_db) must be 0 or 1'
    exit(1)

db = torndb.Connection(
        host=options.mysql_host, database=options.mysql_database,
        user=options.mysql_user, password=options.mysql_password)

now = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
base_dir = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'log')
protocol_dir = os.path.join(base_dir, 'protocol')
dau_dir = os.path.join(base_dir, 'dau')
if not os.path.exists(dau_dir):
    os.mkdir(dau_dir)

LOG_FILE_START_PATTERN = 'protocol-80'

# key is the date 'yyyy-mm-dd', value is a dict
#{
# begin_time:xxx, end_time:xxx,
#
# act_num:xxx,
# act_country:{country1:xxx, country2:xxx},
# act_location:{location1:xxx, location2:xxx},
# act_man:{man1:xxx, man2:xxx},
# act_model:{model1:xxx,model2:xxx},
# act_osv:{osv1:xxx, osv2:xxx},
# act_operator:{ope1:xxx, ope2:xxx},
# act_version:{v1:xxx, v2:xxx},
# act_appkey:{appkey1:xxx,appkey2:xxx},
#
# act_appkey_country:{appkey1:{country1:xxx, country2:xxx}, appkey2:{country2:xxx, country2:xxx}},
# act_appkey_location:{appkey1:{location1:xxx, location2:xxx}, appkey2:{location1:xxx, location2:xxx}},
# act_appkey_man:{appkey1:{man1:xxx, man2:xxx}, appkey2:{man1:xxx, man2:xxx}},
# act_appkey_model:{appkey1:{model1:xxx, model2:xxx}, appkey2:{model1:xxx, model2:xxx}},
# act_appkey_osv:{appkey1:{osv1:xxx, osv2:xxx}, appkey2:{osv1:xxx, osv2:xxx}},
# act_appkey_operator: {appkey1:{operator1:xxx, operator2:xxx}, appkey2:{operator1:xxx, operator2:xxx}},
# act_appkey_version:{appkey1:{v1:xxx, v2:xxx}, appkey2:{v1:xxx, v2:xxx}},
# act_appkey_channel:{appkey1:{channel1:xxx, channel2:xxx}, appkey2:{channel1:xxx, channel2:xxx}},
#
# act_appkey_c_country:{appkey1:{channel1:{country1:xxx, country2:xxx}, channel2:{country1:xxx, country2:xxx}}, appkey2: {channel1:{country1:xxx, country2:xxx}, channel2:{country1:xxx, country2:xxx}}},
# act_appkey_c_location:{appkey1:{channel1:{location1:xxx, location2:xxx}, channel2:{location1:xxx, location2:xxx}}, appkey2: {channel1:{location1:xxx, location2:xxx}, channel2:{location1:xxx, location2:xxx}}},
# act_appkey_c_man:{appkey1:{channel1:{man1:xxx, man2:xxx}, channel2:{man1:xxx, man2:xxx}}, appkey2: {channel1:{man1:xxx, man2:xxx}, channel2:{man1:xxx, man2:xxx}}},
# act_appkey_c_model:{appkey1:{channel1:{model1:xxx, model2:xxx}, channel2:{model1:xxx, model2:xxx}}, appkey2: {channel1:{model1:xxx, model2:xxx}, channel2:{model1:xxx, model2:xxx}}},
# act_appkey_c_osv:{appkey1:{channel1:{osv1:xxx, osv2:xxx}, channel2:{osv1:xxx, osv2:xxx}}, appkey2: {channel1:{osv1:xxx, osv2:xxx}, channel2:{osv1:xxx, osv2:xxx}}},
# act_appkey_c_operator:{appkey1:{channel1:{operator1:xxx, operator2:xxx}, channel2:{operator1:xxx, operator2:xxx}}, appkey2: {channel1:{operator1:xxx, operator2:xxx}, channel2:{operator1:xxx, operator2:xxx}}},
# act_appkey_c_version:{appkey1:{channel1:{v1:xxx, v2:xxx}, channel2:{v1:xxx, v2:xxx}}, appkey2: {channel1:{v1:xxx, v2:xxx}, channel2:{v1:xxx, v2:xxx}}}
#}
result = dict()

RET_KEY_BTIME = 'begin_time'
RET_KEY_ETIME = 'end_time'
RET_KEY_NUM = 'act_num'
RET_KEY_COUNTRY = 'act_country'
RET_KEY_LOCATION = 'act_location'
RET_KEY_MAN = 'act_man'
RET_KEY_MODEL = 'act_model'
RET_KEY_VERSION = 'act_version'
RET_KEY_OSV = 'act_osv'
RET_KEY_OPERATOR = 'act_operator'
RET_KEY_AK = 'act_ak'
RET_KEY_AK_COUNTRY = 'act_ak_country'
RET_KEY_AK_LOCATION = 'act_ak_location'
RET_KEY_AK_MAN = 'act_ak_man'
RET_KEY_AK_MODEL = 'act_ak_model'
RET_KEY_AK_CHANNEL = 'act_ak_channel'
RET_KEY_AK_VERSION = 'act_ak_version'
RET_KEY_AK_OSV = 'act_ak_osv'
RET_KEY_AK_OPERATOR = 'act_ak_operator'
RET_KEY_AK_C_COUNTRY = 'act_ak_c_country'
RET_KEY_AK_C_LOCATION = 'act_ak_c_location'
RET_KEY_AK_C_MAN = 'act_ak_c_man'
RET_KEY_AK_C_MODEL = 'act_ak_c_model'
RET_KEY_AK_C_VERSION = 'act_ak_c_version'
RET_KEY_AK_C_OSV = 'act_ak_c_osv'
RET_KEY_AK_C_OPERATOR = 'act_ak_c_operator'


# key is utdid, value is a dict
#{
# dates:{yyyymmdd1:appkey^channel^version|appkey2^channel^version, yyyymmdd2:appkey^channel^version|appkey2^channel^version},
# man:xxx, model:xxx, osv:xxx, operator:xxx, country:xxx, location:xxx
# }
temp_devices = dict()

TD_KEY_DATES = 'dates'
TD_KEY_MAN = 'man'
TD_KEY_MODEL = 'model'
TD_KEY_OSV = 'osv'
TD_KEY_OPERATOR = 'operator'
TD_KEY_COUNTRY = 'country'
TD_KEY_LOCATION = 'location'

# Following is the key definition in incoming json
JSON_KEY_UTDID = 'utdid'
JSON_KEY_OSV = 'osv'
JSON_KEY_MOD = 'mod'
JSON_KEY_MAN = 'man'
JSON_KEY_OPERATOR = 'operator'
JSON_KEY_CARRIER = 'carrier'
JSON_KEY_APPKEY = 'appid'
JSON_KEY_CHANNEL = 'channel'
JSON_KEY_VERSION = 'version'
JSON_KEY_COUNTRY = 'country_id'
JSON_KEY_LOCATION = 'location'


#process the begin_time and end_time for one day
def process_time_dur(theDateStr, theDateTime):
    if result.has_key(theDateStr):
        old_begin_time = result[theDateStr].get(RET_KEY_BTIME, MAX_DATETIME)
        old_end_time = result[theDateStr].get(RET_KEY_ETIME, MIN_DATETIME)
        if theDateTime < old_begin_time:
            result[theDateStr][RET_KEY_BTIME] = theDateTime
        if theDateTime > old_end_time:
            result[theDateStr][RET_KEY_ETIME] = theDateTime
    else:
        result_value = dict()
        result_value[RET_KEY_BTIME] = theDateTime
        result_value[RET_KEY_ETIME] = theDateTime
        result[theDateStr] = result_value


def add_num(theDateStr):
    if result.has_key(theDateStr):
        act_num = result[theDateStr].get(RET_KEY_NUM, 0)
        act_num = act_num + 1
        result[theDateStr][RET_KEY_NUM] = act_num
    else:
        value = dict()
        value[RET_KEY_NUM] = 1
        result[theDateStr] = value

def add_v(key_tag, theDateStr, v):
    if result.has_key(theDateStr):
        value = result[theDateStr]
        if value.has_key(key_tag):
            num = value[key_tag].get(v, 0)
            num = num + 1
            value[key_tag][v] = num
        else:
            v_dict = dict()
            v_dict[v] = 1
            value[key_tag] = v_dict
    else:
        value = dict()
        v_dict = dict()
        v_dict[v] = 1
        value[key_tag] = v_dict
        result[theDateStr] = value          

def add_ak_v(key_tag, theDateStr, ak, v):
    if result.has_key(theDateStr):
        value = result[theDateStr]
        if value.has_key(key_tag):
            ak_value = value[key_tag]
            if ak_value.has_key(ak):
                final_value = ak_value[ak]
                num = final_value.get(v, 0)
                num = num + 1
                final_value[v] = num
            else:
                final_value = dict()
                final_value[v] = 1
                ak_value[ak] = final_value
        else:
            ak_value = dict()
            final_value = dict()
            final_value[v] = 1
            ak_value[ak] = final_value
            value[key_tag] = ak_value
    else:
        value = dict()
        ak_value = dict()
        final_value = dict()
        final_value[v] = 1
        ak_value[ak] = final_value
        value[key_tag] = ak_value
        result[theDateStr] = value

def add_ak_channel_v(keytag, theDateStr, ak, c, v):
    if result.has_key(theDateStr):
        value = result[theDateStr]
        if value.has_key(keytag):
            ak_value = value[keytag]
            if ak_value.has_key(ak):
                c_value = ak_value[ak]
                if c_value.has_key(c):
                    final_value = c_value[c]
                    num = final_value.get(v, 0)
                    num = num + 1
                    final_value[v] = num
                else:
                    final_value = dict()
                    final_value[v] = 1
                    c_value[c] = final_value
            else:
                c_value = dict()
                final_value = dict()
                final_value[v] = 1
                c_value[c] = final_value
                ak_value[ak] = c_value
        else:
            ak_value = dict()
            c_value = dict()
            final_value = dict()
            final_value[v] = 1
            c_value[c] = final_value
            ak_value[ak] = c_value
            value[keytag] = ak_value
    else:
        value = dict()
        ak_value = dict()
        c_value = dict()
        final_value = dict()
        final_value[v] = 1
        c_value[c] = final_value
        ak_value[ak] = c_value
        value[keytag] = ak_value
        result[theDateStr] = value

def add_all(theDateStr, jsonContent):
    add_num(theDateStr)
    add_v(RET_KEY_COUNTRY, theDateStr, jsonContent[JSON_KEY_COUNTRY])
    add_v(RET_KEY_LOCATION, theDateStr, jsonContent[JSON_KEY_LOCATION])
    add_v(RET_KEY_MAN, theDateStr, jsonContent[JSON_KEY_MAN])
    add_v(RET_KEY_MODEL, theDateStr, jsonContent[JSON_KEY_MOD])
    add_v(RET_KEY_OSV, theDateStr, jsonContent[JSON_KEY_OSV])
    add_v(RET_KEY_OPERATOR, theDateStr, jsonContent[JSON_KEY_OPERATOR])
    add_v(RET_KEY_VERSION, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_VERSION])
    add_v(RET_KEY_AK, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY])

def add_ak_all(theDateStr, jsonContent):
    add_ak_v(RET_KEY_AK_COUNTRY, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_COUNTRY])
    add_ak_v(RET_KEY_AK_LOCATION, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_LOCATION])
    add_ak_v(RET_KEY_AK_MAN, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_MAN])
    add_ak_v(RET_KEY_AK_MODEL, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_MOD])
    add_ak_v(RET_KEY_AK_OSV, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_OSV])
    add_ak_v(RET_KEY_AK_OPERATOR, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_OPERATOR])
    add_ak_v(RET_KEY_AK_VERSION, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_CARRIER][JSON_KEY_VERSION])
    add_ak_v(RET_KEY_AK_CHANNEL, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_CARRIER][JSON_KEY_CHANNEL])

def add_ak_c_all(theDateStr, jsonContent):
    add_ak_channel_v(RET_KEY_AK_C_COUNTRY, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_CARRIER][JSON_KEY_CHANNEL], jsonContent[JSON_KEY_COUNTRY])
    add_ak_channel_v(RET_KEY_AK_C_LOCATION, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_CARRIER][JSON_KEY_CHANNEL], jsonContent[JSON_KEY_LOCATION])
    add_ak_channel_v(RET_KEY_AK_C_MAN, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_CARRIER][JSON_KEY_CHANNEL], jsonContent[JSON_KEY_MAN])
    add_ak_channel_v(RET_KEY_AK_C_MODEL, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_CARRIER][JSON_KEY_CHANNEL], jsonContent[JSON_KEY_MOD])
    add_ak_channel_v(RET_KEY_AK_C_OSV, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_CARRIER][JSON_KEY_CHANNEL], jsonContent[JSON_KEY_OSV])
    add_ak_channel_v(RET_KEY_AK_C_OPERATOR, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_CARRIER][JSON_KEY_CHANNEL], jsonContent[JSON_KEY_OPERATOR])
    add_ak_channel_v(RET_KEY_AK_C_VERSION, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_CARRIER][JSON_KEY_CHANNEL], jsonContent[JSON_KEY_CARRIER][JSON_KEY_VERSION])

#core logic to calculate the json content, fill result and temp_devices
#return True: add
#return False: do nothing
def calc_dau(theDateStr, jsonContent):
    ret_value = True
    match_all_array = [jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_CARRIER][JSON_KEY_CHANNEL], jsonContent[JSON_KEY_CARRIER][JSON_KEY_VERSION]]
    match_all_str = '^'.join(match_all_array)
    if temp_devices.has_key(jsonContent[JSON_KEY_UTDID]):
        dates = temp_devices[jsonContent[JSON_KEY_UTDID]][TD_KEY_DATES]
        if dates.has_key(theDateStr): # this device has reported on the same day
            dates_value = dates[theDateStr]
            if match_all_str in dates_value: # this device appkey^channel^version already recorded, just return False
                ret_value = False
            else: #this device report another appkey or version or channel
                match_ak_channel_str = str(jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY]+'^'+jsonContent[JSON_KEY_CARRIER][JSON_KEY_CHANNEL])
                match_version_str = str('^'+ jsonContent[JSON_KEY_CARRIER][JSON_KEY_VERSION])
                match_ak_str = str(jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY] + '^')

                # convert appkey1^channel1^version1|appkey2^channel2^version2, into list() which item is dict[ak:xxx, c:xxx, v:xxx]
                ak_c_v_str_array = dates_value.split('|')
                ak_c_v_array = list()
                for ak_c_v_str in ak_c_v_str_array:
                    if ak_c_v_str != '':
                        tmp_array = ak_c_v_str.split('^')
                        ak_c_v_dict = dict()
                        ak_c_v_dict['ak'] = tmp_array[0]
                        ak_c_v_dict['c'] = tmp_array[1]
                        ak_c_v_dict['v'] = tmp_array[2]
                        ak_c_v_array.append(ak_c_v_dict)
                
                if match_ak_str not in dates_value: # this device has reported a new appkey
                    add_v(RET_KEY_AK, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY])
                    add_ak_all(theDateStr, jsonContent)
                    add_ak_c_all(theDateStr, jsonContent)

                    if match_version_str not in dates_value: # this device has reported a new appkey and a new version
                        add_v(RET_KEY_VERSION, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_VERSION])
                        
                elif match_ak_channel_str not in dates_value: # this device has reported old appkey but new channel
                    add_ak_v(RET_KEY_AK_CHANNEL, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_CARRIER][JSON_KEY_CHANNEL])
                    #if this device has new version, add_ak_version
                    is_same_version = False
                    for ak_c_v_item in ak_c_v_array:
                        if ak_c_v_item['ak'] == jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY] and ak_c_v_item['v'] == jsonContent[JSON_KEY_CARRIER][JSON_KEY_VERSION]:
                            is_same_version = True
                            break
                    if is_same_version == False:
                        add_ak_v(RET_KEY_AK_VERSION, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_CARRIER][JSON_KEY_VERSION])

                    add_ak_c_all(theDateStr, jsonContent)
                    
                    if match_version_str not in dates_value: # this device has reported old appkey and a new version
                        add_v(RET_KEY_VERSION, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_VERSION])

                else: # this device has reported old appkey and old channel but new version
                    add_ak_v(RET_KEY_AK_VERSION, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_APPKEY], jsonContent[JSON_KEY_CARRIER][JSON_KEY_VERSION])
                    if match_version_str not in dates_value: # this device has reported old appkey and a new version
                        add_v(RET_KEY_VERSION, theDateStr, jsonContent[JSON_KEY_CARRIER][JSON_KEY_VERSION])
                # append new string
                dates[theDateStr] = str(dates_value + '|' + match_all_str)
                temp_devices[jsonContent[JSON_KEY_UTDID]][TD_KEY_DATES] = dates
            
        else: #  it's first time for this device to report on the day
            add_all(theDateStr, jsonContent)
            add_ak_all(theDateStr, jsonContent)
            add_ak_c_all(theDateStr, jsonContent)

            dates[theDateStr] = match_all_str
            temp_devices[jsonContent[JSON_KEY_UTDID]][TD_KEY_DATES] = dates
    else: # this device has never recorded
        add_all(theDateStr, jsonContent)
        add_ak_all(theDateStr, jsonContent)
        add_ak_c_all(theDateStr, jsonContent)

        value = dict()
        dates = dict()
        dates[theDateStr] = match_all_str
        value[TD_KEY_DATES] = dates
        value[TD_KEY_COUNTRY] = jsonContent[JSON_KEY_COUNTRY]
        value[TD_KEY_LOCATION] = jsonContent[JSON_KEY_LOCATION]
        value[TD_KEY_MAN] = jsonContent[JSON_KEY_MAN]
        value[TD_KEY_MODEL] = jsonContent[JSON_KEY_MOD]
        value[TD_KEY_OSV] = jsonContent[JSON_KEY_OSV]
        value[TD_KEY_OPERATOR] = jsonContent[JSON_KEY_OPERATOR]
        temp_devices[jsonContent[JSON_KEY_UTDID]] = value

    return ret_value

#process one line. fill result and temp_devices
#return 0: success; 1: earlier than start_date; 2: later than end_date; 3: exception
def process_one_line(line):
    theDateStr = ''
    jsonStr = ''
    jsonContent = dict()
    
    try:
        theDateStr = line[1:11]
        theDateTime = datetime.datetime.strptime(line[1:20], '%Y-%m-%d %H:%M:%S')
        if theDateTime < start_date:
            return 1
        if theDateTime >= end_date:
            return 2
        jsonStr = line[22:len(line)]
        jsonContent = json.loads(jsonStr)
        jsonContent[JSON_KEY_OPERATOR] = jsonContent[JSON_KEY_OPERATOR].lower()
        jsonContent[JSON_KEY_MAN] = jsonContent[JSON_KEY_MAN].strip('^').lower()
        jsonContent[JSON_KEY_MOD] = jsonContent[JSON_KEY_MOD].strip('^').lower()

        process_time_dur(theDateStr, theDateTime)

        calc_dau(theDateStr, jsonContent)
        
        return 0
    except Exception, e:
        return 3
        pass

def process_file(filepath):
    ret = -1
    lineno = 0
    if os.path.exists(filepath) and os.path.isfile(filepath):
        for line in open(filepath):
            #just to show progress
            lineno = lineno+1
##            if lineno%100000 == 0:
##                print lineno

            line = line.rstrip()
            if line.startswith('[201') and (line.endswith('}') or line.endswith('}\n') or line.endswith('}\r\n')) and 'carrier' in line:
                ret = process_one_line(line)
                if  ret== 2: # the log time is later than end_date, no need to process
                    break
    return ret

# each day for one file
def write_to_file():
    reload(sys)
    sys.setdefaultencoding('utf-8')

    #open all the result files, and store the file handler into dict
    dau_file_dict = dict()
    for k in result:
        dau_file_path = os.path.join(dau_dir, str('dau_'+ k))
        dau_file = file(dau_file_path, "w")
        strb = str(datetime.datetime.strftime(result[k][RET_KEY_BTIME], '%Y-%m-%d %H:%M:%S'))
        stre = str(datetime.datetime.strftime(result[k][RET_KEY_ETIME], '%Y-%m-%d %H:%M:%S'))
        dau_file.write(strb+'---'+stre+'\n')
        dau_file_dict[k] = dau_file
    
    for k,v in temp_devices.items():
        for dk,dv in v[TD_KEY_DATES].items():
            tmp_array = dv.split('|')
            for item in tmp_array:
                #utdid^appkey^channel^version^country_id^location^man^model^osv^operator
                write_line = k+'^'+item+'^'+str(v[TD_KEY_COUNTRY])+'^'+v[TD_KEY_LOCATION]+'^'+v[TD_KEY_MAN]+'^'+v[TD_KEY_MODEL]+'^'+v[TD_KEY_OSV]+'^'+v[TD_KEY_OPERATOR]+'\n'
                dau_file_dict[dk].write(write_line) 

# mad_z_statis_active_man, mad_z_statis_active_model, mad_z_statis_active_operator, mad_z_statis_active_osv, mad_z_statis_active_appkey, mad_z_statis_active_version
def write_to_db_1(dict_1, table_name, col_name, target_date, desc):
    sql = 'replace into %s (%s, target_date, num, `desc`, utime) values ("%s", "%s", %s, "%s", "%s")'
    if table_name == 'mad_z_statis_active_country': # country_id is int type
        sql = 'replace into %s (%s, target_date, num, `desc`, utime) values (%s, "%s", %s, "%s", "%s")'
    for k,v in dict_1.items():
        sql_exe = sql % (table_name, col_name, k, target_date, v, desc, now)
        print sql_exe
        try:
            db.execute(sql_exe)
        except Exception, e:
            print e
            pass

# mad_z_statis_active_appkey_man, mad_z_statis_active_appkey_model, mad_z_statis_active_appkey_operator, mad_z_statis_active_appkey_osv, mad_z_statis_active_appkey_version, mad_z_statis_active_appkey_channel
def write_to_db_ak(dict_ak, table_name, col_name, target_date, desc):
    sql = 'replace into %s (app_key, %s, target_date, num, `desc`, utime) values ("%s", "%s", "%s", %s, "%s", "%s")'
    if table_name == 'mad_z_statis_active_appkey_country': # country_id is int type
        sql = 'replace into %s (app_key, %s, target_date, num, `desc`, utime) values ("%s", %s, "%s", %s, "%s", "%s")'

    for k,v in dict_ak.items():
        for k1, v1 in v.items():
            sql_exe = sql % (table_name, col_name, k, k1, target_date, v1, desc, now)
            print sql_exe
            try:
                db.execute(sql_exe)
            except Exception, e:
                print e
                pass
            
# mad_z_statis_active_appkey_channel_man, mad_z_statis_active_appkey_channel_model, mad_z_statis_active_appkey_channel_operator, mad_z_statis_active_appkey_channel_osv, mad_z_statis_active_appkey_channel_version
def write_to_db_ak_c(dict_ak_c, table_name, col_name, target_date, desc):
    sql = 'replace into %s (app_key, channel, %s, target_date, num, `desc`, utime) values ("%s", "%s", "%s", "%s", %s, "%s", "%s")'
    if table_name == 'mad_z_statis_active_appkey_channel_country': # country_id is int type
        sql = 'replace into %s (app_key, channel, %s, target_date, num, `desc`, utime) values ("%s", "%s", %s, "%s", %s, "%s", "%s")'
    for k_ak,v_ak in dict_ak_c.items():
        for k_c, v_c in v_ak.items():
            for k_v, v_v in v_c.items():
                sql_exe = sql % (table_name, col_name, k_ak, k_c, k_v, target_date, v_v, desc, now)
                print sql_exe
                try:
                    db.execute(sql_exe)
                except Exception, e:
                    print e
                    pass
    
    
def write_to_db():
    for k,v in result.items():
        strb = str(datetime.datetime.strftime(v[RET_KEY_BTIME], '%Y-%m-%d %H:%M:%S'))
        stre = str(datetime.datetime.strftime(v[RET_KEY_ETIME], '%Y-%m-%d %H:%M:%S'))
        desc = '%s-%s' % (strb, stre)
        
        #write to mad_z_statis_active_sum
        sql = 'replace into mad_z_statis_active_sum (target_date, num, `desc`, utime) values ("%s", %s, "%s", "%s")'  % (k, v[RET_KEY_NUM], desc, now)
        print sql
        db.execute(sql)
        
        #write to mad_z_statis_active_country
        write_to_db_1(v[RET_KEY_COUNTRY], 'mad_z_statis_active_country', 'country_id', k, desc)
        #write to mad_z_statis_active_location
        write_to_db_1(v[RET_KEY_LOCATION], 'mad_z_statis_active_location', 'location', k, desc)
        #write to mad_z_statis_active_man
        write_to_db_1(v[RET_KEY_MAN], 'mad_z_statis_active_man', 'man', k, desc)
        #write to mad_z_statis_active_model
        write_to_db_1(v[RET_KEY_MODEL], 'mad_z_statis_active_model', 'model', k, desc)
        #write to mad_z_statis_active_operator
        write_to_db_1(v[RET_KEY_OPERATOR], 'mad_z_statis_active_operator', 'operator', k, desc)
        #write to mad_z_statis_active_osv
        write_to_db_1(v[RET_KEY_OSV], 'mad_z_statis_active_osv', 'osv', k, desc)
        #write to mad_z_statis_active_appkey
        write_to_db_1(v[RET_KEY_AK], 'mad_z_statis_active_appkey', 'app_key', k, desc)
        #write to mad_z_statis_active_version
        write_to_db_1(v[RET_KEY_VERSION], 'mad_z_statis_active_version', 'version', k, desc)
        
        #write to mad_z_statis_active_appkey_country
        write_to_db_ak(v[RET_KEY_AK_COUNTRY], 'mad_z_statis_active_appkey_country', 'country_id', k, desc)
        #write to mad_z_statis_active_appkey_location
        write_to_db_ak(v[RET_KEY_AK_LOCATION], 'mad_z_statis_active_appkey_location', 'location', k, desc)
        #write to mad_z_statis_active_appkey_man
        write_to_db_ak(v[RET_KEY_AK_MAN], 'mad_z_statis_active_appkey_man', 'man', k, desc)
        #write to mad_z_statis_active_appkey_model
        write_to_db_ak(v[RET_KEY_AK_MODEL], 'mad_z_statis_active_appkey_model', 'model', k, desc)
        #write to mad_z_statis_active_appkey_operator
        write_to_db_ak(v[RET_KEY_AK_OPERATOR], 'mad_z_statis_active_appkey_operator', 'operator', k, desc)
        #write to mad_z_statis_active_appkey_osv
        write_to_db_ak(v[RET_KEY_AK_OSV], 'mad_z_statis_active_appkey_osv', 'osv', k, desc)
        #write to mad_z_statis_active_appkey_version
        write_to_db_ak(v[RET_KEY_AK_VERSION], 'mad_z_statis_active_appkey_version', 'version', k, desc)
        #write to mad_z_statis_active_appkey_channel
        write_to_db_ak(v[RET_KEY_AK_CHANNEL], 'mad_z_statis_active_appkey_channel', 'channel', k, desc)
        
        #write to mad_z_statis_active_appkey_channel_country
        write_to_db_ak_c(v[RET_KEY_AK_C_COUNTRY], 'mad_z_statis_active_appkey_channel_country', 'country_id', k, desc)
        #write to mad_z_statis_active_appkey_channel_location
        write_to_db_ak_c(v[RET_KEY_AK_C_LOCATION], 'mad_z_statis_active_appkey_channel_location', 'location', k, desc)
        #write to mad_z_statis_active_appkey_channel_man
        write_to_db_ak_c(v[RET_KEY_AK_C_MAN], 'mad_z_statis_active_appkey_channel_man', 'man', k, desc)
        #write to mad_z_statis_active_appkey_channel_model
        write_to_db_ak_c(v[RET_KEY_AK_C_MODEL], 'mad_z_statis_active_appkey_channel_model', 'model', k, desc)
        #write to mad_z_statis_active_appkey_channel_operator
        write_to_db_ak_c(v[RET_KEY_AK_C_OPERATOR], 'mad_z_statis_active_appkey_channel_operator', 'operator', k, desc)
        #write to mad_z_statis_active_appkey_channel_osv
        write_to_db_ak_c(v[RET_KEY_AK_C_OSV], 'mad_z_statis_active_appkey_channel_osv', 'osv', k, desc)
        #write to mad_z_statis_active_appkey_channel_version
        write_to_db_ak_c(v[RET_KEY_AK_C_VERSION], 'mad_z_statis_active_appkey_channel_version', 'version', k, desc)
       
    
def getFileCount(foldername):
    count = 0
    for filename in os.listdir(foldername):
        pathfile = foldername+'/'+filename
        if filename.startswith(LOG_FILE_START_PATTERN) and os.path.isfile(pathfile):
            count = count +1
    return count

def start():
    totalFiles = getFileCount(protocol_dir)
    fileno = 1
    
    for filename in os.listdir(protocol_dir):
        pathfile = os.path.join(protocol_dir, filename)
        if filename.startswith(LOG_FILE_START_PATTERN) and os.path.isfile(pathfile):
            print str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
            print "PROCESSING: %s(%s/%s)" %(pathfile,fileno, totalFiles)
            fileno = fileno + 1

            mtime = datetime.datetime.fromtimestamp(os.path.getmtime(pathfile))
            if mtime < start_date:
                continue
            process_file(pathfile)

    if write_file == '1':
        write_to_file()

    if write_db == '1':
        write_to_db()


    print str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))    

if __name__=='__main__':
    start()



