#!/usr/bin/env python3
# -*- coding: utf-8 -*-


'''
query_condition_dict = dict()

query_condition_dict['platform = ?'] = ('haodaxue',)
query_condition_dict["course_id = ? AND crawled_plan_num >= ?"] = ("hsdrhbe", 1)
query_condition_dict['save_time BETWEEN ? AND ?'] = ('2018-09-11','2018-09-30')

conditions_list = list()
conditions_list.append(query_condition_dict)
hiveApi = HiveApi()
cursor = conn.cursor()
cursor.execute('show tables')
print(as_pandas(cursor))
data_dict = dict()
data_dict['platform'] = 'haodaxue'
data_dict['save_time'] = '2018-09-12'
data_dict['crawled_plan_num'] = 5
conditions_dict = data_dict
data_dict['crawled_time_gap'] = 30
data_list = list()
data_list.append(data_dict)
print(data_list)
hiveApi.update(table_name='task_info', data_list=data_list,conditions_list=conditions_list)
res = hiveApi.query(table_name='task_info',conditions = query_condition_dict)
print(res)
hiveApi.delete(table_name='task_info',conditions = query_condition_dict)
res = hiveApi.query(table_name='task_info',conditions = query_condition_dict)
print(res)


data_dict = dict()
data_dict['forum_post_type'] = 2
data_list = list()
data_list.append(data_dict)
con_dict = dict()
conditions_list = list()
con_dict['forum_post_id = ?'] = ('199c1ef2-0de5-11ea-8f13-acde48001122',)
conditions_list.append(con_dict)
mysqlApi = MysqlApi()
#mysqlApi.update(table_name='forum_post_info', data_list=data_list, conditions_list=conditions_list)
#mysqlApi.delete(table_name='forum_post_info', conditions={})
time.sleep(900)
mysqlApi.query_table_description(table_name='forum_post_info')

#cursor = conn.cursor()
#cursor.execute('alter table schedule_task change column course_id course_id string')
#cursor.execute('desc schedule_task')
#print(as_pandas(cursor))
from persistence.db.api.mysql_api import MysqlApi

api = MysqlApi()
data_dict = dict()
data_dict['forum_post_id'] = "区Provence-Alpes-Côte d'Azur，沃克吕兹省Vaucluse"
data_list = list()
data_list.append(data_dict)
api.insert(table_name='forum_post_info',data_list=data_list)'''
import datetime

ret_set = set()
print(len(ret_set))
ret_set.add("fasfasfas")
print(len(ret_set))
ret_set.add("1")

print(len(ret_set))
ret_set.add("1")
print(len(ret_set))

print("评论(0)".strip()[3:-1])
print(type(datetime.datetime.now().minute))
print(datetime.datetime.now().timestamp())








