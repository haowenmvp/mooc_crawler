#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from impala.dbapi import connect
import logging
import settings
import datetime

from impala.util import as_pandas

from persistence.db.api.base_api import BaseApi


class HiveApi(BaseApi):
    def __init__(self, host='222.20.95.42', port=10001, username='hadoop', password='hadoop', database='mooc', auth_mechanism='PLAIN'):
        super().__init__(host, port, username, password, database)
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database
        self.__auth_mechanism = auth_mechanism
        self.__cursor = connect(host=self.__host, port=self.__port, database=self.__database, user=self.__user, password=self.__password, auth_mechanism=self.__auth_mechanism).cursor()

    #根据查询条件，返回list（tuple）
    def query(self, table_name:str, query_field_list=None, conditions=None):
        if query_field_list is None:
            query_field_list = []
        if conditions is None:
            conditions = {}
        sql = ''
        condition = ''
        if len(query_field_list) == 0:
            sql += 'select * from ' + table_name
        else:
            return_list1 = query_field_list[0]
            for i in range(1, len(query_field_list)):
                return_list1 += ',' + query_field_list[i]
            sql += 'select ' +return_list1 + ' from ' + table_name

        if len(conditions) != 0:
            for string in conditions.keys():
                values = conditions[string]
                if string.count("?") == len(values):
                    for j in range(len(values)):
                        if type(values[j]) == type(0):
                            string = string.replace("?", str(values[j]), 1)
                        elif type(values[j]) == type(datetime.datetime.now()):
                            string = string.replace("?", "'" + values[j].strftime("%Y-%m-%d %H:%M:%S") + "'", 1)
                        elif type(values[j]) == type(True):
                            string = string.replace("?", str(values[j]), 1)
                        else:
                            string3 = values[j]
                            count = string3.count('\n\r')
                            string1 = string3.replace('\n\r', '', count)
                            count = string1.count('\n')
                            string2 = string1.replace('\n', '', count)
                            string = string.replace("?", "'" + string2 + "'", 1)
                else:
                    return list(tuple())
                condition += ' and ' + '(' + string + ')'
            sql += ' where ' + '1=1' + condition
        logging.info(sql)

        self.__cursor.execute(sql)
        results = self.__cursor.fetchall()
        return results

    def insert(self, table_name:str, data_list:list):
        if len(data_list) == 0:
            return
        sql = 'insert into ' + table_name + '('
        table_name_list = ''
        keys = []
        values = ''
        dict0 = data_list[0]
        for string in dict0.keys():
            table_name_list += string + ','
            keys.append(string)
        sql += table_name_list[0:-1] + ') '

        for i in range(len(data_list)):
            values += '('
            for j in range(len(keys)):
                if j == len(keys) - 1:
                    if type(data_list[i][keys[j]]) == type(1):
                        values += str(data_list[i][keys[j]]) + ')'
                    elif type(data_list[i][keys[j]]) == type(datetime.datetime.now()):
                        values += "'" + data_list[i][keys[j]].strftime("%Y-%m-%d %H:%M:%S") + "')"
                    elif type(data_list[i][keys[j]]) == type(True):
                        values += str(data_list[i][keys[j]]) + ")"
                    else:
                        string = data_list[i][keys[j]]
                        count = string.count('\n\r')
                        string1 = string.replace('\n\r', '', count)
                        count = string1.count('\n')
                        string2 = string1.replace('\n', '', count)
                        values += "'" + string2 + "')"
                else:
                    if type(data_list[i][keys[j]]) == type(1):
                        values += str(data_list[i][keys[j]]) + ','
                    elif type(data_list[i][keys[j]]) == type(datetime.datetime.now()):
                        values += "'" + data_list[i][keys[j]].strftime("%Y-%m-%d %H:%M:%S") + "',"
                    elif type(data_list[i][keys[j]]) == type(True):
                        values += str(data_list[i][keys[j]]) + ","
                    else:
                        string = data_list[i][keys[j]]
                        count = string.count('\n\r')
                        string1 = string.replace('\n\r', '', count)
                        count = string1.count('\n')
                        string2 = string1.replace('\n', '', count)
                        values += "'" + string2 + "',"
            if i != len(data_list) - 1:
                values += ','
        sql += 'values' + values
        logging.info(sql)
        self.__cursor.execute(sql)

    def update(self, table_name : str, data_list : list, conditions_list : list):
        if len(data_list) == 0 or len(conditions_list) == 0:
            return
        for i in range(len(data_list)):
            sql = 'update ' + table_name + ' set '
            for kv in data_list[i].items():
                if type(kv[1]) == type(0):
                    sql += kv[0] + "=" + str(kv[1]) + ","
                elif type(kv[1]) == type(datetime.datetime.now()):
                    sql += kv[0] + "='" + kv[1].strftime("%Y-%m-%d %H:%M:%S") + "',"
                elif type(kv[1]) == type(True):
                    sql += kv[0] + "=" + str(kv[1]) + ","
                else:
                    string = kv[1]
                    count = string.count('\n\r')
                    string1 = string.replace('\n\r', '', count)
                    count = string1.count('\n')
                    string2 = string1.replace('\n', '', count)
                    sql += kv[0] + "='" + string2 + "',"
            sql = sql[0:-1] + " where 1=1 "
            conditions = conditions_list[i]
            if len(conditions) != 0:
                for string in conditions.keys():
                    values = conditions[string]
                    if string.count("?") == len(values):
                        for j in range(len(values)):
                            if type(values[j]) == type(0):
                                string = string.replace("?", str(values[j]), 1)
                            elif type(values[j]) == type(datetime.datetime.now()):
                                string = string.replace("?", "'" + values[j].strftime("%Y-%m-%d %H:%M:%S") + "'", 1)
                            elif type(values[j]) == type(True):
                                string = string.replace("?", str(values[j]), 1)
                            else:
                                string3 = values[j]
                                count = string3.count('\n\r')
                                string1 = string3.replace('\n\r', '', count)
                                count = string1.count('\n')
                                string2 = string1.replace('\n', '', count)
                                string = string.replace("?", "'" + string2 + "'", 1)
                    else:
                        return
                    sql += ' and ' + '(' + string + ')'
            logging.info(sql)
            self.__cursor.execute(sql)

    def delete(self, table_name : str, conditions : dict):
        sql = 'delete from ' + table_name + ' where 1=1 '
        if len(conditions) != 0:
            for string in conditions.keys():
                values = conditions[string]
                if string.count("?") == len(values):
                    for j in range(len(values)):
                        if type(values[j]) == type(0):
                            string = string.replace("?", str(values[j]), 1)
                        elif type(values[j]) == type(datetime.datetime.now()):
                            string = string.replace("?", "'" + values[j].strftime("%Y-%m-%d %H:%M:%S") + "'", 1)
                        elif type(values[j]) == type(True):
                            string = string.replace("?", str(values[j]), 1)
                        else:
                            string3 = values[j]
                            count = string3.count('\n\r')
                            string1 = string3.replace('\n\r', '', count)
                            count = string1.count('\n')
                            string2 = string1.replace('\n', '', count)
                            string = string.replace("?", "'" + string2 + "'", 1)
                else:
                    return
                sql += ' and ' + '(' + string + ')'
        logging.info(sql)
        self.__cursor.execute(sql)

    def query_table_description(self, table_name : str):
        sql = 'desc ' + table_name
        logging.info()
        self.__cursor.execute(sql)
        logging.info(as_pandas(self.__cursor))

    def add_column(self, table_name : str, column_name : str, column_type : str):
        sql = 'alter table ' + table_name + ' add columns(' + column_name + ' ' + column_type + ')'
        logging.info(sql)
        self.__cursor.execute(sql)

    def change_column(self,  table_name : str, old_column_name : str, new_column_name : str, column_type : str):
        sql = 'alter table ' + table_name + ' change column ' + old_column_name + ' ' + new_column_name + ' ' + column_type
        logging.info(sql)
        self.__cursor.execute(sql)

    def show_databases(self):
        sql = 'show databases'
        logging.info(sql)
        self.__cursor.execute(sql)
        logging.info(as_pandas(self.__cursor))

    def show_tables(self):
        sql = 'show tables'
        logging.info(sql)
        self.__cursor.execute(sql)
        logging.info(as_pandas(self.__cursor))