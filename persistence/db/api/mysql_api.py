#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector
import logging
import settings
import datetime
from impala.util import as_pandas
from persistence.db.api.base_api import BaseApi


class MysqlApi(BaseApi):
    def __init__(self, host='222.20.95.42', port=3307, username='mooc', password='Wst1532!@#', database='mooc'):
        super().__init__(host, port, username, password, database)
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database
        self.__conn = mysql.connector.connect(host=self.__host, user=self.__user, password=self.__password,
                                              database=self.__database, port=self.__port, use_unicode=True, charset="utf8")

    def connect(self):
        self.__conn = mysql.connector.connect(host=self.__host, user=self.__user, password=self.__password,
                                              database=self.__database, port=self.__port, use_unicode=True, charset="utf8")

    def fetch_all(self, sql):
        if not self.__conn.is_connected():
            self.connect()
        cursor = self.__conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        self.__conn.commit()
        return results

    def batch_all(self, sql_list: list):
        if not self.__conn.is_connected():
            self.connect()
        cursor = self.__conn.cursor()
        for sql in sql_list:
            print(sql)
            cursor.execute(sql)
        self.__conn.commit()
        cursor.close()

    # 根据查询条件，返回list（tuple）
    def query(self, table_name: str, query_field_list=None, conditions=None):
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
            sql += 'select ' + return_list1 + ' from ' + table_name

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
                        elif values[j] is None:
                            string = string.replace("?", 'NULL', 1)
                        else:
                            string3 = values[j].replace('\\', '\\\\').replace("'", "\\'").replace('"', '\\"')
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
        print(sql)
        if not self.__conn.is_connected():
            self.connect()
        cursor = self.__conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        self.__conn.commit()
        return results

    def query_if_not_and_insert(self, table_name: str, data_list: list, query_field_list=None, conditions=None):
        result_list = self.query(table_name=table_name, query_field_list=query_field_list, conditions=conditions)
        if len(result_list) != 0:
            return result_list
        try:
            self.insert(table_name=table_name, data_list=data_list)
            result_list = self.query(table_name=table_name, query_field_list=query_field_list, conditions=conditions)
        except Exception as e:
            logging.error(e)
        else:
            return result_list

    def insert(self, table_name: str, data_list: list):
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
                    if data_list[i][keys[j]] is None:
                        values += 'NULL' + ')'
                    elif type(data_list[i][keys[j]]) == type(1):
                        values += str(data_list[i][keys[j]]) + ')'
                    elif type(data_list[i][keys[j]]) == type(datetime.datetime.now()):
                        values += "'" + data_list[i][keys[j]].strftime("%Y-%m-%d %H:%M:%S") + "')"
                    elif type(data_list[i][keys[j]]) == type(True):
                        values += str(data_list[i][keys[j]]) + ")"
                    elif type(data_list[i][keys[j]]) == str:
                        string = data_list[i][keys[j]].replace('\\', '\\\\').replace("'", "\\'").replace('"', '\\"')
                        count = string.count('\n\r')
                        string1 = string.replace('\n\r', '', count)
                        count = string1.count('\n')
                        string2 = string1.replace('\n', '', count)
                        values += "'" + string2 + "')"
                    else:
                        values += str(data_list[i][keys[j]]) + ")"
                else:
                    if data_list[i][keys[j]] is None:
                        values += 'NULL' + ','
                    elif type(data_list[i][keys[j]]) == type(1):
                        values += str(data_list[i][keys[j]]) + ','
                    elif type(data_list[i][keys[j]]) == type(datetime.datetime.now()):
                        values += "'" + data_list[i][keys[j]].strftime("%Y-%m-%d %H:%M:%S") + "',"
                    elif type(data_list[i][keys[j]]) == type(True):
                        values += str(data_list[i][keys[j]]) + ","
                    elif type(data_list[i][keys[j]]) == str:
                        string = data_list[i][keys[j]].replace('\\', '\\\\').replace("'", "\\'").replace('"', '\\"')
                        count = string.count('\n\r')
                        string1 = string.replace('\n\r', '', count)
                        count = string1.count('\n')
                        string2 = string1.replace('\n', '', count)
                        values += "'" + string2 + "',"
                    else:
                        values += str(data_list[i][keys[j]]) + ','
            if i != len(data_list) - 1:
                values += ','
        sql += 'values' + values
        logging.info(sql)
        print(sql)
        if not self.__conn.is_connected():
            self.connect()
        cursor = self.__conn.cursor()
        cursor.execute(sql)
        logging.info("finish execute")
        self.__conn.commit()
        logging.info("connector commit finish")
        cursor.close()

    def update(self, table_name: str, data_list: list, conditions_list: list):
        if len(data_list) == 0 or len(conditions_list) == 0:
            return
        for i in range(len(data_list)):
            sql = 'update ' + table_name + ' set '
            for kv in data_list[i].items():
                if kv[1] is None:
                    sql += kv[0] + "=" + 'NULL' + ","
                elif type(kv[1]) == type(0):
                    sql += kv[0] + "=" + str(kv[1]) + ","
                elif type(kv[1]) == type(datetime.datetime.now()):
                    sql += kv[0] + "='" + kv[1].strftime("%Y-%m-%d %H:%M:%S") + "',"
                elif type(kv[1]) == type(True):
                    sql += kv[0] + "=" + str(kv[1]) + ","
                elif type(kv[1]) == str:
                    string = kv[1].replace('\\', '\\\\').replace("'", "\\'").replace('"', '\\"')
                    count = string.count('\n\r')
                    string1 = string.replace('\n\r', '', count)
                    count = string1.count('\n')
                    string2 = string1.replace('\n', '', count)
                    sql += kv[0] + "='" + string2 + "',"
                else:
                    sql += kv[0] + "=" + str(kv[1]) + ","
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
                                string3 = values[j].replace('\\', '\\\\').replace("'", "\\'").replace('"', '\\"')
                                count = string3.count('\n\r')
                                string1 = string3.replace('\n\r', '', count)
                                count = string1.count('\n')
                                string2 = string1.replace('\n', '', count)
                                string = string.replace("?", '"' + string2 + '"', 1)
                    else:
                        return
                    sql += ' and ' + '(' + string + ')'
            logging.info(sql)
            print(sql)
            if not self.__conn.is_connected():
                self.connect()
            cursor = self.__conn.cursor()
            cursor.execute(sql)
            self.__conn.commit()
            cursor.close()

    def delete(self, table_name: str, conditions: dict):
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
                            string3 = values[j].replace('\\', '\\\\').replace("'", "\\'").replace('"', '\\"')
                            count = string3.count('\n\r')
                            string1 = string3.replace('\n\r', '', count)
                            count = string1.count('\n')
                            string2 = string1.replace('\n', '', count)
                            string = string.replace("?", "'" + string2 + "'", 1)
                else:
                    return
                sql += ' and ' + '(' + string + ')'
        logging.info(sql)
        print(sql)
        if not self.__conn.is_connected():
            self.connect()
        cursor = self.__conn.cursor()
        cursor.execute(sql)
        self.__conn.commit()
        cursor.close()

    def query_table_description(self, table_name: str):
        sql = 'desc ' + table_name
        logging.info(sql)
        if not self.__conn.is_connected():
            self.connect()
        cursor = self.__conn.cursor()
        cursor.execute(sql)
        logging.info(as_pandas(cursor))
        cursor.close()

    def add_column(self, table_name: str, column_name: str, column_type: str):
        sql = 'alter table ' + table_name + ' add ' + column_name + ' ' + column_type
        logging.info(sql)
        if not self.__conn.is_connected():
            self.connect()
        cursor = self.__conn.cursor()
        cursor.execute(sql)
        self.__conn.commit()
        cursor.close()

    def change_column(self, table_name: str, old_column_name: str, new_column_name: str, column_type: str):
        sql = 'alter table ' + table_name + ' change column ' + old_column_name + ' ' + new_column_name + ' ' + column_type
        logging.info(sql)
        if not self.__conn.is_connected():
            self.connect()
        cursor = self.__conn.cursor()
        cursor.execute(sql)
        self.__conn.commit()
        cursor.close()

    def show_databases(self):
        sql = 'show databases'
        logging.info(sql)
        if not self.__conn.is_connected():
            self.connect()
        cursor = self.__conn.cursor()
        cursor.execute(sql)
        logging.info(as_pandas(cursor))
        cursor.close()

    def show_tables(self):
        sql = 'show tables'
        logging.info(sql)
        if not self.__conn.is_connected():
            self.connect()
        cursor = self.__conn.cursor()
        cursor.execute(sql)
        logging.info(as_pandas(cursor))
        cursor.close()
