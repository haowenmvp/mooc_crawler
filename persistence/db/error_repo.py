import datetime
from typing import List, Tuple
from persistence.db.api.mysql_api import MysqlApi


class ErrorRepository:
    kTableName = 'error_list'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database
        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def create_error_list_infos(self, error_list):
        data_lines = []
        for error_info in error_list:
            error_info["id"] = 0
            error_info['save_time'] = datetime.datetime.now()
            data_lines.append(error_info)
        self.__db_api.insert(self.kTableName, data_lines)

