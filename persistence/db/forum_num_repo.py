import datetime
from typing import List, Tuple
from persistence.db.api.mysql_api import MysqlApi
from persistence.model.forum import ForumNumInfo


class ForumNumInfoRepository:
    kTableName = 'forum_num'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database
        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def create_forum_num_info(self, forum_num_info: ForumNumInfo):
        self.create_forum_num_infos([forum_num_info])

    def create_forum_num_infos(self, forum_num_list: List[ForumNumInfo]):
        assert isinstance(forum_num_list, list)
        data_lines = []
        for forum_num_info in forum_num_list:
            forum_num_info = forum_num_info.__dict__
            data_lines.append(forum_num_info)
        self.__db_api.insert(self.kTableName, data_lines)
