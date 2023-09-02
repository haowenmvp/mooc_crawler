import datetime
import logging
from typing import List, Tuple

from persistence.model.forum import ForumPostInfo
from persistence.db.api.mysql_api import MysqlApi


class ForumPostInfoRepository:
    kTableName = 'forum_post_info'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def update_forum_post_info(self, forum_post_info: ForumPostInfo):
        assert isinstance(forum_post_info, ForumPostInfo)
        self.__db_api.update(self.kTableName, [{
            'forum_id': forum_post_info.forum_id,
            'forum_post_type': forum_post_info.forum_post_type,
            'forum_reply_id': forum_post_info.forum_reply_id,
            'forum_reply_userid': forum_post_info.forum_reply_userid,
            'forum_post_username': forum_post_info.forum_post_username,
            'forum_post_userrole': forum_post_info.forum_post_userrole,
            'forum_post_content': forum_post_info.forum_post_content,
            'forum_post_time': forum_post_info.forum_post_time,
            'update_time': forum_post_info.update_time}],
                             [{'forum_post_id = ?': (forum_post_info.forum_post_id,)}])

    def create_forum_post_info(self, forum_post_info: ForumPostInfo):
        assert isinstance(forum_post_info, ForumPostInfo)
        self.create_forum_post_infos([forum_post_info])

    def create_forum_post_infos(self, forum_post_info_list: List[ForumPostInfo]):
        assert isinstance(forum_post_info_list, list)
        data_lines = [forum_post_info.__dict__ for forum_post_info in forum_post_info_list]
        logging.info("start insert data[%s] into %s", len(data_lines), self.kTableName)
        self.__db_api.insert(self.kTableName, data_lines)

    @classmethod
    def __get_forum_post_info_field(cls) -> list:
        fields = [
            'id',
            'platform',
            'forum_post_id',
            'forum_id',
            'forum_post_type',
            'forum_reply_id',
            'forum_reply_userid',
            'forum_post_username',
            'forum_post_userrole',
            'forum_post_content',
            'forum_post_time',
            'update_time',
        ]
        return fields
