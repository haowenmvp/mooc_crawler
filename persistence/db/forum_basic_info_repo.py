import datetime
from typing import List, Tuple

from persistence.model.forum import ForumBasicInfo
from persistence.db.api.mysql_api import MysqlApi


class ForumBasicInfoRepository:
    kTableName = 'forum_basic_info'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def get_forum_id_by_semester_id_and_name(self, semester_id: str, forum_name: str) -> str:
        res = self.__db_api.query(self.kTableName, ['forum_id'], {"semester_id = ?": (semester_id, ),
                                                                  "forum_plate = ?": (forum_name, )})
        if not res:
            return ''
        else:
            return res[0][0]

    def update_forum_basic_info(self, forum_basic_info: ForumBasicInfo):
        assert isinstance(forum_basic_info, ForumBasicInfo)
        self.__db_api.update(self.kTableName, [{"semester_id": forum_basic_info.semester_id,
                                                "forum_plate": forum_basic_info.forum_plate}],
                             [{'forum_id': forum_basic_info.forum_id}])

    def create_forum_basic_info(self, forum_basic_info: ForumBasicInfo):
        assert isinstance(forum_basic_info, ForumBasicInfo)
        self.create_forum_basic_infos([forum_basic_info])

    def create_forum_basic_infos(self, forum_basic_info_list: List[ForumBasicInfo]):
        assert isinstance(forum_basic_info_list, list)
        data_lines = [forum_basic_info.__dict__ for forum_basic_info in forum_basic_info_list]
        self.__db_api.insert(self.kTableName, data_lines)

    @classmethod
    def __get_forum_basic_info_field(cls) -> list:
        fields = ['forum_id',
                  'semester_id',
                  'forum_plate'
                  ]
        return fields
