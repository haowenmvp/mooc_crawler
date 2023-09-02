import datetime
from typing import List, Tuple

from persistence.model.basic_info import SchoolInfo
from persistence.db.api.mysql_api import MysqlApi


class SchoolInfoRepository:
    kTableName = 'school_info'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def update_school_info(self, school_info: SchoolInfo):
        assert isinstance(school_info, SchoolInfo)
        self.__db_api.update(self.kTableName,
                             [{'school_name': school_info.school_name, 'school_type': school_info.school_type}],
                             [{'school_id = ?': (school_info.school_id,)}])

    def create_school_info(self, school_info: SchoolInfo):
        assert isinstance(school_info, SchoolInfo)
        self.create_school_infos([school_info])

    def create_school_infos(self, school_info_list: List[SchoolInfo]):
        assert isinstance(school_info_list, list)
        data_lines = [school_info.__dict__ for school_info in school_info_list]
        self.__db_api.insert(self.kTableName, data_lines)

    @classmethod
    def __get_school_info_field(cls) -> list:
        fields = [
            'school_id',
            'school_name',
            'school_type',
        ]
        return fields
