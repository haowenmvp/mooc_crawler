import datetime
from typing import List, Tuple

from persistence.model.basic_info import PlatformResource
from persistence.db.api.mysql_api import MysqlApi


class PlatformResourceRepository:
    kTableName = 'platform_resource'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)



    def create_platform_resource(self, platform_resource: PlatformResource):
        assert isinstance(platform_resource, PlatformResource)
        self.create_platform_resources([platform_resource])

    def create_platform_resources(self, platform_resource_list: List[PlatformResource]):
        assert isinstance(platform_resource_list, list)
        data_lines = [platform_resource.__dict__ for platform_resource in
                      platform_resource_list]
        self.__db_api.insert(self.kTableName, data_lines)

    @classmethod
    def __get_platform_resource_field(cls) -> list:
        fields = [
        'platform',
        'accumulate_course_num',
        'accumulate_crowd',
        'open_course_num',
        'open_course_crowd',
        'update_time'
        ]
        return fields
