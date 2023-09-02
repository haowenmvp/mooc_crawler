import datetime
from typing import List, Tuple

from persistence.model.basic_info import PlatformInfo
from persistence.db.api.mysql_api import MysqlApi


class PlatformInfoRepository:
    kTableName = 'platform_info'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def update_platform_info(self, platform_info: PlatformInfo):
        assert isinstance(platform_info, PlatformInfo)
        self.__db_api.update(self.kTableName, [{'platform_name': platform_info.platform_name,
                                                'platform_mainpage': platform_info.platform_mainpage, }],
                             [{'platform_id = ?': (platform_info.platform_id,)}])

    def create_platform_info(self, platform_info: PlatformInfo):
        assert isinstance(platform_info, PlatformInfo)
        self.create_platform_infos([platform_info])

    def create_platform_infos(self, platform_info_list: List[PlatformInfo]):
        assert isinstance(platform_info_list, list)
        data_lines = [platform_info.__dict__ for platform_info in platform_info_list]
        self.__db_api.insert(self.kTableName, data_lines)

    @classmethod
    def __get_platform_info_field(cls) -> list:
        fields = [
            'platform_id',
            'platform_name',
            'platform_mainpage',
        ]
        return fields
