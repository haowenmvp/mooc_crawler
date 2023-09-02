import datetime
from typing import List, Tuple

from persistence.model.resource import ResourceInfo
from persistence.db.api.mysql_api import MysqlApi


class ResourceInfoRepository:
    kTableName = 'resource_info'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def update_resource_info(self, resource_info: ResourceInfo):
        assert isinstance(resource_info, ResourceInfo)
        self.__db_api.update(self.kTableName, [{'resource_name': resource_info.resource_name,
                                                'resource_type': resource_info.resource_type,
                                                'resource_structure_id': resource_info.resource_structure_id,
                                                'resource_teacherid': resource_info.resource_teacherid,
                                                'resource_teacher': resource_info.resource_teacher,
                                                'resource_time': resource_info.resource_time,
                                                'resource_storage_location': resource_info.resource_storage_location,
                                                'update_time': resource_info.update_time}],
                             [{'resource_id = ?': (resource_info.resource_id,)}])

    def create_resource_info(self, resource_info: ResourceInfo):
        assert isinstance(resource_info, ResourceInfo)
        self.create_resource_infos([resource_info])

    def create_resource_infos(self, resource_info_list: List[ResourceInfo]):
        assert isinstance(resource_info_list, list)
        data_lines = [resource_info.__dict__ for resource_info in resource_info_list]
        self.__db_api.insert(self.kTableName, data_lines)

    @classmethod
    def __get_resource_info_field(cls) -> list:
        fields = [
            'resource_id',
            'resource_name',
            'resource_type',
            'resource_structure_id',
            'resource_teacherid',
            'resource_teacher',
            'resource_time',
            'resource_storage_location',
            'update_time',
        ]
        return fields
