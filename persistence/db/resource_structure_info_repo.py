import datetime
from typing import List, Tuple

from persistence.model.resource import ResourceStructureInfo
from persistence.db.api.mysql_api import MysqlApi


class ResourceStructureInfoRepository:
    kTableName = 'resource_structure_info'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def update_resource_structure_info(self, resource_structure_info: ResourceStructureInfo):
        assert isinstance(resource_structure_info, ResourceStructureInfo)
        self.__db_api.update(self.kTableName, [{'semester_id': resource_structure_info.semester_id,
                                                'resource_chapter_index': resource_structure_info.resource_chapter_index,
                                                'resource_chapter_name': resource_structure_info.resource_chapter_name,
                                                'resource_knobble_index': resource_structure_info.resource_knobble_index,
                                                'resource_knobble_name': resource_structure_info.resource_knobble_name,
                                                'update_time': resource_structure_info.update_time }],
                             [{'resource_structure_id = ?': (resource_structure_info.resource_structure_id,)}])

    def create_resource_structure_info(self, resource_structure_info: ResourceStructureInfo):
        assert isinstance(resource_structure_info, ResourceStructureInfo)
        self.create_resource_structure_infos([resource_structure_info])

    def create_resource_structure_infos(self, resource_structure_info_list: List[ResourceStructureInfo]):
        assert isinstance(resource_structure_info_list, list)
        data_lines = [resource_structure_info.__dict__ for resource_structure_info in resource_structure_info_list]
        self.__db_api.insert(self.kTableName, data_lines)

    @classmethod
    def __get_resource_structure_info_field(cls) -> list:
        fields = ['resource_structure_id',
                  'semester_id',
                  'resource_chapter_index',
                  'resource_chapter_name',
                  'resource_knobble_index',
                  'resource_knobble_name',
                  'update_time',
                  ]
        return fields

