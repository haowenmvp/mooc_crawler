import datetime
from typing import List, Tuple

from persistence.model.basic_info import SchoolResource
from persistence.db.api.mysql_api import MysqlApi


class SchoolResourceRepository:
    kTableName = 'school_resource'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)



    def create_school_resource(self, school_resource: SchoolResource):
        assert isinstance(school_resource, SchoolResource)
        self.create_school_resources([school_resource])

    def create_school_resources(self, school_resource_list: List[SchoolResource]):
        assert isinstance(school_resource_list, list)
        data_lines = [school_resource.__dict__ for school_resource in
                      school_resource_list]
        self.__db_api.insert(self.kTableName, data_lines)

    @classmethod
    def __get_school_resource_field(cls) -> list:
        fields = [
        'school_name', 
        'accumulate_course_num',
        'accumulate_crowd',
        'open_course_num',
        'open_course_crowd',
        'update_time'
        ]
        return fields
