import datetime
from typing import List, Tuple

from persistence.model.basic_info import CourseListInfo
from persistence.db.api.mysql_api import MysqlApi


class TestCourseListInfoRepository:
    kTableName = 'test_course_list_info'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def delete_info_by_platform_name(self, platform : str):
        self.__db_api.delete(self.kTableName, {'platform = ?': (platform,)})

    def create_course_list_info(self, course_list_info: CourseListInfo):
        assert isinstance(course_list_info, CourseListInfo)
        self.create_course_list_infos([course_list_info])

    def create_course_list_infos(self, course_list_infos: List[CourseListInfo]):
        assert isinstance(course_list_infos, list)
        data_lines = [course_list_info.__dict__ for course_list_info in course_list_infos]
        self.__db_api.insert(self.kTableName, data_lines)

    @classmethod
    def __get_course_list_info_field(cls) -> list:
        fields = [
            "id",
            "course_id",
            "url",
            "course_name",
            "term",
            "platform",
            "school",
            "teacher",
            "team",
            "start_date",
            "end_date",
            "save_time",
            "status",
            "extra",
            "crowd",
            "clicked",
            "isquality"
                  ]
        return fields
