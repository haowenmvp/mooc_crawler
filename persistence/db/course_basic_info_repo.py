import datetime
from typing import List, Tuple

from persistence.model.course_info import CourseBasicInfo
from persistence.db.api.mysql_api import MysqlApi


class CourseBasicInfoRepository:
    kTableName = 'course_basic_info'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def update_course_basic_info(self, course_basic_info: CourseBasicInfo):
        assert isinstance(course_basic_info, CourseBasicInfo)
        self.__db_api.update(self.kTableName, [{"course_name": course_basic_info.course_name,
                                                "course_termnum": course_basic_info.course_termnum,
                                                "course_mainplatformid": course_basic_info.course_mainplatformid,
                                                "course_mainplatform": course_basic_info.course_mainplatform,
                                                "course_schoolid": course_basic_info.course_schoolid,
                                                "course_school": course_basic_info.course_school,
                                                "course_teacherid": course_basic_info.course_teacherid,
                                                "course_teacher": course_basic_info.course_teacher,
                                                "course_start_date": course_basic_info.course_start_date,
                                                "course_isquality": course_basic_info.course_isquality,
                                                "course_qualitytime": course_basic_info.course_qualitytime}],
                             [{"course_group_id = ?": (course_basic_info.course_group_id,)}])

    def create_course_basic_info(self, course_basic_info: CourseBasicInfo):
        assert isinstance(course_basic_info, CourseBasicInfo)
        self.create_course_basic_infos([course_basic_info])

    def create_course_basic_infos(self, course_basic_infos: List[CourseBasicInfo]):
        assert isinstance(course_basic_infos, list)
        data_lines = [course_basic_info.__dict__ for course_basic_info in course_basic_infos]
        self.__db_api.insert(self.kTableName, data_lines)

    @classmethod
    def __get_course_basic_info_field(cls) -> list:
        fields = ["course_group_id",
                  "course_name",
                  "course_termnum",
                  "course_mainplatformid",
                  "course_mainplatform",
                  "course_schoolid",
                  "course_school",
                  "course_teacherid",
                  "course_teacher",
                  "course_start_date",
                  "course_isquality",
                  "course_qualitytime"
                  ]
        return fields
