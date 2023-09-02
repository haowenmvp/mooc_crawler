import datetime
from typing import List, Tuple

from persistence.model.basic_info import SchoolCourseStatistic
from persistence.db.api.mysql_api import MysqlApi


class SchoolCourseStatisticRepository:
    kTableName = 'school_course_statistic'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def update_school_course_statistic(self, school_course_statistic: SchoolCourseStatistic):
        assert isinstance(school_course_statistic, SchoolCourseStatistic)
        self.__db_api.update(self.kTableName,
                            [{'school_total_course_num': school_course_statistic.school_total_course_num,
                              'school_onlinecourse_num': school_course_statistic.school_onlinecourse_num,
                              'school_qualifiedcourse_num': school_course_statistic.school_qualifiedcourse_num,
                              'update_time': school_course_statistic.update_time}],
                            [{'school_id = ?': (school_course_statistic.school_id,)}])

    def create_school_course_statistic(self, school_course_statistic: SchoolCourseStatistic):
        assert isinstance(school_course_statistic, SchoolCourseStatistic)
        self.create_school_course_statistics([school_course_statistic])

    def create_school_course_statistics(self, school_course_statistic_list: List[SchoolCourseStatistic]):
        assert isinstance(school_course_statistic_list, list)
        data_lines = [school_course_statistic.__dict__ for school_course_statistic in school_course_statistic_list]
        self.__db_api.insert(self.kTableName, data_lines)

    @classmethod
    def __get_school_course_statistic_field(cls) -> list:
        fields = [
            'school_id',
            'school_total_course_num',
            'school_onlinecourse_num',
            'school_qualifiedcourse_num',
            'update_time',
        ]
        return fields
