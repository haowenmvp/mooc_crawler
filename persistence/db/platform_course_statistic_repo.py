import datetime
from typing import List, Tuple

from persistence.model.basic_info import PlatformCourseStatistic
from persistence.db.api.mysql_api import MysqlApi


class PlatformCourseStatisticRepository:
    kTableName = 'platform_course_statistic'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def update_platform_course_statistic(self, platform_course_statistic: PlatformCourseStatistic):
        assert isinstance(platform_course_statistic, PlatformCourseStatistic)
        self.__db_api.update(self.kTableName,
                             [{'platform_total_course_num': platform_course_statistic.platform_total_course_num,
                               'platform_onlinecourse_num': platform_course_statistic.platform_onlinecourse_num,
                               'platform_qualifiedcourse_num': platform_course_statistic.platform_qualifiedcourse_num,
                               'update_time': platform_course_statistic.update_time, }],
                             [{'platform_id = ?': (platform_course_statistic.platform_id,)}])

    def create_platform_course_statistic(self, platform_course_statistic: PlatformCourseStatistic):
        assert isinstance(platform_course_statistic, PlatformCourseStatistic)
        self.create_platform_course_statistics([platform_course_statistic])

    def create_platform_course_statistics(self, platform_course_statistic_list: List[PlatformCourseStatistic]):
        assert isinstance(platform_course_statistic_list, list)
        data_lines = [platform_course_statistic.__dict__ for platform_course_statistic in
                      platform_course_statistic_list]
        self.__db_api.insert(self.kTableName, data_lines)

    @classmethod
    def __get_platform_course_statistic_field(cls) -> list:
        fields = [
            'platform_id',
            'platform_total_course_num',
            'platform_onlinecourse_num',
            'platform_qualifiedcourse_num',
            'update_time',
        ]
        return fields
