import datetime
from typing import List, Tuple

from persistence.model.basic_info import CourseCrowdWeekly
from persistence.db.api.mysql_api import MysqlApi


class CourseCrowdWeeklyRepo:
    kTableName = 'course_crowd_weekly'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)
        self.fields = self.__get_course_crowd_field()

    def create_course_crowd_weekly(self, course_crowd_weekly: CourseCrowdWeekly):
        assert isinstance(course_crowd_weekly, CourseCrowdWeekly)
        self.create_course_crowd_weeklys([course_crowd_weekly])

    def create_course_crowd_weeklys(self, course_crowd_weeklys: List[CourseCrowdWeekly]):
        assert isinstance(course_crowd_weeklys, list)
        data_lines = [course_crowd_weekly.__dict__ for course_crowd_weekly in course_crowd_weeklys]
        self.__db_api.insert(self.kTableName, data_lines)

    def is_crowd_exsits(self, course_crowd_weekly: CourseCrowdWeekly):
        data_lines = self.__db_api.query(self.kTableName, ['id', 'update_time'],
                                         {"course_id = ?": (course_crowd_weekly.course_id,)})
        if len(data_lines) == 0:
            return False
        else:
            for line in data_lines:
                if line[1].date() == course_crowd_weekly.update_time.date():
                    return line[0]
            return False

    def update_course_crowd_weekly(self, course_crowd_weekly: CourseCrowdWeekly, id: str):
        course_crowd_weekly.id = id
        course_update = course_crowd_weekly.__dict__.copy()
        course_update.pop('course_id')
        course_update.pop('id')
        self.__db_api.update(self.kTableName,
                             [course_update],
                             [{"id = ?": (course_crowd_weekly.id,)}])

    @classmethod
    def __get_course_crowd_field(cls) -> list:
        fields = [
            "id",
            "course_id",
            "course_name",
            "term",
            "platform",
            "school",
            "teacher",
            "crowd",
            "total_crowd",
            "status",
            "start_date",
            "end_date",
            "update_time",
            "term_id",
            "url",
            "total_crowd_num",
            "crowd_num",
            "term_number",
            "block"
        ]
        return fields

    @classmethod
    def __construct_course_crowd(cls, data: Tuple) -> CourseCrowdWeekly:
        course_crowd_weekly = CourseCrowdWeekly()
        course_crowd_weekly.id = data[0]
        course_crowd_weekly.course_id = data[1]
        course_crowd_weekly.course_name = data[2]
        course_crowd_weekly.term = data[3]
        course_crowd_weekly.platform = data[4]
        course_crowd_weekly.school = data[5]
        course_crowd_weekly.teacher = data[6]
        course_crowd_weekly.crowd = data[7]
        course_crowd_weekly.total_crowd = data[8]
        course_crowd_weekly.status = data[9]
        course_crowd_weekly.start_date = data[10]
        course_crowd_weekly.end_date = data[11]
        course_crowd_weekly.update_time = data[12]
        course_crowd_weekly.term_id = data[13]
        course_crowd_weekly.url = data[14]
        course_crowd_weekly.crowd_num = data[15]
        course_crowd_weekly.total_crowd_num = data[16]
        course_crowd_weekly.term_number = data[17]
        course_crowd_weekly.block = data[18]
        return course_crowd_weekly
