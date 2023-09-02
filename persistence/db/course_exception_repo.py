import datetime
from typing import List, Tuple
from persistence.db.api.mysql_api import MysqlApi
from persistence.model.course_exception import CourseException


class CourseExceptionRepository:
    kTableName = 'course_list_exception'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database
        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def create_course_exception(self, course_list_exception: CourseException):
        self.create_course_exception_list([course_list_exception])

    def create_course_exception_list(self, course_list_exception: List[CourseException]):
        assert isinstance(course_list_exception, list)
        data_lines = []
        for course_exception in course_list_exception:
            course_exception = course_exception.__dict__
            data_lines.append(course_exception)
        self.__db_api.insert(self.kTableName, data_lines)
