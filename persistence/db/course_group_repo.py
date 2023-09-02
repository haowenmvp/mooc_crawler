import datetime
from typing import List, Tuple

from persistence.model.basic_info import CourseGroupInfo
from persistence.db.api.mysql_api import MysqlApi


class CourseGroupRepository:
    kTableName = 'course_group'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)
        self.fields = self.__get_course_group_field()

    def update_course_group_by_id(self, course_group: CourseGroupInfo):
        course_update = course_group.__dict__.copy()
        course_update.pop('course_group_id')
        course_update.pop('id')
        self.__db_api.update(self.kTableName, [course_update], [{"course_group_id=?": (course_group.course_group_id, )}])

    def update_new_term_by_id(self, course_group: CourseGroupInfo):
        course_update = course_group.__dict__.copy()
        course_update.pop('id')
        self.__db_api.update(self.kTableName,
                             [course_update],
                             [{"id=?": (course_group.id,)}])

    def get_tid_by_groupid(self, course_group_id: str):
        res = self.__db_api.query(self.kTableName, ['id'], {"course_group_id = ?": (course_group_id,)})
        if not res:
            return ''
        else:
            return res[0][0]

    def create_course_group(self, course_group_info: CourseGroupInfo):
        assert isinstance(course_group_info, CourseGroupInfo)
        self.create_course_groups([course_group_info])

    def create_course_groups(self, course_group_info_list: List[CourseGroupInfo]):
        assert isinstance(course_group_info_list, list)
        data_lines = [course_group_info.__dict__ for course_group_info in
                      course_group_info_list]
        self.__db_api.insert(self.kTableName, data_lines)

    def update_course_status(self, course_group_id, status):
        self.__db_api.update(self.kTableName, [{"status": status, "update_time": datetime.datetime.now()}],
                             [{'course_group_id = ?': (course_group_id,)}])


    def update_new_term(self, course_group_id, term_count):
        self.__db_api.update(self.kTableName, [{"term_count": term_count, "update_time": datetime.datetime.now()}],
                             [{'course_group_id = ?': (course_group_id,)}])

    def get_group_by_groupid(self,course_group_id):
        res = self.__db_api.query(self.kTableName, self.fields, {"course_group_id = ?": (course_group_id,)})
        if not res:
            return ''
        else:
            return self.__construct_course_group(res[0])

    def delete_info_by_groupid(self, course_group_id: str):
        self.__db_api.delete(self.kTableName, {'course_group_id = ?': (course_group_id,)})

    def clear_table(self):
        self.__db_api.delete(self.kTableName, {})

    @classmethod
    def __get_course_group_field(cls) -> list:
        fields = [
            'id',
            'course_group_id',
            'course_name',
            'platform',
            'school',
            'teacher',
            'term_count',
            'status',
            'update_time'
        ]
        return fields

    @classmethod
    def __construct_course_group(cls, data: Tuple) -> CourseGroupInfo:
        course_group = CourseGroupInfo()
        course_group.course_group_id = data[1]
        course_group.course_name = data[2]
        course_group.platform = data[3]
        course_group.school = data[4]
        course_group.teacher = data[5]
        course_group.term_count = data[6]
        course_group.status = data[7]
        course_group.update_time = data[8]
        return course_group
