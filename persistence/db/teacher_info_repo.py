import datetime
from typing import List, Tuple

from persistence.model.basic_info import TeacherInfo
from persistence.db.api.mysql_api import MysqlApi


class TeacherInfoRepository:
    kTableName = 'teacher_info'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def update_teacher_info(self, teacher_info: TeacherInfo):
        assert isinstance(teacher_info, TeacherInfo)
        self.__db_api.update(self.kTableName, [{'teacher_name': teacher_info.teacher_name,
                                                'teacher_school': teacher_info.teacher_school,
                                                'teacher_department': teacher_info.teacher_department,
                                                'teacher_titles': teacher_info.teacher_titles,
                                                'teacher_forumid': teacher_info.teacher_forumid, }],
                             [{'teacher_id = ?': (teacher_info.teacher_id,)}])

    def create_teacher_info(self, teacher_info: TeacherInfo):
        assert isinstance(teacher_info, TeacherInfo)
        self.create_teacher_infos([teacher_info])

    def create_teacher_infos(self, teacher_info_list: List[TeacherInfo]):
        assert isinstance(teacher_info_list, list)
        data_lines = [teacher_info.__dict__ for teacher_info in teacher_info_list]
        self.__db_api.insert(self.kTableName, data_lines)

    def get_teacher_id_by_school_teacher(self, school: str, teacher_name: str) -> str:
        res = self.__db_api.query(self.kTableName, ['teacher_id'], {"teacher_school = ?": (school,),
                                                                    "teacher_name = ?": (teacher_name,)})
        if not res:
            return ''
        else:
            return res[0][0]
        # ？res

    @classmethod
    def __get_teacher_info_field(cls) -> list:
        fields = [
            'id',
            'teacher_id',
            'teacher_name',
            'teacher_school',
            'teacher_department',
            'teacher_titles',
            'teacher_forumid',
        ]
        return fields
