import datetime
from typing import List, Tuple

from persistence.model.course_info import SemesterResultInfo
from persistence.db.api.mysql_api import MysqlApi


class SemesterResultInfoRepository:
    kTableName = 'semester_result_info'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def update_semester_result_info(self, semester_result_info: SemesterResultInfo):
        assert isinstance(semester_result_info, SemesterResultInfo)
        self.__db_api.update(self.kTableName, [{'semester_start_date': semester_result_info.semester_start_date,
                                                'semester_end_date': semester_result_info.semester_end_date,
                                                'semester_teacherteam_info': semester_result_info.semester_teacherteam_info,
                                                'semester_studentnum': semester_result_info.semester_studentnum,
                                                'semester_resource_info': semester_result_info.semester_resource_info,
                                                'semester_homework_info': semester_result_info.semester_homework_info,
                                                'semester_interact_info': semester_result_info.semester_interact_info,
                                                'semester_exam_info': semester_result_info.semester_exam_info,
                                                'semester_test_info': semester_result_info.semester_test_info,
                                                'semester_extension_info': semester_result_info.semester_extension_info,
                                                'update_time': semester_result_info.update_time, }],
                             [{'semester_id = ?': (semester_result_info.semester_id,)}])

    def create_semester_result_info(self, semester_result_info: SemesterResultInfo):
        assert isinstance(semester_result_info, SemesterResultInfo)
        self.create_semester_result_infos([semester_result_info])

    def create_semester_result_infos(self, semester_result_info_list: List[SemesterResultInfo]):
        assert isinstance(semester_result_info_list, list)
        data_lines = [semester_result_info.__dict__ for semester_result_info in semester_result_info_list]
        self.__db_api.insert(self.kTableName, data_lines)

    @classmethod
    def __get_semester_result_info_field(cls) -> list:
        fields = ['semester_id',
                  'semester_start_date',
                  'semester_end_date',
                  'semester_teacherteam_info',
                  'semester_studentnum',
                  'semester_resource_info',
                  'semester_homework_info',
                  'semester_interact_info',
                  'semester_exam_info',
                  'semester_test_info',
                  'semester_extension_info',
                  'update_time'
                  ]
        return fields

