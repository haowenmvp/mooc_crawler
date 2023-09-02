import datetime
from typing import List, Tuple

from persistence.model.course_info import SemesterBasicInfo
from persistence.db.api.mysql_api import MysqlApi


class SemesterBasicInfoRepository:
    kTableName = 'semester_basic_info'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def get_semester_basic_info_by_id(self, semester_id: str) -> SemesterBasicInfo:
        data_lines = self.__db_api.query(self.kTableName, self.__get_semester_basic_info_field(),
                                         {"semester_id = ?": (semester_id, )})
        if not data_lines:
            raise KeyError("Semester [%s] not found.", semester_id)

        data = data_lines[0]
        info = SemesterBasicInfo()
        info.semester_id = data[0]
        info.course_group_id = data[1]
        info.semester_seq = data[2]
        info.semester_url = data[3]
        info.semester_platformid = data[4]
        info.semester_platform = data[5]
        info.semester_label = data[6]
        info.semester_crawltask = data[7]
        return info


    def update_semester_basic_info(self, semester_basic_info: SemesterBasicInfo):
        assert isinstance(semester_basic_info, SemesterBasicInfo)
        self.__db_api.update(self.kTableName, [{'course_group_id': semester_basic_info.course_group_id,
                                                'semester_seq': semester_basic_info.semester_seq,
                                                'semester_url': semester_basic_info.semester_url,
                                                'semester_platformid': semester_basic_info.semester_platformid,
                                                'semester_platform': semester_basic_info.semester_platform,
                                                'semester_label': semester_basic_info.semester_label,
                                                'semester_crawltask': semester_basic_info.semester_crawltask}],
                             [{'semester_id = ?': (semester_basic_info.semester_id,)}])

    def create_semester_basic_info(self, semester_basic_info: SemesterBasicInfo):
        assert isinstance(semester_basic_info, SemesterBasicInfo)
        self.create_semester_basic_infos([semester_basic_info])

    def create_semester_basic_infos(self, semester_basic_info_list: List[SemesterBasicInfo]):
        assert isinstance(semester_basic_info_list, list)
        data_lines = [semester_basic_info.__dict__ for semester_basic_info in semester_basic_info_list]
        self.__db_api.insert(self.kTableName, data_lines)

    @classmethod
    def __get_semester_basic_info_field(cls) -> list:
        fields = ['semester_id',
                  'course_group_id',
                  'semester_seq',
                  'semester_url',
                  'semester_platformid',
                  'semester_platform',
                  'semester_label',
                  'semester_crawltask',
                  ]
        return fields
