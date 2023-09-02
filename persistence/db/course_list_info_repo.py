import datetime
from typing import List, Tuple
import re
from persistence.model.basic_info import CourseListInfo
from persistence.db.api.mysql_api import MysqlApi


class CourseListInfoRepository:
    kTableName = 'course_list_info'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database
        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)
        self.fields = self.__get_course_list_info_field()

    def fetch_all(self, sql):
        return self.__db_api.fetch_all(sql)

    def batch_all(self, sql):
        if isinstance(sql, list):
            self.__db_api.batch_all(sql_list=sql)
        else:
            self.__db_api.batch_all(sql_list=[sql])

    def get_all_list(self):
        data_lines = self.__db_api.query(self.kTableName, self.fields)
        res = list()
        for line in data_lines:
            course = self.__construct_course_list_info(line)
            res.append(course)
        return res

    def get_courseid_by_term_id(self, course_list_info: CourseListInfo):
        res = self.__db_api.query(self.kTableName, ['course_id'],
                                  {"platform_term_id = ?": (course_list_info.platform_term_id,),
                                   "platform = ?": (course_list_info.platform,)})
        if not res:
            return ''
        else:
            return res[0][0]

    def get_courseid_by_url(self, course_list_info: CourseListInfo):
        res = self.__db_api.query(self.kTableName, ['course_id'], {"url = ?": (course_list_info.url,),
                                                                   "platform = ?": (course_list_info.platform,)})
        if not res:
            return ''
        else:
            return res[0][0]

    def get_courseid_by_courseinfo(self, course_list_info: CourseListInfo):
        res = self.__db_api.query(self.kTableName, ['course_id'], {"course_name = ?": (course_list_info.course_name,),
                                                                   "term = ?": (course_list_info.term,),
                                                                   "teacher = ?": (course_list_info.teacher,),
                                                                   "platform = ?": (course_list_info.platform,),
                                                                   "school = ?": (course_list_info.school,),
                                                                   "start_date = ?": (course_list_info.start_date,),
                                                                   "end_date = ?": (course_list_info.end_date,)})
        if not res:
            return ''
        else:
            return res[0][0]

    def get_courseid_by_all_info(self, course_list_info: CourseListInfo):
        res = self.__db_api.query(self.kTableName, ['course_id'], {"url=?": (course_list_info.url,),
                                                                   "course_name = ?": (course_list_info.course_name,),
                                                                   "term = ?": (course_list_info.term,),
                                                                   "teacher = ?": (course_list_info.teacher,),
                                                                   "platform = ?": (course_list_info.platform,),
                                                                   "school = ?": (course_list_info.school,),
                                                                   "start_date = ?": (course_list_info.start_date,),
                                                                   "end_date = ?": (course_list_info.end_date,)})
        if not res:
            return ''
        else:
            return res[0][0]

    def get_courseid_by_url_term(self, course_list_info: CourseListInfo):
        res = self.__db_api.query(self.kTableName, ['course_id'], {"url = ?": (course_list_info.url,),
                                                                   "term = ?": (course_list_info.term,),
                                                                   "platform = ?": (course_list_info.platform,)})
        if not res:
            return ''
        else:
            return res[0][0]

    def get_courseid_by_courseinfo_except_date(self, course_list_info: CourseListInfo):
        res = self.__db_api.query(self.kTableName, ['course_id'],
                                  {"course_name = ?": (course_list_info.course_name,),
                                   "term = ?": (course_list_info.term,),
                                   "teacher = ?": (course_list_info.teacher,),
                                   "school = ?": (course_list_info.school,),
                                   "platform = ?": (course_list_info.platform,)})
        if not res:
            return 0
        else:
            return res[0][0]

    def get_courseid_by_url_school(self, course_list_info: CourseListInfo) -> int:
        dateLine = self.__db_api.query(self.kTableName, ["course_id", "max(update_time)"],
                                       {"url = ?": (course_list_info.url,),
                                        "school = ?": (course_list_info.school,),
                                        "platform": (course_list_info.platform,)})
        if len(dateLine):
            return dateLine[0][0]
        else:
            return 0
        pass

    def get_courseid_by_name_teacher_term(self, course_list_info: CourseListInfo):
        dateLine = self.__db_api.query(self.kTableName, ["course_id", "max(update_time)"],
                                       {"course_name = ?": (course_list_info.course_name,),
                                        "teacher = ?": (course_list_info.teacher,),
                                        "term = ?": (course_list_info.term,),
                                        "platform": (course_list_info.platform,)})
        if len(dateLine):
            return dateLine[0][0]
        else:
            return 0
        pass

    def get_course_by_course_id(self, course_id: str):
        data_lines = self.__db_api.query(self.kTableName, self.fields, {"course_id = ?": (course_id,)})
        res = list()
        for line in data_lines:
            course = self.__construct_course_list_info(line)
            res.append(course.__dict__)
        return res

    def get_groupid_by_course_id(self, course_id: int):
        res = self.__db_api.query(self.kTableName, ['course_group_id'], {"course_id = ?": (course_id,)})
        if not res:
            return 0
        else:
            return res[0][0]
        pass

    def get_if_url_existed(self, url: str):
        if len(self.__db_api.query(self.kTableName, ['course_id'],
                                   {"url = ?": (url,)})) != 0:
            return True
        else:
            return False

    def get_if_courseinfo_exsited(self, course_list_info: CourseListInfo):
        if len(self.__db_api.query(self.kTableName, ['course_id'],
                                   {"course_name = ?": (course_list_info.course_name,),
                                    "term = ?": (course_list_info.term,),
                                    "teacher = ?": (course_list_info.teacher,),
                                    "platform = ?": (course_list_info.platform,),
                                    "school = ?": (course_list_info.school,),
                                    "start_date = ?": (course_list_info.start_date,),
                                    "end_date = ?": (course_list_info.end_date,)})) != 0:
            return True
        else:
            return False

    def get_if_course_exsited_by_all_info(self, course_list_info: CourseListInfo):
        if len(self.__db_api.query(self.kTableName, ['course_id'],
                                   {"url=?": (course_list_info.url,),
                                    "course_name = ?": (course_list_info.course_name,),
                                    "term = ?": (course_list_info.term,),
                                    "teacher = ?": (course_list_info.teacher,),
                                    "platform = ?": (course_list_info.platform,),
                                    "school = ?": (course_list_info.school,),
                                    "start_date = ?": (course_list_info.start_date,),
                                    "end_date = ?": (course_list_info.end_date,)})) != 0:
            return True
        else:
            return False

    def get_if_course_exsited(self, course_list_info: CourseListInfo):
        if len(self.__db_api.query(self.kTableName, ['course_id'],
                                   {"url = ?": (course_list_info.url,)})) != 0:
            return True
        elif len(self.__db_api.query(self.kTableName, ['course_id'],
                                     {"course_name = ?": (course_list_info.course_name,),
                                      "term = ?": (course_list_info.term,),
                                      "teacher = ?": (course_list_info.teacher,),
                                      "platform = ?": (course_list_info.platform,),
                                      "school = ?": (course_list_info.school,),
                                      "start_date = ?": (course_list_info.start_date,),
                                      "end_date = ?": (course_list_info.end_date,)})) != 0:
            return True
        else:
            return False

    def delete_info_by_platform_name(self, platform: str):
        self.__db_api.delete(self.kTableName, {'platform = ?': (platform,)})

    def get_course_list_by_platform_status(self, platform: str, status: int):

        data_lines = self.__db_api.query(self.kTableName, self.fields,
                                         {"platform = ?": (platform,),
                                          "status = ?": (status,)})
        res = list()
        for line in data_lines:
            course = self.__construct_course_list_info(line)
            res.append(course)
        return res

    def get_course_list_by_platform(self, platform: str):
        data_lines = self.__db_api.query(self.kTableName, self.fields,
                                         {"platform = ?": (platform,)})
        res = list()
        for line in data_lines:
            course = self.__construct_course_list_info(line)
            res.append(course)
        return res

    def get_valid_course_list(self):
        data_lines = self.__db_api.query(self.kTableName, self.fields,
                                         {"valid = ?": (1,)})
        res = list()
        for line in data_lines:
            course = self.__construct_course_list_info(line)
            res.append(course)
        return res

    def create_course_list_info(self, course_list_info: CourseListInfo):
        assert isinstance(course_list_info, CourseListInfo)
        self.create_course_list_infos([course_list_info])

    def create_course_list_infos(self, course_list_infos: List[CourseListInfo]):
        assert isinstance(course_list_infos, list)
        data_lines = []
        for course_list_info in course_list_infos:
            course_list_info = course_list_info.__dict__
            course_list_info['update_time'] = course_list_info['save_time']
            data_lines.append(course_list_info)
        self.__db_api.insert(self.kTableName, data_lines)

    def update_list_info_by_courseinfo(self, course_list_info: CourseListInfo):
        course_update = course_list_info.__dict__.copy()
        course_update.pop('course_id')
        course_update['update_time'] = course_update['save_time']
        course_update.pop('save_time')
        course_update.pop('course_name')
        course_update.pop('term')
        course_update.pop('teacher')
        course_update.pop('school')
        course_update.pop('platform')
        course_update.pop('start_date')
        course_update.pop('end_date')
        self.__db_api.update(self.kTableName,
                             [course_update],
                             [{"course_name = ?": (course_list_info.course_name,),
                               "term = ?": (course_list_info.term,),
                               "teacher = ?": (course_list_info.teacher,),
                               "platform = ?": (course_list_info.platform,),
                               "school = ?": (course_list_info.school,),
                               "start_date = ?": (course_list_info.start_date,),
                               "end_date = ?": (course_list_info.end_date,)}])

    def update_list_info_by_all_info(self, course_list_info: CourseListInfo):
        course_update = course_list_info.__dict__.copy()
        course_update.pop('course_id')
        course_update['update_time'] = course_update['save_time']
        course_update.pop('save_time')
        course_update.pop('url')
        course_update.pop('course_name')
        course_update.pop('term')
        course_update.pop('teacher')
        course_update.pop('school')
        course_update.pop('platform')
        course_update.pop('start_date')
        course_update.pop('end_date')
        self.__db_api.update(self.kTableName,
                             [course_update],
                             [{"url=?": (course_list_info.url,),
                               "course_name = ?": (course_list_info.course_name,),
                               "term = ?": (course_list_info.term,),
                               "teacher = ?": (course_list_info.teacher,),
                               "platform = ?": (course_list_info.platform,),
                               "school = ?": (course_list_info.school,),
                               "start_date = ?": (course_list_info.start_date,),
                               "end_date = ?": (course_list_info.end_date,)}])

    def update_list_info_by_course_id(self, course_list_info: CourseListInfo, course_id):
        course_update = course_list_info.__dict__
        course_update.pop('course_id')
        course_update['update_time'] = course_update['save_time']
        course_update.pop('save_time')
        self.__db_api.update(self.kTableName,
                             [course_update],
                             [{"course_id = ?": (course_id,)}])

    def update_list_info_by_url(self, course_list_info: CourseListInfo):
        course_update = course_list_info.__dict__.copy()
        course_update.pop('course_id')
        course_update.pop('url')
        course_update['update_time'] = course_update['save_time']
        course_update.pop('save_time')
        self.__db_api.update(self.kTableName,
                             [course_update],
                             [{"url = ?": (course_list_info.url,)}])

    def update_weizhiku_by_course_id(self, course_list_info: CourseListInfo, course_id: str):
        course_update = course_list_info.__dict__
        course_update.pop('course_id')
        course_update.pop('update_time')
        self.__db_api.update(self.kTableName,
                             [course_update],
                             [{"course_id = ?": (course_id,)}])

    def update_status_by_url(self, course_list_info: CourseListInfo, status: int):
        """
        根据url更新课程状态
        :param course_list_info:
        :param status:
        :return:
        """
        self.__db_api.update(self.kTableName,
                             [{'status': status, 'update_time': datetime.datetime.now()}],
                             [{"url = ?": (course_list_info.url,),
                               "platform = ?": (course_list_info.platform)}])
        pass

    def add_course_list_infos(self, course_list_infos: List[CourseListInfo]):
        assert isinstance(course_list_infos, list)
        fields = self.__get_course_list_info_field()
        update_list = []
        create_list = []
        update_conditions = []
        for course_list_info in course_list_infos:
            # print(course_list_info.__dict__)
            if len(self.__db_api.query(self.kTableName, ['course_id'],
                                       {"url = ?": (course_list_info.url,)})) != 0:
                course_id = course_list_info.course_id
                course_update = course_list_info.__dict__.pop('course_id')
                print(course_update)
                print(type(course_update))
                update_list.append(course_update)
                update_condition = {}
                update_condition['course_id = ?'] = (course_id,)
                update_conditions.append(update_condition)
            else:
                create_list.append(course_list_info)
        create_data_lines = [course_list_info.__dict__ for course_list_info in create_list]
        self.__db_api.insert(self.kTableName, create_data_lines)
        # update_data_lines = [course_list_info for course_list_info in update_list]
        self.__db_api.update(self.kTableName, update_list, update_conditions)

    def get_courses_info_by_platform_date(self, platform, begin_date: datetime.datetime, end_date: datetime.datetime,
                                          field_type) -> list:
        if platform == "all":
            dataline = self.__db_api.query(self.kTableName, None, {field_type + " BETWEEN '" + begin_date.strftime(
                "%Y-%m-%d %H:%M:%S") + "' and '" + end_date.strftime(
                "%Y-%m-%d %H:%M:%S") + "'": ()})
        else:
            dataline = self.__db_api.query(self.kTableName, None, {"platform = " + "'" + platform + "'": (),
                                                                   field_type + " BETWEEN '" + begin_date.strftime(
                                                                       "%Y-%m-%d %H:%M:%S") + "' and '" + end_date.strftime(
                                                                       "%Y-%m-%d %H:%M:%S") + "'": ()})
        courselist = []
        for course in dataline:
            courselist.append(self.__construct_course_list_info(course))
        return courselist
        pass

    def get_group_id_by_term_id(self, course_list_info: CourseListInfo):
        res = self.__db_api.query(self.kTableName, ['course_group_id'],
                                  {"platform_term_id = ?": (course_list_info.platform_term_id,),
                                   "platform = ?": (course_list_info.platform,)})
        if not res:
            return ''
        else:
            return res[0][0]

    def get_status_by_course_group_id(self, course_group_id):
        res = self.__db_api.query(self.kTableName, ['status', 'update_time'],
                                  {"course_group_id = ?": (course_group_id,)})
        if not res:
            return ''
        else:
            return res[0]

    def get_maxterm_status(self, course_group_id, term_id):
        res = self.__db_api.query(self.kTableName, ['status', 'update_time'],
                                  {"course_group_id = ?": (course_group_id,),
                                   "term_id = ?": (term_id,)})
        if not res:
            return ('', '')
        else:
            return res[0]

    def update_term_id_number_by_groupid_weizhiku(self, course_list: List[CourseListInfo]):
        for course_list_info in course_list:
            course_urls_sql = 'select url from course_list_info where platform="微知库数字校园学习平台" and course_group_id= "{}" order by start_date'
            term_number_sql = 'select count(*) from course_list_info where platform="微知库数字校园学习平台" and course_group_id= "{}"'
            url_list = []
            course_group_id = course_list_info.course_group_id
            course_urls_sql = course_urls_sql.format(course_group_id)
            term_number_sql = term_number_sql.format(course_group_id)
            urls = self.fetch_all(course_urls_sql)
            term_number = self.fetch_all(term_number_sql)[0][0]
            for item in urls:
                url_list.append(item[0])
            term_id = url_list.index(course_list_info.url) + 1
            course_list_info.term_id = term_id
            course_list_info.term_number = term_number

            course_id = self.get_courseid_by_url(course_list_info)
            course_list_info.course_id = course_id
            self.update_weizhiku_by_course_id(course_list_info, course_id)

    def update_list_info_by_course_id_except_term_id_number(self, course_list_info: CourseListInfo, course_id: int):
        course_update = course_list_info.__dict__.copy()
        course_update.pop('course_id')
        course_update.pop('url')
        course_update['update_time'] = course_update['save_time']
        course_update.pop('save_time')
        course_update.pop('term_id')
        course_update.pop('term_number')
        self.__db_api.update(self.kTableName,
                             [course_update],
                             [{"course_id = ?": (course_id,)}])
        pass

    def get_course_list_by_groupid(self, course_list_info: CourseListInfo):
        data_lines = self.__db_api.query(self.kTableName, self.fields,
                                         {"platform = ?": (course_list_info.platform,),
                                          "course_group_id = ?": (course_list_info.course_group_id,)})
        res = list()
        for line in data_lines:
            course = self.__construct_course_list_info(line)
            res.append(course)
        return res

    def update_term_id_number_by_groupid(self, course_list: List[CourseListInfo]):
        for course_list_info in course_list:
            if course_list_info.valid == 1 and course_list_info.term != "自主模式" and \
                    course_list_info.term != "仅供预览请勿学习" and course_list_info.term != "自主学习" and \
                    course_list_info.term != "仅供预览" and course_list_info.term != "Self-Paced" and course_list_info.term != "" and \
                    course_list_info.term != "体验课" and course_list_info.term != "体验版":
                course_urls_sql = 'select url from course_list_info where platform="学堂在线" and course_group_id= "{}" ' \
                                  'and term not in ("自主模式","仅供预览请勿学习", "自主学习","","仅供预览","Self-Paced") order by start_date;'
                term_number_sql = 'select count(*) from course_list_info where platform="学堂在线" and course_group_id= "{}" and ' \
                                  'term not in ("自主模式","仅供预览请勿学习", "自主学习","","仅供预览","Self-Paced");'
                url_list = []
                course_group_id = course_list_info.course_group_id
                course_urls_sql = course_urls_sql.format(course_group_id)
                term_number_sql = term_number_sql.format(course_group_id)
                urls = self.fetch_all(course_urls_sql)
                term_number = self.fetch_all(term_number_sql)[0][0]
                for item in urls:
                    url_list.append(item[0])
                term_id = url_list.index(course_list_info.url) + 1
                course_list_info.term_id = term_id
                course_list_info.term_number = term_number
                course_id = self.get_courseid_by_url(course_list_info)
                course_list_info.course_id = course_id
                self.update_list_info_by_course_id(course_list_info, course_id)
        pass

    def update_ids_by_course_id(self, course_list_info: CourseListInfo, course_id):
        self.__db_api.update(self.kTableName,
                             [{'platform_course_id': course_list_info.platform_term_id,
                               'platform_term_id': course_list_info.platform_term_id,
                               'course_group_id': course_list_info.course_group_id}],
                             [{"course_id = ?": (course_id,)}])

    def get_on_course_list(self, platform: str):
        data_lines = self.__db_api.query(self.kTableName, ['course_id', 'url', 'replace(term,"\r","") as term'],
                                         {"platform = ?": (platform,), "status = ?": (1,), "valid = ?": (1,)})
        return data_lines

    @classmethod
    def __get_course_list_info_field(cls) -> list:
        fields = [
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
            "isquality",
            "course_score",
            "introduction",
            "subject",
            "course_target",
            "scoring_standard",
            "isfree",
            "certification",
            "ispublic",
            "term_id",
            "total_crowd",
            "update_time",
            "crowd_num",
            "total_crowd_num",
            "term_number",
            "block",
            "course_attributes",
            "platform_course_id",
            "platform_term_id",
            "course_group_id",
            "valid"
        ]
        return fields

    @classmethod
    def __construct_course_list_info(cls, data: Tuple) -> CourseListInfo:
        # id_offset = 1
        course_list_info = CourseListInfo()
        course_list_info.course_id = data[0]
        course_list_info.url = data[1]
        course_list_info.course_name = data[2]
        course_list_info.term = data[3]
        course_list_info.platform = data[4]
        course_list_info.school = data[5]
        course_list_info.teacher = data[6]
        course_list_info.team = data[7]
        course_list_info.start_date = data[8]
        course_list_info.end_date = data[9]
        course_list_info.save_time = data[10]
        course_list_info.status = data[11]
        course_list_info.extra = data[12]
        course_list_info.crowd = data[13]
        course_list_info.clicked = data[14]
        course_list_info.isquality = data[15]
        course_list_info.course_score = data[16]
        course_list_info.introduction = data[17]
        course_list_info.subject = data[18]
        course_list_info.course_target = data[19]
        course_list_info.scoring_standard = data[20]
        course_list_info.isfree = data[21]
        course_list_info.certification = data[22]
        course_list_info.ispublic = data[23]
        course_list_info.term_id = data[24]
        course_list_info.total_crowd = data[25]
        course_list_info.update_time = data[26]
        course_list_info.crowd_num = data[27]
        course_list_info.total_crowd_num = data[28]
        course_list_info.term_number = data[29]
        course_list_info.block = data[30]
        course_list_info.course_attributes = data[31]
        course_list_info.platform_course_id = data[32]
        course_list_info.platform_term_id = data[33]
        course_list_info.course_group_id = data[34]
        course_list_info.valid = data[35]
        return course_list_info
