import hashlib
from datetime import datetime

import mysql.connector

from data_process.platform_processors.base_processor import BaseProcessor
from data_process.processor import Processor
from persistence.db.course_group_repo import CourseGroupRepository
from persistence.db.course_list_info_repo import CourseListInfoRepository
from persistence.db.platform_resource_repo import PlatformResourceRepository
from persistence.db.school_resource_repo import SchoolResourceRepository
from persistence.model.basic_info import CourseGroupInfo
from persistence.model.process_config import ProcessConfig
import datetime
from typing import List

from persistence.db.course_list_info_repo import CourseListInfoRepository
from persistence.model.basic_info import CourseListInfo
from persistence.model.basic_info import PlatformResource
from persistence.model.basic_info import SchoolResource
from persistence.model.basic_info import CourseGroupInfo
from data_process.processors.base_process_platformcourse import BaseProcessorPlatformCourse
from threading import Thread
import pandas as pd


class ProcessorPlatformCourse(BaseProcessorPlatformCourse):
    def __init__(self):
        super().__init__()
        # 平台名不输入， 自己填写
        self.platform = '智慧职教'
        self.yesterday = datetime.datetime(1999, 1, 1)
        self.validData = list()
        self.validCourseGroup = list()
        # self.validCourseGroupDict = dict()

    def process_platform(self, course_list_info=None):
        # 参数course_list_info 是一个CourseListInfo 列表， 只包含当前platform的数据(且valid = 1)，但并不筛选其他条件如update_time，平台自己处理
        # 返回类型为PlatformResource
        open_course_list = list()
        platform_info = PlatformResource()
        platform_info.update_time = datetime.datetime.now()
        platform_info.accumulate_course_num = len(self.validCourseGroup)
        platform_info.platform = '智慧职教'
        for course in self.validCourseGroup:
            if course.status == 1:
                open_course_list.append(course)
        platform_info.open_course_num = len(open_course_list)
        for semester in self.validData:
            if semester.block == "MOOC学院":
                if type(semester.crowd_num) == int:
                    platform_info.accumulate_crowd += semester.crowd_num
                if semester.status == 1:
                    if type(semester.crowd_num) == int:
                        platform_info.open_course_crowd += semester.crowd_num
            elif semester.block == "资源库" or semester.block == "数字课程":
                if type(semester.total_crowd_num) == int:
                    platform_info.accumulate_crowd += semester.total_crowd_num
                if semester.status == 1:
                    if type(semester.total_crowd_num) == int:
                        platform_info.open_course_crowd += semester.total_crowd_num
        # print(platform_info.__dict__)
        return platform_info
        pass

    def process_school(self, course_list_info=None):
        # 参数course_list_info不变
        # 返回类型为SchoolResource列表， PlatformResource和SchoolResource字段是一致的
        school_dict = dict()
        school_list = list()
        for valid_course in self.validData:
            if valid_course.school in school_dict.keys():
                school_dict[valid_course.school].append(valid_course)
            else:
                course_list = list()
                course_list.append(valid_course)
                school_dict[valid_course.school] = course_list
        for key in school_dict.keys():
            school_info = SchoolResource()
            course_dict = dict()
            for valid_semester in school_dict[key]:
                if valid_semester.block == "MOOC学院":
                    if type(valid_semester.crowd_num) == int:
                        school_info.accumulate_crowd += valid_semester.crowd_num
                    if valid_semester.status == 1:
                        if type(valid_semester.crowd_num) == int:
                            school_info.open_course_crowd += valid_semester.crowd_num
                elif valid_semester.block == "数字课程" or valid_semester.block == "资源库":
                    if type(valid_semester.total_crowd_num) == int:
                        school_info.accumulate_crowd += valid_semester.total_crowd_num
                    if valid_semester.status == 1:
                        if type(valid_semester.total_crowd_num) == int:
                            school_info.open_course_crowd += valid_semester.total_crowd_num
                if valid_semester.course_group_id in course_dict.keys():
                    if valid_semester.status == 1:
                        course_dict[valid_semester.course_group_id].status = 1
                else:
                    course_group_info = CourseGroupInfo()
                    course_group_info.course_group_id = valid_semester.course_group_id
                    course_group_info.platform = self.platform
                    course_group_info.course_name = valid_semester.course_name
                    course_group_info.teacher = valid_semester.teacher
                    course_group_info.school = valid_semester.school
                    course_group_info.term_count = valid_semester.term_number
                    if valid_semester.status == 1:
                        course_group_info.status = 1
                    else:
                        course_group_info.status = 0
                    course_group_info.update_time = datetime.datetime.now()
                    course_dict[valid_semester.course_group_id] = course_group_info
            school_info.update_time = datetime.datetime.now()
            school_info.school_name = key
            for key2 in course_dict.keys():
                school_info.accumulate_course_num += 1
                if course_dict[key2].status == 1:
                    school_info.open_course_num += 1
            school_list.append(school_info)
        course_num = 0
        open_course_num = 0
        for value in school_list:
            course_num += value.accumulate_course_num
            open_course_num += value.open_course_num
        #     print(value.__dict__)
        # print(course_num, open_course_num)
        return school_list
        pass

    def process_course_group(self, course_list_info=None):
        # 参数course_list_info不变
        # 返回类型为CourseGroupInfo列表
        course_dict = dict()
        for valid_semester in self.validData:
            if valid_semester.course_group_id in course_dict.keys():
                if valid_semester.status == 1:
                    course_dict[valid_semester.course_group_id].status = 1
            else:
                course_group_info = CourseGroupInfo()
                course_group_info.course_group_id = valid_semester.course_group_id
                course_group_info.platform = self.platform
                course_group_info.course_name = valid_semester.course_name
                course_group_info.teacher = valid_semester.teacher
                course_group_info.school = valid_semester.school
                if valid_semester.block == "MOOC学院":
                    course_group_info.term_count = valid_semester.term_number
                else:
                    course_group_info.term_count = 1
                if valid_semester.status == 1:
                    course_group_info.status = 1
                else:
                    course_group_info.status = 0
                course_group_info.update_time = datetime.datetime.now()
                course_dict[valid_semester.course_group_id] = course_group_info
        for value in course_dict.values():
            self.validCourseGroup.append(value)
            # print(value.__dict__)
        return self.validCourseGroup
        pass

    def run(self, course_list_info=None):
        # 参数course_list_info不变
        # 前面三个函数只是规范，如果情况方便可以不分开写
        # 上层函数只调用run接口，返回以下三个参数，CourseGroupInfo列表，PlatformResource对象， SchoolResource列表
        # 继承的类名统一为ProcessorPlatformCourse, 文件名标识平台即可，直接发群里
        timeList = [item.update_time for item in course_list_info]
        yesterday = max(timeList).date() - datetime.timedelta(days=1)
        print(yesterday)
        for course_semester_info in course_list_info:
            if (course_semester_info.school == "" or course_semester_info.school == None):
                course_semester_info.school = "空"
            if ((course_semester_info.update_time).date() >= yesterday):
                self.validData.append(course_semester_info)
        return self.process_course_group(course_list_info), self.process_platform(
            course_list_info), self.process_school(course_list_info)


if __name__ == '__main__':
    config = {"host": "localhost", "port": 3306, "username": "root", "password": "943652865",
              "database": "mooc_test"}
    course_list_info_repo = CourseListInfoRepository(**config)
    course_list_info = course_list_info_repo.get_all_list()
    course_list_info2 = list()
    for item in course_list_info:
        if item.platform == "智慧职教" and item.valid == 1:
            course_list_info2.append(item)
    test = ProcessorPlatformCourse()
    test.run(course_list_info2)
    # course_ziyuan = list()
    # course_shuzi = list()
    # for item2 in course_list_info2:
    #     if item2.block == "资源库":
    #         course_ziyuan.append(item2)
    #     elif item2.block == "数字课程":
    #         course_shuzi.append(item2)
    # for semester in course_ziyuan:
    #     for item3 in course_shuzi:
    #         if semester.course_name == item3.course_name and semester.school == item3.school and semester.teacher == item3.teacher:
    #             print("------------------------")
    #             print(semester.__dict__)
    #             print(item3.__dict__)
    #             print("----------------------------")


class PlatformProcessor(BaseProcessor):
    """
    需要先手动在数据库把block中的资源库和数字课程中的term_id改为1
    """
    def __init__(self, processor: Processor):
        super().__init__(processor)
        self.platform = "智慧职教"
        self.__conn = mysql.connector.connect(host=self.processor.repo["host"], user=self.processor.repo["username"],
                                              password=self.processor.repo["password"],
                                              database=self.processor.repo["database"], port=self.processor.repo["port"])

    def init_table(self):
        sql = "select a.course_group_id,a.course_name,a.school,a.teacher,a.platform,a.status,a.term_id,a.block from course_list_info a \
        inner join (select course_group_id,max(term_id) as term_count from course_list_info where platform=\"智慧职教\" and course_group_id != '' group by course_group_id)b \
        on a.course_group_id=b.course_group_id and a.term_id =b.term_count;"
        res = self.processor.course_list_repo.fetch_all(sql)
        courses = []
        for course in res:
            course_group = CourseGroupInfo()
            course_group.course_group_id = course[0]
            course_group.course_name = course[1]
            course_group.school = course[2]
            course_group.teacher = course[3]
            course_group.platform = course[4]
            course_group.status = 0 if course[5] == 0 else 1
            course_group.term_count = course[6]
            course_group.update_time = datetime.now()
            courses.append(course_group)
        self.processor.course_group_repo.create_course_groups(courses)
        pass

    def platform_process(self):
        pass

    def school_process(self):
        pass

    def course_groupify(self):
        pass

    def process_table_old_course(self):
        sql = "select course_id,replace(url,'https://mooc.icve.com.cn/course.html?cid=','') " \
              "from course_list_info where platform = '智慧职教' and course_group_id = '' and block = 'MOOC学院';"
        res = self.processor.course_list_repo.fetch_all(sql)
        cursor = self.__conn.cursor()
        for course in res:
            str2 = course[1].split("#oid=")[0]
            hash_md5 = hashlib.md5((str2+"智慧职教").encode('utf-8')).hexdigest()
            sql = "update course_list_info set course_group_id = " + "'" + hash_md5 + "'" + \
                  " , platform_course_id = " + "'" + str2 + "'" + " where course_id = " + str(course[0]) + ";"
            try:
                cursor.execute(sql)
                self.__conn.commit()
                print(cursor.rowcount, " 条MOOC学院记录被修改")
            except Exception as e:
                self.__conn.rollback()
                print("更新失败：", e)
        sql = "update course_list_info inner join" \
              " (select course_id,replace(url,'https://www.icve.com.cn/portal_new/courseinfo/courseinfo.html?courseid=','') " \
              "as a from course_list_info where platform = '智慧职教' and course_group_id = '' and block in ('资源库', '数字课程')) b " \
              "on course_list_info.course_id = b.course_id set course_list_info.course_group_id = MD5(CONCAT(a,'智慧职教')) , " \
              "course_list_info.platform_course_id = a;"
        try:
            cursor.execute(sql)
            self.__conn.commit()
            print(cursor.rowcount, " 条(资源库/数字课程)记录被修改")
            cursor.close()
            self.__conn.close()
        except Exception as e:
            self.__conn.rollback()
            print("更新失败：", e)
        pass

    def run(self):
        self.course_groupify()
        self.school_process()
        self.platform_process()