# -*- coding: UTF-8 -*-
import datetime
import mysql.connector
from typing import List
from persistence.model.task import Task
from persistence.db.course_list_info_repo import CourseListInfoRepository
from persistence.db.task_info_repo import TaskInfoRepository
from persistence.model.basic_info import CourseListInfo
from persistence.db.api.mysql_api import MysqlApi


class LoadTaskInfo(object):
    def __init__(self, config):
        self.course_list_info_repo = CourseListInfoRepository(**config)
        self.task_info_repo = TaskInfoRepository(**config)
        self.api_type = MysqlApi
        self.__db_api = self.api_type(**config)
        self.__conn = mysql.connector.connect(host=config["host"], user=config["username"], password=config["password"], database=config["database"], port=config["port"])

    def create_forum_task_info_by_course_info_list(self, course_info_list: List[CourseListInfo], type_info: dict):
        course_numner = 0
        for course in course_info_list:
            course_numner += 1
            task = self.__construct_task_info_by_course_list_info(course_list_info=course, type_info=type_info)
            self.task_info_repo.create_task_info(task)
        pass

    def load_all_course(self, platform: str, begin_date: datetime.datetime, end_date: datetime.datetime,
                        type_info: dict):
        course_info_list = self.course_list_info_repo.get_courses_info_by_platform_date(platform=platform,
                                                                                        begin_date=begin_date,
                                                                                        end_date=end_date,
                                                                                        field_type="update_time")
        self.create_forum_task_info_by_course_info_list(course_info_list=course_info_list, type_info=type_info)
        pass

    def load_crowd_top_course(self, platfroms: list, top_number: int, type_info: dict):
        cursor = self.__conn.cursor()
        course_list = list()
        for platform in platfroms:
            if platform != None:
                sql = "DELETE from task_info WHERE platform = '" + platform + "'"
                cursor.execute(sql)
                self.__conn.commit()
                # sql = "SELECT course_id, url, course_name, term, platform, school, teacher, crowd_num FROM course_list_info WHERE platform = " + "'" + platform + "'" + " AND update_time > " + "'" + datetime.datetime.now().date().strftime('%Y-%m-%d %H:%M:%S') + "'" +"AND status = 1" + " ORDER BY crowd_num DESC LIMIT " + str(top_number)
                sql = "SELECT course_id, url, course_name, term, platform, school, teacher, crowd_num FROM course_list_info WHERE platform = " + "'" + platform + "'" + " AND update_time > " + "'" + "2020-04-04 00:00:01" + "'" +"AND status = 1" + " ORDER BY crowd_num DESC LIMIT " + str(top_number)
                cursor.execute(sql)
                results = cursor.fetchall()
                for item in results:
                    course_list_info = CourseListInfo()
                    course_list_info.course_id = item[0]
                    course_list_info.url = item[1]
                    course_list_info.course_name = item[2]
                    course_list_info.term = item[3]
                    course_list_info.platform = item[4]
                    course_list_info.school = item[5]
                    course_list_info.teacher = item[6]
                    course_list.append(course_list_info)
        self.create_forum_task_info_by_course_info_list(course_info_list=course_list, type_info=type_info)


    @classmethod
    def __construct_task_info_by_course_list_info(cls, course_list_info: CourseListInfo, type_info: dict) -> Task:
        task = Task()
        task.course_id = course_list_info.course_id
        task.url = course_list_info.url
        task.course_name = course_list_info.course_name
        task.term = course_list_info.term
        task.platform = course_list_info.platform
        task.school = course_list_info.school
        task.teacher = course_list_info.teacher
        task.save_time = datetime.datetime.now()
        task.fetcher_type = type_info["fetcher_type"]
        task.is_need_crawled = True
        task.crawled_plan_num = type_info["crawled_plan_num"]
        task.crawled_finished_num = 0
        task.crawled_time_gap = type_info["crawled_time_gap"]
        task.crawled_next_time = type_info["crawled_next_time"]
        return task


if __name__ == '__main__':
    # config = {"host": "192.168.232.254", "port": 3307, "username": "root", "password": "123qweASD!@#", "database": "mooc_test"}
    config = {"host": "localhost", "port": 3306, "username": "root", "password": "943652865",
              "database": "mooc_test"}
    test = LoadTaskInfo(config)
    type_info = dict()
    type_info["fetcher_type"] = 'forum'
    type_info["crawled_plan_num"] = 0
    type_info["crawled_time_gap"] = 604800
    type_info["crawled_next_time"] = datetime.datetime(1999, 1, 1)
    test.load_crowd_top_course(["爱课程(中国大学MOOC)"], 100, type_info)
    # test.load_all_course("爱课程(中国大学MOOC)", datetime.datetime(2020, 4, 6, 0, 0, 1),
    #                      datetime.datetime(2020, 4, 6, 23, 12, 1), type_info)
