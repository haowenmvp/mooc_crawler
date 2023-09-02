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


class PlatformProcessor(BaseProcessor):
    def __init__(self, processor: Processor):
        super().__init__(processor)
        self.platform = "中科云教育"
        self.__conn = mysql.connector.connect(host=self.processor.repo["host"], user=self.processor.repo["username"],
                                              password=self.processor.repo["password"],
                                              database=self.processor.repo["database"], port=self.processor.repo["port"])

    def init_table(self):
        sql = "select a.course_group_id,a.course_name,a.school,a.teacher,a.platform,a.status,a.term_id from course_list_info a \
        inner join (select course_group_id,max(term_id) as term_count from course_list_info where platform=\"中科云教育\" and course_group_id != '' group by course_group_id)b \
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
        sql = "SELECT url, course_name, school, teacher FROM `mooc_test`.`course_list_info` " \
              "WHERE `platform` = '中科云教育' AND `course_group_id` = '' AND valid = 1;"
        res_empty_groupid = self.processor.course_list_repo.fetch_all(sql)
        cursor = self.__conn.cursor()
        for item in res_empty_groupid:
            sql2 = "select course_id from course_list_info where platform = '中科云教育' and course_group_id != ''" \
                   " and course_name = '" + item[1] + "' and school = '" + item[2] + "' and teacher = '" + item[3] + "';"
            if len(self.processor.course_list_repo.fetch_all(sql2)):
                sql3 = "update course_list_info set valid = 0 where platform = '中科云教育' and course_group_id = '' and " \
                       "course_name = '" + item[1] + "' and school = '" + item[2] + "' and teacher = '" + item[3] + "';"
                try:
                    cursor.execute(sql3)
                    self.__conn.commit()
                    print(cursor.rowcount, " 条记录被修改为无效")
                except Exception as e:
                    self.__conn.rollback()
                    print(sql3)
                    print("error:", e)
            else:
                sql4 = "update course_list_info set course_group_id = " \
                       "MD5(CONCAT('"+item[1]+"','"+item[2]+"','"+item[3]+"','中科云教育')) " \
                       "where url = " + "'" + item[0] + "'" + " and platform = '中科云教育'"
                try:
                    cursor.execute(sql4)
                    self.__conn.commit()
                    print(cursor.rowcount, " 条有效记录增添了course_group_id")
                except Exception as e:
                    self.__conn.rollback()
                    print(sql4)
                    print("error:", e)
        cursor.close()
        self.__conn.close()
        pass

    def run(self):
        self.course_groupify()
        self.school_process()
        self.platform_process()

if __name__ == '__main__':
    config = ProcessConfig()
    processor = Processor(config)
    platform_processor = PlatformProcessor(processor)
    platform_processor.process_table_old_course()