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
        self.platform = "中国高校外语慕课平台"
        self.__conn = mysql.connector.connect(host=self.processor.repo["host"], user=self.processor.repo["username"],
                                              password=self.processor.repo["password"],
                                              database=self.processor.repo["database"], port=self.processor.repo["port"])


    def init_table(self):
        # sql = "update course_list_info inner join " \
        #       "(select course_id, url from course_list_info where platform = '中国高校外语慕课平台') a " \
        #       "on course_list_info.course_id = a.course_id " \
        #       "set course_list_info.platform_term_id = replace(a.url, 'http://moocs.unipus.cn/course/','');"
        # cursor = self.__conn.cursor()
        # try:
        #     cursor.execute(sql)
        #     self.__conn.commit()
        #     print(cursor.rowcount, " 条记录被修改")
        #     cursor.close()
        #     self.__conn.close()
        # except Exception as e:
        #     self.__conn.rollback()
        #     print("更新失败：", e)
        sql = "select a.course_group_id,a.course_name,a.school,a.teacher,a.platform,a.status,a.term_id from course_list_info a \
    inner join (select distinct(course_group_id),max(term_id) as term_count from course_list_info where platform=\"中国高校外语慕课平台\" and course_group_id != '' group by course_group_id)b \
    on a.course_group_id=b.course_group_id and a.term_id =b.term_count"
        res = self.processor.course_list_repo.fetch_all(sql)
        courses = []
        for course in res:
            Found = False
            for item in courses:
                if course[0] == item.course_group_id:
                    Found = True
            course_group = CourseGroupInfo()
            course_group.course_group_id = course[0]
            course_group.course_name = course[1]
            course_group.school = course[2]
            course_group.teacher = course[3]
            course_group.platform = course[4]
            course_group.status = 0 if course[5] == 0 else 1
            course_group.term_count = course[6]
            course_group.update_time = datetime.now()
            if Found == True:
                continue
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
        pass

    def run(self):
        self.course_groupify()
        self.school_process()
        self.platform_process()

if __name__ == '__main__':
    config = ProcessConfig()
    processor = Processor(config)
    platform_processor = PlatformProcessor(processor)
    platform_processor.init_table()