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


class PlatformProcessor(BaseProcessor):
    def __init__(self, processor: Processor):
        super().__init__(processor)
        self.platform = "优课联盟"
        self.__conn = mysql.connector.connect(host=self.processor.repo["host"], user=self.processor.repo["username"],
                                              password=self.processor.repo["password"],
                                              database=self.processor.repo["database"], port=self.processor.repo["port"])

    def init_table(self):
        sql = "select course_group_id,course_name,school,teacher,platform, max(`status`),max(term_id) " \
              "from course_list_info where platform = '优课联盟' and course_group_id != '' group by course_group_id;"
        res = self.processor.course_list_repo.fetch_all(sql)
        courses = []
        for course in res:
            course_group = CourseGroupInfo()
            course_group.course_group_id = course[0]
            course_group.course_name = course[1]
            course_group.school = course[2]
            course_group.teacher = course[3]
            course_group.platform = course[4]
            sql2 = "select status from course_list_info where platform = '优课联盟' " \
                   "and course_group_id =" + "'" + course[0] + "';"
            res2 = self.processor.course_list_repo.fetch_all(sql2)
            status = 0
            for term_status in res2:
                if term_status[0] == 1:
                   status = 1
                   break
                else:
                   status = 0
            course_group.status = 0 if status == 0 else 1
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
        sql = "update course_list_info inner join" \
              " (select course_id,replace(url,'http://www.uooc.net.cn/course/','') " \
              "as a from course_list_info where platform = '优课联盟' and course_group_id = '') b " \
              "on course_list_info.course_id = b.course_id set course_group_id = MD5(CONCAT(a,'优课联盟'));"
        cursor = self.__conn.cursor()
        try:
            cursor.execute(sql)
            self.__conn.commit()
            print(cursor.rowcount, " 条记录被修改")
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

if __name__ == '__main__':
    config = ProcessConfig()
    processor = Processor(config)
    platform_processor = PlatformProcessor(processor)
    platform_processor.init_table()
    # print(hashlib.md5('1234112174优课联盟'.encode('utf-8')).hexdigest())