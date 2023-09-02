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


if __name__ == '__main__':
    config = ProcessConfig()
    processor = Processor(config)
    platform_processor = PlatformProcessor(processor)
    platform_processor.init_table()