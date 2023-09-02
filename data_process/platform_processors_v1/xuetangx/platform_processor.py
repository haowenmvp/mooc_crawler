import hashlib
import re
from datetime import datetime

import mysql.connector

from data_process.platform_processors.base_processor import BaseProcessor
from data_process.processor import Processor
from persistence.db.course_group_repo import CourseGroupRepository
from persistence.db.course_list_info_repo import CourseListInfoRepository
from persistence.db.platform_resource_repo import PlatformResourceRepository
from persistence.db.school_resource_repo import SchoolResourceRepository
from persistence.model.basic_info import CourseGroupInfo, CourseListInfo
from persistence.model.process_config import ProcessConfig


class PlatformProcessor(BaseProcessor):
    """
    需要手动在数据库把一些直播课，体验课等无效课程的valid改为0
    """

    def __init__(self, processor: Processor):
        super().__init__(processor)
        self.platform = "学堂在线"
        self.__conn = mysql.connector.connect(host=self.processor.repo["host"], user=self.processor.repo["username"],
                                              password=self.processor.repo["password"],
                                              database=self.processor.repo["database"],
                                              port=self.processor.repo["port"])

    def init_table(self):
        # sql = "update course_list_info inner join" \
        #       " (select course_id, url from course_list_info where platform = '学堂在线' and valid = 1) a " \
        #       "on course_list_info.course_id = a.course_id set platform_term_id = " \
        #       "SUBSTRING_INDEX(replace(a.url, 'https://next.xuetangx.com/course/', ''), '/', -1);"
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
        inner join (select course_group_id,max(term_id) as term_count from course_list_info where platform=\"学堂在线\" and course_group_id !='' group by course_group_id)b \
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

    def process_table_old_course(self):
        sql = "select course_id,SUBSTRING_INDEX(replace(url, 'https://next.xuetangx.com/course/', ''), '/', 1),course_name " \
              "from course_list_info where platform = '学堂在线' and course_group_id = '' and valid = 1"
        res = self.processor.course_list_repo.fetch_all(sql)
        cursor = self.__conn.cursor()
        for item in res:
            course_id = item[0]
            course_name = re.sub(r'(20\d+春)|（20\d+春）|(20\d+秋)|（20\d+秋）', '', item[2]).replace("()", '')
            group_id = hashlib.md5(("".join(list(filter(str.isdigit, item[1]))) + course_name + "学堂在线").encode('utf-8')).hexdigest()
            sql2 = "update course_list_info set course_name = '" + course_name +"' ,course_group_id = " + "'" + group_id + "'" + ", platform_course_id = '" + "".join(list(filter(str.isdigit, item[1]))) + course_name + "' where course_id = " + str(
                course_id) + ";"
            try:
                cursor.execute(sql2)
                self.__conn.commit()
                print(cursor.rowcount, " 条记录被修改")
            except Exception as e:
                self.__conn.rollback()
                print("更新失败：", e)
        cursor.close()
        self.__conn.close()
        pass

    def platform_process(self):
        pass

    def school_process(self):
        pass

    def course_groupify(self):
        pass


    def init_table2(self):
        course_list = self.processor.course_list_repo.get_course_list_by_platform(self.platform)
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
                urls = self.processor.course_list_repo.fetch_all(course_urls_sql)
                term_number = self.processor.course_list_repo.fetch_all(term_number_sql)[0][0]
                for item in urls:
                    url_list.append(item[0])
                term_id = url_list.index(course_list_info.url) + 1
                course_list_info.term_id = term_id
                course_list_info.term_number = term_number
                course_id = self.processor.course_list_repo.get_courseid_by_url(course_list_info)
                course_list_info.course_id = course_id
                self.processor.course_list_repo.update_list_info_by_course_id(course_list_info, course_id)
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
