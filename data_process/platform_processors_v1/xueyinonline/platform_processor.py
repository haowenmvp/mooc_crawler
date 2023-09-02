import hashlib
from datetime import datetime

from data_process.processor import Processor
from utils.utils import load_class_type
from persistence.db.course_list_info_repo import CourseListInfoRepository
from persistence.db.course_group_repo import CourseGroupRepository
from persistence.db.platform_resource_repo import PlatformResourceRepository
from persistence.db.school_resource_repo import SchoolResourceRepository
from persistence.model.basic_info import CourseGroupInfo
from persistence.model.basic_info import CourseListInfo
from persistence.model.process_config import ProcessConfig
from crawler.fetcher.xueyinonline.list_fetcher import ListFetcher


class PlatformProcessor:
    def __init__(self, processor: Processor):
        self.platform = "北京学银在线教育科技有限公司"
        self.processor = processor

    def process_table_old_course(self):
        no_group_id_sql = 'select url from course_list_info where platform = "北京学银在线教育科技有限公司" and course_group_id = ""'
        no_group_id_res = self.processor.course_list_repo.fetch_all(no_group_id_sql)
        no_group_id_urls = []
        print(len(no_group_id_res))
        for no_group_id_course in no_group_id_res:
            url = str(no_group_id_course[0])
            no_group_id_urls.append(url)
        driver = ''
        fetcher = ListFetcher(driver)
        data = fetcher.run_by_urls(no_group_id_urls)
        for course_info in data['course_list_info']:
            course_list_info = CourseListInfo()
            course_list_info.platform_course_id = course_info['platform_course_id']
            course_list_info.platform_term_id = course_info['platform_term_id']
            course_list_info.course_group_id = course_info['course_group_id']
            course_list_info.platform = course_info['platform']
            course_id = self.processor.course_list_repo.get_courseid_by_term_id(course_list_info)
            self.processor.course_list_repo.update_ids_by_course_id(course_list_info, course_id)

    # 初始化表
    def init_table(self):
        sql = "select a.course_group_id,a.course_name,a.school,a.teacher,a.platform,a.status from course_list_info a \
            inner join (select course_group_id from course_list_info where platform=\"北京学银在线教育科技有限公司\"  and course_group_id<>'')b \
            on a.course_group_id=b.course_group_id group by course_group_id"
        res = self.processor.course_list_repo.fetch_all(sql)
        print(len(res))
        courses = []
        for course in res:
            course_group = CourseGroupInfo()
            course_group.course_group_id = course[0]
            course_group.course_name = course[1]
            course_group.school = course[2]
            course_group.teacher = course[3]
            course_group.platform = course[4]
            course_group.status = 0 if course[5] == 0 else 1
            course_group.update_time = datetime.now()
            count_sql = "SELECT COUNT(course_id) AS term_count FROM course_list_info WHERE `course_group_id`='%s'" % (course[0])
            count_res = self.processor.course_list_repo.fetch_all(count_sql)
            course_group.term_count = count_res[0][0]
            print('add:' + str(course_group))
            courses.append(course_group)
        self.processor.course_group_repo.create_course_groups(courses)


if __name__ == '__main__':
    config = ProcessConfig()
    processor = Processor(config)
    platform_processor = PlatformProcessor(processor)
    platform_processor.process_table_old_course()
    platform_processor.init_table()
