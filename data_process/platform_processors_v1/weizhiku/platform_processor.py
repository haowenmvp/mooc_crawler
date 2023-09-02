from data_process.platform_processors import BaseProcessor
from data_process.processor import Processor
from persistence.model.process_config import ProcessConfig
import hashlib
from datetime import datetime
from persistence.model.basic_info import CourseGroupInfo


class PlatformProcessor(BaseProcessor):
    def __init__(self, processor: Processor):
        super(PlatformProcessor, self).__init__(processor)
        self.platform = '微知库数字校园学习平台'

    def init_table(self):
        # #1.更新数据产生所有platform_course_id，group_id，courese_group_id
        course_list = self.processor.course_list_repo.get_all_list_by_platform(self.platform)
        for course_list_info in course_list:
            url = course_list_info.url
            asub = url.split('?')[-1].split('&')[0]
            bsub = url.split('?')[-1].split('&')[1]
            group_id = bsub.split('=')[-1]
            course_list_info.platform_course_id = group_id
            course_list_info.platform_term_id = asub.split('=')[-1]
            hash_str = group_id + course_list_info.platform
            course_list_info.course_group_id = hashlib.md5(hash_str.encode('utf-8')).hexdigest()
            course_id = self.processor.course_list_repo.get_courseid_by_url(course_list_info)
            self.processor.course_list_repo.update_list_info_by_course_id(course_list_info, course_id)

        #2.根据course_group_id查询term_id和term_number
        for course_list_info in course_list:
            course_urls_sql = 'select url from course_list_info where platform="微知库数字校园学习平台" and course_group_id= "{}" order by start_date'
            term_number_sql = 'select count(*) from course_list_info where platform="微知库数字校园学习平台" and course_group_id= "{}"'
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
            self.processor.course_list_repo.update_weizhiku_by_course_id(course_list_info, course_id)

        #3.初始化crouse_group表
        # sql = 'SELECT * FROM (SELECT a.course_group_id,a.course_name,a.school,a.teacher,a.platform,a.STATUS,b.term_count FROM course_list_info a INNER JOIN (SELECT course_group_id,max( term_id ) AS term_id,count( term_id ) AS term_count FROM course_list_info WHERE platform = "微知库数字校园学习平台" AND course_group_id IS NOT NULL GROUP BY course_group_id ) b ON a.course_group_id = b.course_group_id AND a.term_id = b.term_id) c GROUP BY c.course_group_id '
        # res = self.processor.course_list_repo.fetch_all(sql)
        # courses = []
        # for course in res:
        #     course_group = CourseGroupInfo()
        #     course_group.course_group_id = course[0]
        #     course_group.course_name = course[1]
        #     course_group.school = course[2]
        #     course_group.teacher = course[3]
        #     course_group.platform = course[4]
        #     course_group.status = 0 if course[5] == 0 else 1
        #     course_group.term_count = course[6]
        #     course_group.update_time = datetime.now()
        #     courses.append(course_group)
        # self.processor.course_group_repo.create_course_groups(courses)

if __name__ == '__main__':
    config = ProcessConfig()
    processor = Processor(config)
    plt_processor = PlatformProcessor(processor)
    plt_processor.init_table()