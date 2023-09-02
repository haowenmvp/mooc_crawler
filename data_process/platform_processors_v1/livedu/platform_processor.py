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


class PlatformProcessor:
    def __init__(self, processor: Processor):
        self.platform = "北京高校优质课程研究会"
        self.processor = processor

    def process_table_old_course(self):
        no_group_id_sql = "select * from course_list_info where platform = '北京高校优质课程研究会' and course_group_id = ''"
        no_group_id_res = self.processor.course_list_repo.fetch_all(no_group_id_sql)
        for no_group_id_course in no_group_id_res:
            course_list_info = CourseListInfo()
            cid = no_group_id_course[1].split('kcid=', 1)[1]  # no_group_id_course[1]为url
            group = no_group_id_course[4] + cid  # no_group_id_course[4]为platform
            group_hash = hashlib.md5(group.encode('utf-8'))
            course_list_info.platform_course_id = cid
            course_list_info.platform_term_id = cid
            course_list_info.course_group_id = group_hash.hexdigest()
            self.processor.course_list_repo.update_ids_by_course_id(course_list_info, no_group_id_course[0])

    def init_table(self):
        sql = "select a.course_group_id,a.course_name,a.school,a.teacher,a.platform,a.status,a.term_id from course_list_info a \
    inner join (select course_group_id,max(term_id) as term_count from course_list_info where platform=\"北京高校优质课程研究会\"  and course_group_id<>'' group by course_group_id)b \
    on a.course_group_id=b.course_group_id and a.term_id =b.term_count"
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


if __name__ == '__main__':
    config = ProcessConfig()
    processor = Processor(config)
    platform_processor = PlatformProcessor(processor)
    platform_processor.process_table_old_course()
    platform_processor.init_table()
