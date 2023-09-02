from data_process.processor import Processor
from utils.utils import load_class_type
from persistence.db.course_list_info_repo import CourseListInfoRepository
from persistence.db.course_group_repo import CourseGroupRepository
from persistence.db.platform_resource_repo import PlatformResourceRepository
from persistence.db.school_resource_repo import SchoolResourceRepository
from persistence.model.basic_info import CourseGroupInfo


class PlatformProcessor:
    def __init__(self, processor: Processor):
        self.platform = "爱课程(中国大学MOOC)"
        self.processor = processor


    # 初始化表
    def init_table(self):
        sql = "select a.course_group_id,a.course_name,a.school,a.teacher,a.platform,a.status,a.term_id from course_list_info a \
    inner join (select course_group_id,max(term_id) as term_count from course_list_info where platform=\"爱课程(中国大学MOOC)\"  and course_group_id is not NULL group by course_group_id)b \
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
            courses.append(course_group)
        self.processor.course_group_repo.create_course_groups(courses)


if __name__ == '__main__':
    processor = Processor(None)
    platform_processor = PlatformProcessor(processor)
    platform_processor.init_table()