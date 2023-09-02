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
from crawler.fetcher.ehuixue.list_fetcher import ListFetcher
from persistence.model.basic_info import CourseListInfo
from persistence.model.basic_info import PlatformResource
from persistence.model.basic_info import SchoolResource
from persistence.model.basic_info import CourseGroupInfo
from datetime import datetime, timedelta
from data_process.processors.base_process_platformcourse import BaseProcessorPlatformCourse
from threading import Thread


class ProcessorPlatformCourse(BaseProcessorPlatformCourse):
    def __init__(self):
        super().__init__()
        # 平台名不输入， 自己填写
        self.platform = '安徽省网络课程学习中心平台'

    def process_platform(self, time_ref, course_list_info=None):
        # 参数course_list_info 是一个CourseListInfo 列表， 只包含当前platform的数据(且valid = 1)，但并不筛选其他条件如update_time，平台自己处理
        # 返回类型为PlatformResource
        platform_resource_dict = {}
        for item in course_list_info:
            if item.update_time > time_ref:
                if item.course_group_id in platform_resource_dict:
                    if item.crowd_num is not None and item.crowd_num is not '':
                        platform_resource_dict[item.course_group_id]['crowd_num'] = platform_resource_dict[item.course_group_id]['crowd_num'] + item.crowd_num
                    if item.status == 1 or item.status == -1:
                        platform_resource_dict[item.course_group_id]['course_status'] = 1
                else:
                    if item.status == 1 or item.status == -1:
                        course_status = 1
                    else:
                        course_status = 0
                    crowd_num = 0
                    if item.crowd_num is not None and item.crowd_num != '':
                        crowd_num = item.crowd_num
                    if item.course_group_id is not None and item.course_group_id is not '':
                        platform_resource_dict[item.course_group_id] = {
                            'course_status': course_status,
                            'crowd_num': crowd_num
                        }
        platform_resource = PlatformResource()
        platform_resource.platform = self.platform
        platform_resource.update_time = datetime.now()
        for group_id in platform_resource_dict:
            platform_resource.accumulate_crowd = platform_resource.accumulate_crowd + platform_resource_dict[group_id]['crowd_num']
            platform_resource.accumulate_course_num = platform_resource.accumulate_course_num + 1
            if platform_resource_dict[group_id]['course_status'] == 1:
                platform_resource.open_course_crowd = platform_resource.open_course_crowd + platform_resource_dict[group_id]['crowd_num']
                platform_resource.open_course_num = platform_resource.open_course_num + 1
        # print('==============================================================')
        # print(str(platform_resource.__dict__))
        return platform_resource
        pass

    def process_school(self, time_ref, course_list_info=None):
        # 参数course_list_info不变
        # 返回类型为SchoolResource列表， PlatformResource和SchoolResource字段是一致的
        school_resource_list = []
        school_resource_dict = {}
        for item in course_list_info:
            if item.update_time > time_ref:
                if item.school in school_resource_dict:
                    if item.course_group_id in school_resource_dict[item.school]:
                        if item.crowd_num is not None and item.crowd_num is not '':
                            school_resource_dict[item.school][item.course_group_id]['crowd_num'] = school_resource_dict[item.school][item.course_group_id]['crowd_num'] + item.crowd_num
                        if item.status == 1 or item.status == -1:
                            school_resource_dict[item.school][item.course_group_id]['course_status'] = 1
                    else:
                        if item.status == 1 or item.status == -1:
                            course_status = 1
                        else:
                            course_status = 0
                        crowd_num = 0
                        if item.crowd_num is not None and item.crowd_num is not '':
                            crowd_num = item.crowd_num
                        if item.course_group_id is not None and item.course_group_id is not '':
                            school_resource_dict[item.school][item.course_group_id] = {
                                'course_status': course_status,
                                'crowd_num': crowd_num
                            }
                else:
                    if item.school is not None:
                        school_resource_dict[item.school] = {}
                        if item.status == 1 or item.status == -1:
                            course_status = 1
                        else:
                            course_status = 0
                        crowd_num = 0
                        if item.crowd_num is not None and item.crowd_num is not '':
                            crowd_num = item.crowd_num
                        if item.course_group_id is not None and item.course_group_id is not '':
                            school_resource_dict[item.school][item.course_group_id] = {
                                'course_status': course_status,
                                'crowd_num': crowd_num
                            }
        for school in school_resource_dict:
            school_resource = SchoolResource()
            school_resource.school_name = school
            school_resource.update_time = datetime.now()
            for group_id in school_resource_dict[school]:
                school_resource.accumulate_course_num = school_resource.accumulate_course_num + 1
                school_resource.accumulate_crowd = school_resource.accumulate_crowd + school_resource_dict[school][group_id]['crowd_num']
                if school_resource_dict[school][group_id]['course_status'] == 1:
                    school_resource.open_course_num = school_resource.open_course_num + 1
                    school_resource.open_course_crowd = school_resource.open_course_crowd + school_resource_dict[school][group_id]['crowd_num']
            school_resource_list.append(school_resource)
        #     print('-------------------------------------------------------------------')
        #     print(str(school_resource.__dict__))
        # print(str(len(school_resource_list)))
        return school_resource_list
        pass

    def process_course_group(self, time_ref, course_list_info=None):
        # 参数course_list_info不变
        # 返回类型为CourseGroupInfo列表
        course_group_list = []
        course_group_dict = {}
        for item in course_list_info:
            if item.update_time > time_ref:
                if item.course_group_id in course_group_dict:
                    if item.term_id != 0:
                        course_group_dict[item.course_group_id].term_count = course_group_dict[item.course_group_id].term_count + 1
                    if item.status == 1 or item.status == -1:
                        course_group_dict[item.course_group_id].status = CourseGroupInfo.CourseGroupStatusEnum.kStatusOn
                else:
                    course_group_info = CourseGroupInfo()
                    course_group_info.course_group_id = item.course_group_id
                    course_group_info.course_name = item.course_name
                    course_group_info.platform = item.platform
                    course_group_info.teacher = item.teacher
                    course_group_info.school = item.school
                    course_group_info.update_time = datetime.now()
                    course_group_info.status = CourseGroupInfo.CourseGroupStatusEnum.kStatusEnd
                    if item.term_id != 0:
                        course_group_info.term_count = 1
                    if item.status == 1 or item.status == -1:
                        course_group_info.status = CourseGroupInfo.CourseGroupStatusEnum.kStatusOn
                    if item.course_group_id is not None and item.course_group_id is not '':
                        course_group_dict[item.course_group_id] = course_group_info
        for group in course_group_dict:
            course_group_list.append(course_group_dict[group])
        #     print(str(course_group_dict[group].__dict__))
        # print(str(len(course_group_list)))
        return course_group_list
        pass

    def run(self, course_list_info=None):
        # 参数course_list_info不变
        # 前面三个函数只是规范，如果情况方便可以不分开写
        # 上层函数只调用run接口，返回以下三个参数，CourseGroupInfo列表，PlatformResource对象， SchoolResource列表
        # 继承的类名统一为ProcessorPlatformCourse, 文件名标识平台即可，直接发群里
        time_ref = datetime(1999, 1, 1)
        for item in course_list_info:
            if item.update_time > time_ref:
                time_ref = item.update_time
        time_ref = time_ref + timedelta(-1)
        return self.process_course_group(time_ref, course_list_info), self.process_platform(time_ref, course_list_info), self.process_school(time_ref, course_list_info)


class PlatformProcessor:
    def __init__(self, processor: Processor):
        self.platform = "安徽省网络课程学习中心平台"
        self.processor = processor

    def process_table_old_course(self):
        no_group_id_sql = "select * from course_list_info where platform = '安徽省网络课程学习中心平台' and course_group_id = ''"
        no_group_id_res = self.processor.course_list_repo.fetch_all(no_group_id_sql)
        no_group_id_urls = []
        # print(len(no_group_id_res))
        for no_group_id_course in no_group_id_res:
            url = str(no_group_id_course[1])
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
            inner join (select course_group_id from course_list_info where platform=\"安徽省网络课程学习中心平台\"  and course_group_id<>'')b \
            on a.course_group_id=b.course_group_id group by course_group_id"
        res = self.processor.course_list_repo.fetch_all(sql)
        # print(len(res))
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
            # print('add:' + str(course_group))
            courses.append(course_group)
        self.processor.course_group_repo.create_course_groups(courses)
