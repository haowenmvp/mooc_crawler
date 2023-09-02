from data_process.platform_processors.base_processor import BaseProcessor
from data_process.processor import Processor
from persistence.model.process_config import ProcessConfig
import hashlib
from datetime import datetime
from persistence.model.basic_info import CourseGroupInfo
from persistence.model.basic_info import CourseListInfo
from persistence.model.basic_info import PlatformResource
from persistence.model.basic_info import SchoolResource
from persistence.model.basic_info import CourseGroupInfo
from data_process.processors.base_process_platformcourse import BaseProcessorPlatformCourse
from threading import Thread
import datetime


class ProcessorPlatformCourse(BaseProcessorPlatformCourse):
    def __init__(self):
        super().__init__()
        # 平台名不输入， 自己填写
        self.platform = '智慧树'

    def process_platform(self, course_list_info=None):
        # 参数course_list_info 是一个CourseListInfo 列表， 只包含当前platform的数据(且valid = 1)，但并不筛选其他条件如update_time，平台自己处理
        # 返回类型为PlatformResource
        platformResource = PlatformResource()
        course_group_bucket = dict()
        for course in course_list_info:
            if course.course_group_id in course_group_bucket.keys():
                course_group_bucket[course.course_group_id].append(course)
            else:
                course_group_bucket[course.course_group_id] = [course]

        accumulate_crowd = 0
        open_course_num = 0
        open_course_crowd = 0
        for key in course_group_bucket.keys():
            group = course_group_bucket[key]
            if len(group) == 0:
                continue

            status = 0
            for term in group:
                if term.status == 1:
                    status = 1
                    break
            if status == 1:
                open_course_num += 1

            for term in group:
                try:
                    accumulate_crowd += int(term.crowd)
                except:
                    pass
                if term.block == "大学共享课":
                    try:
                        open_course_crowd += int(term.crowd)
                    except:
                        pass

        platformResource.platform = self.platform
        platformResource.accumulate_course_num = len(course_group_bucket.keys())
        platformResource.accumulate_crowd = accumulate_crowd
        platformResource.open_course_num = open_course_num
        platformResource.open_course_crowd = open_course_crowd
        platformResource.update_time = datetime.datetime.now()
        return platformResource

    def process_school(self, course_list_info=None):
        # 参数course_list_info不变
        # 返回类型为SchoolResource列表， PlatformResource和SchoolResource字段是一致的
        schoolResourceList = []
        school_bucket = dict()
        for course in course_list_info:
            if course.school in school_bucket.keys():
                school_bucket[course.school].append(course)
            else:
                school_bucket[course.school] = [course]

        for key in school_bucket.keys():
            schoolResource = SchoolResource()
            schoolResource.school_name = key
            platformResource = self.process_platform(school_bucket[key])
            schoolResource.accumulate_course_num = platformResource.accumulate_course_num
            schoolResource.accumulate_crowd = platformResource.accumulate_crowd
            schoolResource.open_course_num = platformResource.open_course_num
            schoolResource.open_course_crowd = platformResource.open_course_crowd
            schoolResource.update_time = datetime.datetime.now()
            schoolResourceList.append(schoolResource)
        return schoolResourceList

    def process_course_group(self, course_list_info=None):
        # 参数course_list_info不变
        # 返回类型为CourseGroupInfo列表
        courseGroupInfoList = []
        course_group_bucket = dict()

        for course in course_list_info:
            if course.course_group_id in course_group_bucket.keys():
                course_group_bucket[course.course_group_id].append(course)
            else:
                course_group_bucket[course.course_group_id] = [course]

        for key in course_group_bucket.keys():
            courseGroupInfo = CourseGroupInfo()
            group = course_group_bucket[key]
            if len(group) == 0:
                continue
            courseGroupInfo.course_group_id = group[0].course_group_id
            courseGroupInfo.course_name = group[0].course_name
            courseGroupInfo.platform = self.platform
            courseGroupInfo.term_count = len(group)
            courseGroupInfo.teacher = group[0].teacher
            courseGroupInfo.school = group[0].school
            courseGroupInfo.status = 1
            courseGroupInfo.update_time = datetime.datetime.now()
            courseGroupInfoList.append(courseGroupInfo)
        return courseGroupInfoList

    def run(self, course_list_info=None):
        # 参数course_list_info不变
        # 前面三个函数只是规范，如果情况方便可以不分开写
        # 上层函数只调用run接口，返回以下三个参数，CourseGroupInfo列表，PlatformResource对象， SchoolResource列表
        # 继承的类名统一为ProcessorPlatformCourse, 文件名标识平台即可，直接发群里
        course_list_info = self.fliter(course_list_info)
        return self.process_course_group(course_list_info), self.process_platform(course_list_info), self.process_school(course_list_info)

    def fliter(self, course_list_info):
        time_ref = datetime.datetime(1999, 1, 1)
        for item in course_list_info:
            if item.update_time > time_ref:
                time_ref = item.update_time
        yesterday = time_ref - datetime.timedelta(days=1)
        course_list = []
        for course in course_list_info:
            if isinstance(course.update_time, str):
                course.update_time = datetime.datetime.strptime(course.update_time, "%Y-%m-%d %H:%M:%S")
            if course.update_time > yesterday:
                course_list.append(course)
        return course_list


class PlatformProcessor(BaseProcessor):
    def __init__(self, processor: Processor):
        super(PlatformProcessor, self).__init__(processor)
        self.platform = '智慧树'

    def init_table(self):
        # #1.更新数据产生所有platform_course_id，group_id，courese_group_id
        # course_list = self.processor.course_list_repo.get_course_list_by_platform(self.platform)
        # for course_list_info in course_list:
        #     url = course_list_info.url
        #     group_id = url.split('/')[-1].split('?')[0]
        #     course_list_info.platform_course_id = group_id
        #     course_list_info.platform_term_id = group_id
        #     hash_str = group_id + course_list_info.platform
        #     course_list_info.course_group_id = hashlib.md5(hash_str.encode('utf-8')).hexdigest()
        #     course_id = self.processor.course_list_repo.get_courseid_by_url(course_list_info)
        #     self.processor.course_list_repo.update_list_info_by_course_id(course_list_info, course_id)

        # 2.初始化crouse_group表
        sql = 'SELECT * FROM (SELECT a.course_group_id,a.course_name,a.school,a.teacher,a.platform,a.STATUS,b.term_count FROM course_list_info a INNER JOIN (SELECT course_group_id,max( term_id ) AS term_id,count( term_id ) AS term_count FROM course_list_info WHERE platform = "智慧树" AND course_group_id IS NOT NULL GROUP BY course_group_id ) b ON a.course_group_id = b.course_group_id AND a.term_id = b.term_id) c GROUP BY c.course_group_id '
        res = self.processor.course_list_repo.fetch_all(sql)
        courses = []
        # print(len(res))
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
