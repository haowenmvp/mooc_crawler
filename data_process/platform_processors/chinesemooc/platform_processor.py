from ..base_processor import BaseProcessor
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
        self.platform = '华文慕课'

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

            accumulate_crowd += int(group[0].total_crowd) if group[0].total_crowd != '' else 0

            status = 0
            for term in group:
                if term.status == 1:
                    status = 1
                    break
            if status == 1:
                open_course_num += 1
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

            status = 0
            for term in group:
                if term.status == 1:
                    status = 1
                    break
            courseGroupInfo.course_group_id = group[0].course_group_id
            courseGroupInfo.course_name = group[0].course_name
            courseGroupInfo.platform = self.platform
            courseGroupInfo.term_count = len(group)
            courseGroupInfo.teacher = group[0].teacher
            courseGroupInfo.school = group[0].school
            courseGroupInfo.status = status
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
    def __init__(self):
        super(PlatformProcessor, self).__init__()

