from datetime import datetime
from typing import List


class PlatformInfo:

    def __init__(self):
        self.platform_id = 0
        self.platform_name = ''
        self.platform_mainpage = ''


class SchoolInfo:
    class SchoolTypeEnum:
        kTypeUnknown = 0
        kTypeHigh = 1
        kTypeVocational = 2

    def __init__(self):
        self.school_id = 0
        self.school_name = ''
        self.school_type = SchoolInfo.SchoolTypeEnum.kTypeUnknown


class TeacherInfo:
    class TeacherTitleEnum:
        kTypeUnknown = 0
        kProfessor = 1
        kAssociateProfessor = 2
        kLecturer = 3
        kAssistant = 4

    def __init__(self):
        self.teacher_id = ''
        self.teacher_name = ''
        self.teacher_school = ''
        self.teacher_department = ''
        self.teacher_titles = TeacherInfo.TeacherTitleEnum.kTypeUnknown
        self.teacher_forumid = 0


class PlatformResource:
    def __init__(self):
        self.id = 0
        self.platform = ''
        self.accumulate_course_num = 0
        self.accumulate_crowd = 0
        self.open_course_num = 0
        self.open_course_crowd = 0
        self.update_time = datetime(1999, 1, 1)


class SchoolResource:
    def __init__(self):
        self.id = 0
        self.school_name = ''
        self.accumulate_course_num = 0
        self.accumulate_crowd = 0
        self.open_course_num = 0
        self.open_course_crowd = 0
        self.update_time = datetime(1999, 1, 1)


class CourseCrowdWeekly:
    class CourseStatusEnum:
        kStatusEnd = 0  # 结束
        kStatusOn = 1  # 进行中
        kStatusLaterOn = 2  # 未开始
        kStatusAlwaysOn = -1  # 自主模式

    def __init__(self):
        self.id = 0
        self.course_id = 0
        self.url = ''
        self.course_name = ''
        self.term = ''
        self.term_id = 0
        self.platform = ''
        self.school = ''
        self.teacher = ''
        self.start_date = datetime(1999, 1, 1)
        self.end_date = datetime(1999, 1, 1)
        self.update_time = datetime(1999, 1, 1)
        self.status = CourseCrowdWeekly.CourseStatusEnum.kStatusOn
        self.crowd = ''
        self.total_crowd = ''
        self.crowd_num = None
        self.total_crowd_num = None
        self.term_number = -1
        self.block = ''


class CourseGroupInfo:
    class CourseGroupStatusEnum:
        kStatusEnd = 0  # 结束
        kStatusOn = 1  # 进行中

    def __init__(self):
        self.id = 0
        self.course_group_id = '0'
        self.course_name = ''
        self.platform = ''
        self.term_count = 0  # 开课学期数
        self.teacher = ''
        self.school = ''
        self.status = CourseGroupInfo.CourseGroupStatusEnum.kStatusOn
        self.update_time = datetime(1999, 1, 1)


class CourseListInfo:
    kStatusEnd = 0  # 结束
    kStatusOn = 1  # 进行中
    kStatusLaterOn = 2  # 未开始
    kStatusUnknown = 3  # 未知
    kStatusAlwaysOn = -1  # 自主模式
    kNotQuality = 0  # 非精品课程
    kIsQuality = 1  # 精品课程
    kQualityUnknown = 2  # 不能获取是否为精品课程
    kIsPublic = 1  # 公开课
    kNotPublic = 0  # 非公开课
    kIsFree = 1  # 免费
    kNotFree = 0  # 收费

    def __init__(self):
        self.course_id = 0
        self.url = ''
        self.course_name = ''
        self.term_id = 0
        self.term = ''
        self.team = ''
        self.platform = ''
        self.school = ''
        self.teacher = ''
        self.start_date = datetime(1999, 1, 1)
        self.end_date = datetime(1999, 1, 1)
        self.save_time = datetime(1999, 1, 1)
        self.status = CourseListInfo.kStatusUnknown
        self.extra = ''
        self.crowd = ''
        self.total_crowd = ''
        self.crowd_num = None
        self.total_crowd_num = None
        self.term_number = -1
        self.clicked = ''
        self.isquality = CourseListInfo.kQualityUnknown
        self.course_score = ''  # 课程评分
        self.introduction = ''  # 课程简介
        self.subject = ''  # 学科分类
        self.course_attributes = ''  # 课程属性
        self.course_target = ''  # 课程目标
        self.scoring_standard = ''  # 考核评分标准
        self.isfree = CourseListInfo.kIsFree  # 是否免费，默认为免费
        self.certification = ''  # 认证情况
        self.ispublic = CourseListInfo.kIsPublic  # 是否为公开课，默认为公开课
        self.block = ''
        self.platform_course_id = ''
        self.platform_term_id = ''
        self.course_group_id = ''
        self.valid = 1

    def __eq__(self, other):
        return (self.course_id == other.course_id and
                self.url == other.url and
                self.course_name == other.course_name and
                self.term_id == other.term_id and
                self.term == other.term and
                self.team == other.team and
                self.platform == other.platform and
                self.school == other.school and
                self.teacher == other.teacher and
                self.start_date == other.start_date and
                self.end_date == other.end_date and
                self.status == other.status and
                self.extra == other.extra and
                self.crowd == other.crowd and
                self.total_crowd == other.total_crowd and
                self.crowd_num == other.crowd_num and
                self.total_crowd_num == other.total_crowd_num and
                self.term_number == other.term_number and
                self.clicked == other.clicked and
                self.isquality == other.isquality and
                self.course_score == other.course_score and
                self.introduction == other.introduction and
                self.subject == other.subject and
                self.course_target == other.course_target and
                self.scoring_standard == other.scoring_standard and
                self.isfree == other.isfree and
                self.certification == other.certification and
                self.ispublic == other.ispublic and
                self.block == other.block)

    def __hash__(self):
        return hash(str(self.url) + str(self.course_name) + str(self.term_id) + str(self.term) + str(self.team) + str(
            self.platform) + str(self.school) + str(
            self.teacher) + str(self.start_date) + str(self.end_date) + str(self.status) + str(self.extra) + str(
            self.crowd) + str(self.total_crowd) + str(
            self.crowd_num) + str(self.total_crowd_num) + str(self.term_number) + str(self.clicked) + str(
            self.isquality) + str(
            self.course_score) + str(self.introduction) + str(self.subject) + str(self.course_target) + str(
            self.scoring_standard) + str(
            self.isfree) + str(self.certification) + str(self.ispublic) + str(self.block))

    def list2crowd(self) -> CourseCrowdWeekly:
        crowd = CourseCrowdWeekly()
        crowd.course_id = self.course_id
        crowd.course_name = self.course_name
        crowd.term = self.term
        crowd.platform = self.platform
        crowd.school = self.school
        crowd.teacher = self.teacher
        crowd.start_date = self.start_date
        crowd.end_date = self.end_date
        crowd.update_time = self.save_time
        crowd.status = self.status
        crowd.crowd = self.crowd
        crowd.total_crowd = self.total_crowd
        crowd.term_id = self.term_id
        crowd.url = self.url
        crowd.crowd_num = self.crowd_num
        crowd.total_crowd_num = self.total_crowd_num
        crowd.term_number = self.term_number
        crowd.block = self.block
        return crowd

    def list2group(self) -> CourseGroupInfo:
        course_group = CourseGroupInfo()
        course_group.course_group_id = self.course_group_id
        course_group.school = self.school
        course_group.teacher = self.teacher
        course_group.term_count = 1 if self.term_id == 0 else self.term_id
        course_group.platform = self.platform
        course_group.course_name = self.course_name
        course_group.status = 0 if self.status == 0 else 1
        return course_group
