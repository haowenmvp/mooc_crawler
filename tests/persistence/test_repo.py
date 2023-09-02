from datetime import datetime
import uuid
from persistence.db.forum_basic_info_repo import ForumBasicInfoRepository
from persistence.db.forum_post_info_repo import ForumPostInfoRepository
from persistence.db.course_basic_info_repo import CourseBasicInfoRepository
from persistence.db.semester_result_info_repo import SemesterResultInfoRepository
from persistence.db.teacher_info_repo import TeacherInfoRepository
from persistence.db.semester_basic_info_repo import SemesterBasicInfoRepository
from persistence.db.school_info_repo import SchoolInfoRepository
from persistence.db.school_course_statistic_repo import SchoolCourseStatisticRepository
from persistence.db.resource_structure_info_repo import ResourceStructureInfoRepository
from persistence.db.resource_info_repo import ResourceInfoRepository
from persistence.db.platform_course_statistic_repo import PlatformCourseStatisticRepository
from persistence.db.platform_info_repo import PlatformInfoRepository
from persistence.db.task_info_repo import TaskInfoRepository
from persistence.model.basic_info import TeacherInfo
from persistence.model.basic_info import SchoolCourseStatistic
from persistence.model.basic_info import SchoolInfo
from persistence.model.course_info import CourseBasicInfo
from persistence.model.course_info import SemesterResultInfo
from persistence.model.course_info import SemesterBasicInfo
from persistence.model.resource import ResourceStructureInfo
from persistence.model.resource import ResourceInfo
from persistence.model.forum import ForumBasicInfo
from persistence.model.basic_info import PlatformCourseStatistic
from persistence.model.basic_info import PlatformInfo
from persistence.model.task import Task
from persistence.model.forum import ForumPostInfo
from persistence.model.task import ScheduleTask
from persistence.db.schedule_task_repo import ScheduleTaskInfoRepository
from persistence.db.course_crowd_weekly_repo import CourseCrowdWeeklyRepo
from persistence.db.course_list_info_repo import CourseListInfoRepository
from persistence.model.basic_info import CourseListInfo
from typing import List
from persistence.model.basic_info import CourseCrowdWeekly


def test_crowd_repo():
    conn1 = CourseListInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                     database='mooc_test')
    course_list = conn1.get_all_list()
    conn2 = CourseCrowdWeeklyRepo(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                  database='mooc_test')
    crowd_list = []
    for course in course_list:
        crowd = course.list2crowd()
        print(crowd.__dict__)
        crowd_list.append(crowd)
    conn2.create_course_crowd_weeklys(crowd_list)


def test_schedule_task_info():
    conn = ScheduleTaskInfoRepository(host='192.168.232.254', port=3307, username='root', password='123qweASD!@#',
                                      database='mooc_test')
    # test = ScheduleTask()
    # test.task_id = '111'
    # test.course_id = '111111111'
    # test.url = 'https://next.xuetangx.com'
    # test.login_info = None
    # conn.create_schedule_task_info(test)

    # print(conn.get_id_by_taskid_crawltime(17,'2020-02-13 00:00:00'))
    print(conn.update_task_start(111184))
    # conn.update_after_task_finish(test.task_id)
    # print(conn.get_schedule_task_info_by_task_id(test.task_id).__dict__)
    # conn.update_task_status(test.task_id)


def test_task_info():
    conn = TaskInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                              database='mooc_test')
    test = Task()
    test.id = 111
    test.url = 'http://www.icourse163.org/course/JNU-1207459804'
    test.platform = '大学mooc'
    conn.create_task_info(test)
    test.school = 'HUST'
    conn.update_info_after_finish(test.id)
    print(conn.get_task_info_by_id(test.id).__dict__)
    conn.delete_tasks_by_url(test.url)
    print(conn.get_tasknumber_info_by_url(test.url))
    print(conn.set_task_update_time_by_url(test.url))


def test_platform_info():
    conn = PlatformInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                  database='mooc_test')
    test = PlatformInfo()
    test.platform_id = 11
    test.platform_mainpage = 'https://icourse163.org'
    conn.create_platform_info(test)
    test.platform_name = '大学mooc'
    conn.update_platform_info(test)


def test_platform_course_stastic_info():
    conn = PlatformCourseStatisticRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                             database='mooc_test')
    test = PlatformCourseStatistic()
    test.platform_id = 11
    test.platform_onlinecourse_num = 10
    conn.create_platform_course_statistic(test)
    test.platform_total_course_num = 100
    conn.update_platform_course_statistic(test)


def test_resource_structure_info():
    conn = ResourceStructureInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                           database='mooc_test')
    test = ResourceStructureInfo()
    test.resource_structure_id = str(uuid.uuid1())
    test.resource_chapter_index = 10
    conn.create_resource_structure_info(test)
    test.resource_knobble_index = 100
    conn.update_resource_structure_info(test)


def test_resource_info():
    conn = ResourceInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                  database='mooc_test')
    test = ResourceInfo()
    test.resource_id = str(uuid.uuid1())
    test.resource_name = 'test_resource'
    conn.create_resource_info(test)
    test.resource_type = 1
    conn.update_resource_info(test)


def test_school_course_stastic_info():
    conn = SchoolCourseStatisticRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                           database='mooc_test')
    test = SchoolCourseStatistic()
    test.school_id = str(uuid.uuid1())
    test.school_qualifiedcourse_num = 100
    conn.create_school_course_statistic(test)
    test.school_onlinecourse_num = 1
    conn.update_school_course_statistic(test)


def test_school_info():
    conn = SchoolInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                database='mooc_test')
    test = SchoolInfo()
    test.school_id = str(uuid.uuid1())
    test.school_name = 'HUST'
    conn.create_school_info(test)
    test.school_type = 1
    conn.update_school_info(test)


def test_teacher_info():
    conn = TeacherInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                 database='mooc_test')
    test = TeacherInfo()
    test.teacher_id = str(uuid.uuid1())
    test.teacher_school = 'HUST'
    conn.create_teacher_info(test)
    test.teacher_department = 'EIC'
    conn.update_teacher_info(test)
    print(conn.get_teacher_id_by_school_teacher('HUST', 'sdn'))


def test_semester_basic_info():
    conn = SemesterBasicInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                       database='mooc_test')
    test = SemesterBasicInfo()
    test.semester_id = str(uuid.uuid1())
    test.course_group_id = str(uuid.uuid1())
    conn.create_semester_basic_info(test)
    test.semester_platform = '大学mooc'
    conn.update_semester_basic_info(test)


def test_semester_result_info():
    conn = SemesterResultInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                        database='mooc_test')
    test = SemesterResultInfo()
    test.semester_id = str(uuid.uuid1())
    test.semester_test_info = "test_info"
    conn.create_semester_result_info(test)
    test.semester_studentnum = 100000
    conn.update_semester_result_info(test)


def test_forum_basic_info():
    conn = ForumBasicInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                    database='mooc_test')
    test = ForumBasicInfo()
    test.forum_id = str(uuid.uuid1())
    test.forum_plate = "讨论区"
    conn.create_forum_basic_info(test)
    test.semester_id = str(uuid.uuid1())
    conn.update_forum_basic_info(test)
    print(conn.get_forum_id_by_semester_id_and_name(test.semester_id, test.forum_plate))


def test_forum_post_info():
    conn = ForumPostInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                   database='mooc_test')
    test = ForumPostInfo()
    test.forum_post_id = str(uuid.uuid1())
    test.forum_id = str(uuid.uuid1())
    test.forum_post_type = ForumPostInfo.TopicTypeEnum.kTypeUnknown
    test.forum_reply_id = str(uuid.uuid1())
    test.forum_reply_userid = str(uuid.uuid1())
    test.forum_post_username = ''
    test.forum_post_userrole = ForumPostInfo.OwnerRoleTypeEnum.kTypeUnknown
    test.forum_post_content = ''
    test.forum_post_time = datetime(1999, 1, 1)
    test.update_time = datetime(1999, 1, 1)
    test1 = ForumPostInfo()
    test1.forum_post_id = str(uuid.uuid1())
    conn.create_forum_post_info(test)
    conn.create_forum_post_info(test1)
    # conn.create_forum_post_infos([test,test1])
    test1.forum_post_time = datetime(2019, 1, 1)
    conn.update_forum_post_info(test1)


def test_course_basic_info():
    conn = CourseBasicInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                     database='mooc_test')
    course_test = CourseBasicInfo()
    course_test.course_group_id = str(uuid.uuid1())
    conn.create_course_basic_info(course_test)
    course_test.course_name = 'test'
    conn.update_course_basic_info(course_test)


def test_course_list_info_repo():
    conn1 = CourseListInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                     database='mooc_test')
    return conn1.get_all_list()
    # course_list = conn1.get_course_by_course_id('133675')
    # course_list += conn1.get_course_by_course_id('133676')
    # return course_list


def process_course_list_info(data: List[dict]):
    course_list_info_repo = CourseListInfoRepository(host='222.20.95.42', port=3307, username='root',
                                                     password='123qweASD!@#',
                                                     database='mooc_test')
    course_crowd_repo = CourseCrowdWeeklyRepo(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                              database='mooc_test')
    for course_info in data:
        course_list_info = CourseListInfo()
        course_list_info.url = course_info['url']
        course_list_info.course_name = course_info['course_name']
        course_list_info.course_id = 0
        course_list_info.school = course_info['school']
        course_list_info.teacher = course_info['teacher']
        course_list_info.team = course_info['team']
        course_list_info.term = course_info['term']
        course_list_info.term_id = course_info['term_id']
        course_list_info.platform = course_info['platform']
        course_list_info.status = course_info['status']
        course_list_info.start_date = course_info['start_date']
        course_list_info.end_date = course_info['end_date']
        course_list_info.crowd = course_info['crowd']
        course_list_info.total_crowd = course_info['total_crowd']
        course_list_info.clicked = course_info['clicked']
        course_list_info.save_time = course_info['save_time']
        course_list_info.extra = course_info['extra']
        course_list_info.isquality = course_info['isquality']
        course_list_info.course_score = course_info['course_score']
        course_list_info.introduction = course_info['introduction']
        course_list_info.subject = course_info['subject']
        course_list_info.course_target = course_info['course_target']
        course_list_info.scoring_standard = course_info['scoring_standard']
        course_list_info.isfree = course_info['isfree']
        course_list_info.certification = course_info['certification']
        course_list_info.ispublic = course_info['ispublic']
        if course_list_info_repo.get_if_course_exsited(course_list_info):
            course_list_info_repo.update_list_info(course_list_info)
        else:
            course_list_info_repo.create_course_list_info(course_list_info)
        course_list_info.course_id = course_list_info_repo.get_courseid_by_courseinfo(course_list_info)
        crowd = course_list_info.list2crowd()
        if not course_crowd_repo.is_crowd_exsits(crowd):
            course_crowd_repo.create_course_crowd_weekly(crowd)

def test_schedule_repo():
    conn1 = ScheduleTaskInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                       database='mooc_test')
    conn2 = TaskInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                               database='mooc_test')

    task_id = 58316
    task_info = conn2.get_task_info_by_id(task_id)
    # print(task_info.__dict__)
    # print(task_info.crawled_next_time)
    scheduletask = conn1.get_scheduletask_by_taskid_crawltime(task_id,
                                                              task_info.crawled_next_time)
    #
    # print(scheduletask)
    # print("---------------------------------schedule_id:%s-----------------------------------", scheduletask.id)
    if scheduletask:
        conn1.update_task_status(110856, scheduletask.kStatusInProcess)

test_schedule_task_info()