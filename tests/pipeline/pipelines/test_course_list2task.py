from persistence.model.task import Task
from persistence.model.basic_info import CourseListInfo
from persistence.db.course_list_info_repo import CourseListInfoRepository
from persistence.db.task_info_repo import TaskInfoRepository
from typing import List

config = {
    "host": "222.20.95.42",
    "port": 3307,
    "username": "root",
    "password": "123qweASD!@#",
    "database": "mooc_test"
}



def process_course_list2task(data: List[dict]):
    # 1. 通过url判断更新的courselist课程是否存在task表中（查询结果是否等于fetcher数目）
    # 2. 等于则不添加，不等于则清空再添加
    # 3. schedule_task()
    FETCHER_NUM = 3
    task_info_repo = TaskInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                        database='mooc_test')
    for course_info in data:
        url = course_info['url']
        count = task_info_repo.get_tasknumber_info_by_url(url)
        if count != 0:
            task_info_repo.delete_tasks_by_url(url)
            continue
        else:
            # task_basic = Task()
            # task_video = Task()
            task_forum = Task()
            task_forum.id = 0
            task_forum.course_id = course_info['course_id']
            task_forum.url = course_info['url']
            task_forum.crawled_time_gap = 600000
            task_forum.course_name = course_info['course_name']
            task_forum.term = course_info['term']
            task_forum.platform = course_info['platform']
            task_forum.school = course_info['school']
            task_forum.teacher = course_info['teacher']
            task_forum.fetcher_type = 'forum'
            task_forum.crawled_plan_num = 1
            task_info_repo.delete_tasks_by_url(url)
            task_info_repo.create_task_info(task_forum)

    pass


def icourse_process_course_list2task(data: List[dict]):
    # 1. 通过url判断更新的courselist课程是否存在task表中（查询结果是否等于fetcher数目）
    # 2. 等于则不添加，不等于则清空再添加
    # 3. schedule_task()
    task_info_repo = TaskInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                        database='mooc_test')
    FETCHER_NUM = 2
    for course_info in data:
        url = course_info['url']
        count = task_info_repo.get_tasknumber_info_by_url(url)
        if count == FETCHER_NUM:
            task_info_repo.set_task_update_time_by_url(url)
            continue
        elif course_info['status'] > 1:
            continue
        else:
            task_basic = Task()
            task_video = Task()
            task_forum = Task()
            task_basic.id = 0
            task_basic.course_id = course_info['course_id']
            task_basic.url = course_info['url']
            task_basic.course_name = course_info['course_name']
            task_basic.term = course_info['term']
            task_basic.platform = course_info['platform']
            task_basic.school = course_info['school']
            task_basic.teacher = course_info['teacher']
            task_basic.fetcher_type = 'basic'
            task_basic.crawled_plan_num = 1
            task_basic.crawled_finished_num = 0
            task_forum.crawled_time_gap = 600000
            task_forum = task_basic
            task_video = task_basic
            task_info_repo.delete_tasks_by_url(url)
            # self.task_info_repo.create_task_info(task_basic)
            task_forum.fetcher_type = 'forum'
            task_info_repo.create_task_info(task_forum)
            task_video.fetcher_type = 'video'
            task_info_repo.create_task_info(task_video)


def test_process_course_list2task(data: List[dict]):
    # 1. 通过url判断更新的courselist课程是否存在task表中（查询结果是否等于fetcher数目）
    # 2. 等于则不添加，不等于则清空再添加
    # 3. schedule_task()
    FETCHER_NUM = 3
    task_info_repo = TaskInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                        database='mooc_test')
    for course_info in data:
        url = course_info['url']
        course_id =course_info['course_id']
        if task_info_repo.get_if_task_by_course_id(course_id):
            task_info_repo.update_task_by_course_id(course_id)
        else:
            pass

repo = CourseListInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
                                database='mooc_test')
course0 = repo.get_course_list_by_platform_status('爱课程(中国大学MOOC)', 0)
course1 = repo.get_course_list_by_platform_status('爱课程(中国大学MOOC)', 1)
courses = course0 + course1
print(courses[0])
print(len(courses))
icourse_process_course_list2task(courses)



# task1 = CourseListInfo()
# task1.url = 'test'
# task2 = CourseListInfo()
# task2.url = 'test2'
# task3 = CourseListInfo()
# task3.url = 'test3'
# task1.course_id = str(uuid.uuid1())
# task2.course_id = str(uuid.uuid1())
# task3.course_id = str(uuid.uuid1())
# task_list = [task1,task2,task3]
# repo.create_course_list_infos(task_list)
