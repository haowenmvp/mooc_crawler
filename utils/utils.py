import logging
import datetime
import importlib

from typing import Type
from persistence.db.course_crowd_weekly_repo import CourseCrowdWeeklyRepo
from persistence.db.course_list_info_repo import CourseListInfoRepository
from persistence.model.basic_info import CourseListInfo
from typing import List
from persistence.db.error_repo import ErrorRepository

kTimeFormats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%m-%d',
                '%Y年%m月%d日 %H:%M:%S', '%Y年%m月%d日', '%m月%d日',
                '%H:%M']


def parse_time(time_str: str) -> datetime.datetime:
    time_str = time_str.strip()

    if time_str.endswith('前'):
        time_str = time_str[: -1]
        time = datetime.datetime.now()
        if time_str.endswith('天'):
            time_str = time_str[: -1]
            time += datetime.timedelta(days=-int(time_str))
        elif time_str.endswith('小时'):
            time_str = time_str[: -2]
            time += datetime.timedelta(hours=-int(time_str))
        elif time_str.endswith('分钟'):
            time_str = time_str[: -2]
            time += datetime.timedelta(minutes=-int(time_str))
        elif time_str.endswith('秒'):
            time_str = time_str[: -1]
            time += datetime.timedelta(seconds=-int(time_str))
        else:
            raise ValueError("unknown relative time format: %s", time_str)
        return time

    for time_format in kTimeFormats:
        res = datetime.datetime(1999, 1, 1)
        try:
            res = datetime.datetime.strptime(time_str, time_format)
        except ValueError:
            continue

        year = res.year
        month = res.month
        day = res.day
        if "%Y" not in time_format:
            year = datetime.datetime.now().year
        if "%m" not in time_format:
            month = datetime.datetime.now().month
        if "%d" not in time_format:
            day = datetime.datetime.now().day

        res = datetime.datetime(year=year,
                                month=month,
                                day=day,
                                hour=res.hour,
                                minute=res.minute,
                                second=res.second)
        return res
    raise ValueError("unknown time format: %s", time_str)


def load_class_type(class_path: str) -> Type[object]:
    cut_pos = class_path.rfind('.')
    module_name = class_path[: cut_pos]
    class_name = class_path[cut_pos + 1:]
    try:
        target_module = importlib.import_module(module_name)
        target_class = getattr(target_module, class_name)
    except ModuleNotFoundError as e:
        logging.warning("[utils.load_class_type] module [%s] not found", module_name)
        raise e
    except AttributeError as e:
        logging.warning("[utils.load_class_type] class [%s] is not found in module [%s]", class_name, module_name)
        raise e

    return target_class


def process_course_list_info(data: dict):
    course_list_info_repo = CourseListInfoRepository(host='192.168.232.254', port=3307, username='root',
                                                     password='123qweASD!@#',
                                                     database='mooc_test')
    course_crowd_repo = CourseCrowdWeeklyRepo(host='192.168.232.254', port=3307, username='root',
                                              password='123qweASD!@#',
                                              database='mooc_test')
    error_repo = ErrorRepository(host='192.168.232.254', port=3307, username='root',
                                 password='123qweASD!@#',
                                 database='mooc_test')
    for course_info in data["course_list_info"]:
        course_list_info = CourseListInfo()
        course_list_info.__dict__.update(course_info)
        course_list_info.course_id = 0
        save_time = course_list_info.save_time
        if course_list_info.platform == '国家开放大学出版社有限公司（荟学习网）':
            course_id = course_list_info_repo.get_courseid_by_part_courseinfo(course_list_info)
            if course_id:
                course_list_info_repo.update_list_info_by_courseinfo(course_list_info)
                course_list_info.course_id = course_id
            else:
                course_list_info_repo.create_course_list_info(course_list_info)
                course_list_info.course_id = course_list_info_repo.get_courseid_by_part_courseinfo(course_list_info)
        # elif course_list_info.platform == '中科云教育':
        elif course_list_info.platform == '华文慕课' or course_list_info.platform == '优课联盟':
            course_id = course_list_info_repo.get_courseid_by_all_info(course_list_info)
            if course_id:
                course_list_info_repo.update_list_info_by_all_info(course_list_info)
                course_list_info.course_id = course_id
            else:
                course_list_info_repo.create_course_list_info(course_list_info)
                course_list_info.course_id = course_list_info_repo.get_courseid_by_all_info(course_list_info)
        else:
            course_id = course_list_info_repo.get_courseid_by_url(course_list_info.url)
            if course_id:
                course_list_info_repo.update_list_info_by_url(course_list_info)
                course_list_info.course_id = course_id
            else:
                course_list_info_repo.create_course_list_info(course_list_info)
                course_list_info.course_id = course_list_info_repo.get_courseid_by_url(course_list_info.url)
        course_list_info.save_time = save_time
        crowd = course_list_info.list2crowd()
        crowd_id = course_crowd_repo.is_crowd_exsits(crowd)
        if not crowd_id:
            course_crowd_repo.create_course_crowd_weekly(crowd)
        else:
            course_crowd_repo.update_course_crowd_weekly(crowd, crowd_id)
    error_list = data["error_list"]
    if error_list:
        error_repo.create_error_list_infos(error_list)
