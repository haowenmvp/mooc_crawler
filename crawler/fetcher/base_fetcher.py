import abc
from selenium.webdriver.remote.webdriver import WebDriver
from persistence.model.basic_info import CourseListInfo
from typing import List


class BaseFetcher:
    kWaitSecond = 10

    def __init__(self, driver=None):
        self.driver = driver

    @abc.abstractmethod
    def run(self, data=None, login_info=None) -> dict:
        pass

    def run_by_url_forum(self, course_url: str, login_info: dict) -> dict:
        pass

    @staticmethod
    def distinct(course_info_list: List[dict]):
        res = []
        for course_info in course_info_list:
            course_list_info = CourseListInfo()
            course_list_info.__dict__.update(course_info)
            res.append(course_list_info)
        new_course_list = []

        for course in list(set(res)):
            new_course_list.append(course.__dict__)
        return new_course_list
