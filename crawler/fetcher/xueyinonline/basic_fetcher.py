import uuid
import logging
import json
import datetime

import utils

import selenium.common.exceptions

from typing import List, Tuple
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.remote.webdriver import WebDriver, WebElement

from persistence.model.forum import ForumPostInfo
from .. import BaseFetcher


class BasicFetcher(BaseFetcher):
    def __init__(self, driver: WebDriver):
        super().__init__(driver)
        self.course_info = dict()

    def run(self) -> dict:
        res = dict()
        self.__get_course_info()
        res['semester_result_info'] = self.course_info
        return res

    def __get_course_info(self):
        self.course_info['semester_start_date'], self.course_info['semester_end_date'] = self.__get_start_end_date()
        self.course_info['semester_teacherteam_info'] = self.__get_teacher_team_info()
        self.course_info['semester_studentnum'] = self.__get_student_num()
        self.course_info['semester_resource_info'] = ""
        self.course_info['semester_homework_info'] = ""
        self.course_info['semester_interact_info'] = ""
        self.course_info['semester_exam_info'] = ""
        self.course_info['semester_test_info'] = ""
        self.course_info['semester_extension_info'] = ""
        self.course_info['update_time'] = datetime.datetime.now()

    def __get_start_end_date(self) -> Tuple[datetime.datetime, datetime.datetime]:
        try:
            element = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_elements_by_class_name("mgCard_deta_er")[0].find_elements_by_class_name('fl')[-1])
        except selenium.common.exceptions.TimeoutException:
            logging.error("[__get_start_end_date] cannot load date text elem, returning 1999-1-1")
            return datetime.datetime(1999, 1, 1), datetime.datetime(1999, 1, 1)

        date_text = element.text
        assert isinstance(date_text, str)
        date_text = date_text[date_text.find('：') + 1:]
        cut_pos = date_text.find('至')
        start_text = date_text[: cut_pos].strip()
        end_text = date_text[cut_pos + 1:].strip()

        logging.info("[__get_start_end_date] date: %s -> %s", start_text, end_text)
        return utils.parse_time(start_text), utils.parse_time(end_text)

    def __get_teacher_team_info(self) -> str:
        data = dict()
        data['course_director_name'] = ""
        data['teacher_team'] = list()

        try:
            div_teacher_list = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_elements_by_class_name('kc_teach')[0].find_elements_by_tag_name('dt'))
        except selenium.common.exceptions.TimeoutException:
            logging.error("[__get_teacher_team_info] cannot load teachers info. return empty")
            return json.dumps(data)

        for div_teacher in div_teacher_list:
            text = div_teacher.text
            assert isinstance(text, str)
            data['teacher_team'].append(text[: text.find(' ')])
        data['course_director_name'] = data['teacher_team'][0]

        return json.dumps(data)

    def __get_student_num(self) -> int:
        try:
            text = self.driver.find_element_by_id('_chooseCourseCount').text
            assert isinstance(text, str)
        except selenium.common.exceptions.TimeoutException:
            logging.error("[__get_student_num] cannot load student num elem. return 0.")
            return 0

        try:
            res = int(text.strip())
        except ValueError:
            logging.error("[__get_student_num] student num text is not a num: [%s]", text)
            return 0

        return res
