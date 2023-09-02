import requests
import logging
import time
import uuid
import re
from persistence.model.basic_info import CourseListInfo
from ..base_fetcher import BaseFetcher
from selenium.webdriver.support.wait import WebDriverWait
from datetime import datetime
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import selenium.common.exceptions


class ListFetcher(BaseFetcher):

    def run(self):
        error_list = []
        course_list = self.get_course_info()
        all_list = {
            'course_list_info': course_list,
            'error_list': error_list
        }
        return all_list

    def get_course_info(self):
        course_list = []
        fake_ua = UserAgent()
        page_num = 0
        while 1:
            page_num = page_num + 1
            url = 'https://imooc.gaoxiaobang.com/class/api'
            headers = {
                'Accept': 'application/json',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
                'Content-Type': 'application/json',
                'Host': 'imooc.gaoxiaobang.com',
                'Origin': 'http://www.ehuixue.cn',
                'User-Agent': fake_ua.random,
                'Referer': 'https://imooc.gaoxiaobang.com/',
                'X-Requested-With': 'XMLHttpRequest'
            }
            params = {
                'curPage': str(page_num),
                'pageSize': '50'
            }
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            resp_json = resp.json()
            data_json = resp_json['dataList']
            for json_item in data_json:
                course_list_info = CourseListInfo()
                course_list_info.course_name = str(json_item['className'])
                class_id = str(json_item['classId'])
                course_list_info.url = 'https://imooc.gaoxiaobang.com/#/courses/detail/' + class_id
                course_list_info.platform = "北京高校邦科技有限公司"
                course_list_info.term_id = 1
                course_list_info.term_number = 1
                if 'instructors' in json_item:
                    instructors = json_item['instructors']
                    if len(instructors):
                        course_list_info.teacher = str(instructors[0]['name'])
                        team = ''
                        for instructor in instructors:
                            team = team + "，" + str(instructor['name'])
                        if "，" in team:
                            course_list_info.team = team.split("，", 1)[1]
                if 'intro' in json_item:
                    ts = json_item['intro']
                    ts_soup = BeautifulSoup(str(ts), "html.parser")
                    ts_b = ts_soup.find_all('b')
                    if len(ts_b):
                        ts_text = str(ts_b[0].string)
                        if '/' in ts_text:
                            school_text = str(ts_text.split('/', 1)[1])
                            if "大学" or "学院" or "学校" in school_text:
                                course_list_info.school = school_text
                if 'startAt' in json_item:
                    timeStamp = int(json_item['startAt'])
                    course_list_info.start_date = datetime.fromtimestamp(timeStamp/1000.0)
                if 'concludeAt' in json_item:
                    timeStamp = int(json_item['concludeAt'])
                    course_list_info.end_date = datetime.fromtimestamp(timeStamp/1000.0)
                course_list_info.save_time = datetime.now()
                if course_list_info.start_date != datetime(1999, 1, 1) and course_list_info.end_date != datetime(1999, 1, 1):
                    start = course_list_info.start_date
                    end = course_list_info.end_date
                    if start < datetime.now() < end:
                        course_list_info.status = course_list_info.kStatusOn
                    elif datetime.now() < start:
                        course_list_info.status = course_list_info.kStatusLaterOn
                    elif end < datetime.now():
                        course_list_info.status = course_list_info.kStatusEnd
                if 'userNum' in json_item:
                    course_list_info.total_crowd = str(json_item['userNum'])
                    course_list_info.total_crowd_num = int(json_item['userNum'])
                if 'avgScore' in json_item:
                    course_list_info.course_score = str(json_item['avgScore'])
                if 'courseIntro' in json_item:
                    intro = json_item['courseIntro']
                    intro_soup = BeautifulSoup(str(intro), "html.parser")
                    course_list_info.introduction = intro_soup.get_text()
                print(course_list_info.__dict__)
                course_list.append(course_list_info.__dict__)
            paging_json = resp_json['paging']
            total_num = int(paging_json['total'])
            end_row = int(paging_json['endRow'])
            if end_row == total_num:
                break
        return course_list
