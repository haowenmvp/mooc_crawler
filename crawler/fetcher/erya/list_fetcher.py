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
        counter = 0
        for type_no in range(13, 19):
            is_last_page = 'False'
            page_no = 0
            while is_last_page == 'False':
                page_no = page_no + 1
                url = 'http://erya.mooc.chaoxing.com/api/getResByManyType'
                headers = {
                    'Accept': 'application/json, text/javascript, */*; q=0.01',
                    'Accept-Encoding': 'gzip, deflate',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Cache-Control': 'max-age=0',
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Host': 'erya.mooc.chaoxing.com',
                    'X-Requested-With': 'XMLHttpRequest',
                    'User-Agent': fake_ua.random
                }
                params = {
                    'oneType': str(type_no),
                    'twoType': '-1',
                    'schoolType': '-1',
                    'pageNo': str(page_no)
                }
                resp = requests.get(url, headers=headers, params=params, timeout=10)
                json_item = resp.json()
                data_json = json_item['data']
                list_json = data_json['list']
                for course_item in list_json:
                    counter = counter + 1
                    course_list_info = CourseListInfo()
                    course_list_info.save_time = datetime.now()
                    course_list_info.platform = "北京超星尔雅教育科技有限公司"
                    course_list_info.status = course_list_info.kStatusOn
                    course_list_info.term_number = 1
                    course_list_info.term_id = 1
                    if type_no == 13:
                        course_list_info.block = "综合素养"
                    elif type_no == 14:
                        course_list_info.block = "通用能力"
                    elif type_no == 15:
                        course_list_info.block = "成长基础"
                    elif type_no == 16:
                        course_list_info.block = "公共必修"
                    elif type_no == 17:
                        course_list_info.block = "创新创业"
                    elif type_no == 18:
                        course_list_info.block = "考研辅导"
                    if "author" in course_item:
                        course_list_info.teacher = str(course_item["author"])
                    if "createTime" in course_item:
                        timeStamp = int(course_item["createTime"])
                        course_list_info.start_date = datetime.fromtimestamp(timeStamp / 1000.0)
                    if "flow" in course_item:
                        course_list_info.clicked = str(course_item["flow"])
                    if "general" in course_item:
                        course_list_info.introduction = str(course_item["general"])
                    if "school" in course_item:
                        course_list_info.school = str(course_item["school"])
                    if "title" in course_item:
                        course_list_info.course_name = str(course_item["title"])
                    if "ztId" in course_item:
                        cid = str(course_item["ztId"])
                        course_url = 'https://mooc1.chaoxing.com/course/' + cid + '.html'
                        course_list_info.url = course_url
                        score_url = 'https://mooc1.chaoxing.com/CourseController/getCourseStarTag'
                        headers = {
                            'Accept': 'text/html, */*; q=0.01',
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'Host': 'mooc1.chaoxing.com',
                            'Sec-Fetch-Mode': 'cors',
                            'Sec-Fetch-Site': 'same-origin',
                            'X-Requested-With': 'XMLHttpRequest',
                            'User-Agent': fake_ua.random
                        }
                        params = {
                            'courseId': cid,
                            'edit': 'false'
                        }
                        score_resp = requests.get(score_url, headers=headers, params=params, timeout=10)
                        score_html = score_resp.text
                        score_soup = BeautifulSoup(str(score_html), "html.parser")
                        if len(score_soup.find_all("span", attrs={"class": "zev_score"})):
                            score_text = score_soup.find("span", attrs={"class": "zev_score"})
                            s_soup = BeautifulSoup(str(score_text), "html.parser")
                            course_list_info.course_score = str(s_soup.get_text())
                        headers = {
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                            'Cache-Control': 'max-age=0',
                            'Host': 'mooc1.chaoxing.com',
                            'Sec-Fetch-Mode': 'navigate',
                            'Sec-Fetch-Site': 'cross-site',
                            'Sec-Fetch-User': '?1',
                            'User-Agent': fake_ua.random
                        }
                        course_resp = requests.get(course_url, headers=headers, params=params, timeout=10)
                        html = course_resp.text
                        whole_soup = BeautifulSoup(str(html), "html.parser")
                        if len(whole_soup.find_all("strong", attrs={"class": "mr10 f24"})):
                            course_list_info.team = self.get_page_detail(whole_soup)
                        else:
                            course_list_info.team = course_list_info.teacher
                    print(course_list_info.__dict__)
                    course_list.append(course_list_info.__dict__)
                is_last_page = str(data_json['isLastPage'])
        return course_list

    def get_page_detail(self, whole_soup):
        team = ""
        team_item = whole_soup.find_all("strong", attrs={"class": "mr10 f24"})
        for n in range(0, len(team_item)):
            team = team + "，" + team_item[n].string.strip()
        if "，" in team:
            team = team.split("，", 1)[1]
        return team
