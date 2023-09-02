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
        course_num = 0
        while 1:
            page_num = page_num + 1
            url = 'https://www.livedu.com.cn/ispace4.0/moocxjkc/queryAllKc.do'
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Cache-Control': 'max-age=0',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Host': 'www.livedu.com.cn',
                'Origin': 'http://www.livedu.com.cn',
                'User-Agent': fake_ua.random
            }
            data = {
                'pageNo': str(page_num),
                'pageSize': '50',
                'pageCount': str(page_num)
            }
            resp = requests.post(url, headers=headers, data=data, timeout=10)
            html = resp.text
            whole_soup = BeautifulSoup(html, "html.parser")
            content = whole_soup.find("ul", attrs={"class": "list-b-list"})
            content_soup = BeautifulSoup(str(content), "html.parser")
            course_item = content_soup.find_all("li")
            for i in range(0, len(course_item)):
                course_num = course_num + 1
                course_list_info = CourseListInfo()
                course_item_soup = BeautifulSoup(str(course_item[i]), "html.parser")
                js_click = str(course_item_soup.a['onclick'])
                js_split = js_click.split("'", 2)
                href = js_split[1]
                course_url = "https://www.livedu.com.cn" + href
                course_list_info.url = course_url
                b2 = course_item_soup.find("div", attrs={"class": "list-b-two fl"})
                b3 = course_item_soup.find("div", attrs={"class": "list-b-three fl"})
                b2_soup = BeautifulSoup(str(b2), "html.parser")
                b3_soup = BeautifulSoup(str(b3), "html.parser")
                course_name = b3_soup.find("h3")
                course_name_soup = BeautifulSoup(str(course_name), "html.parser")
                course_list_info.course_name = course_name_soup.get_text().strip()
                course_list_info.teacher = str(b2_soup.find("h5").string)
                course_list_info.school = str(b2_soup.find("h3").string)
                if len(b3_soup.find_all("img", attrs={"title": "获得国家精品在线开放课程认定的课程"})):
                    course_list_info.isquality = course_list_info.kIsQuality
                details = b3_soup.find_all("dd")
                crowd_d = BeautifulSoup(str(details[0]), "html.parser")
                click_d = BeautifulSoup(str(details[1]), "html.parser")
                crowd = crowd_d.get_text()
                course_list_info.crowd = crowd.split("人", 1)[0]
                course_list_info.crowd_num = int(course_list_info.crowd)
                clicked = click_d.get_text()
                course_list_info.clicked = clicked.split("点", 1)[0]
                course_list_info.platform = "北京高校优质课程研究会"
                course_list_info.term_id = 1
                course_list_info.term_number = 1
                course_list_info.save_time = datetime.now()
                course_list_info.status = course_list_info.kStatusOn
                detail_resp = requests.get(course_url, headers=headers, timeout=10)
                detail_html = detail_resp.text
                detail_soup = BeautifulSoup(str(detail_html), "html.parser")
                if len(detail_soup.find_all("div", attrs={"class": "vice-main-txt"})):
                    intro, team = self.get_page_1st_detail(detail_soup)
                    course_list_info.introduction = intro
                    course_list_info.team = team
                elif len(detail_soup.find_all("div", attrs={"class": "details-b-text"})):
                    intro, team = self.get_page_2nd_detail(detail_soup)
                    course_list_info.introduction = intro
                    course_list_info.team = team
                print(course_list_info.__dict__)
                course_list.append(course_list_info.__dict__)
            if len(whole_soup.find_all("i", attrs={"class": "fa fa-angle-double-right"})) == 0:
                break
        return course_list

    def get_page_1st_detail(self, whole_soup):
        if len(whole_soup.find_all("div", attrs={"class": "vice-main-txt"})):
            intro_info = whole_soup.find("div", attrs={"class": "vice-main-txt"})
            intro_soup = BeautifulSoup(str(intro_info), "html.parser")
            intro = intro_soup.get_text()
        else:
            intro = ""
        team = ""
        team_item = whole_soup.find_all("div", attrs={"class": "vice-teacher-title"})
        for n in range(0, len(team_item)):
            item_soup = BeautifulSoup(str(team_item[n]), "html.parser")
            teacher = item_soup.find("label").string.strip()
            team = team + "，" + teacher
        if "，" in team:
            team = team.split("，", 1)[1]
        return intro, team

    def get_page_2nd_detail(self, whole_soup):
        if len(whole_soup.find_all("div", attrs={"class": "details-b-text"})):
            intro_info = whole_soup.find("div", attrs={"class": "details-b-text"})
            intro_soup = BeautifulSoup(str(intro_info), "html.parser")
            intro = intro_soup.get_text()
        else:
            intro = ""
        team = ""
        team_item = whole_soup.find_all("div", attrs={"class": "details-d-tite"})
        for n in range(0, len(team_item)):
            item_soup = BeautifulSoup(str(team_item[n]), "html.parser")
            teacher = item_soup.find("h3").string.strip()
            team = team + "，" + teacher
        if "，" in team:
            team = team.split("，", 1)[1]
        return intro, team
