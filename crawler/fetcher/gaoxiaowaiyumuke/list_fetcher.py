import pickle
import re
import uuid
from typing import List

import selenium
from requests_html import HTMLSession, HTML
from retrying import retry
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.remote.webdriver import WebDriver
from crawler.fetcher import BaseFetcher
from persistence.model.basic_info import CourseListInfo
from datetime import datetime
import time


class ListFetcher(BaseFetcher):
    def __init__(self):
        super().__init__()
        self.Cookie = "Hm_lvt_8a1d0cf914523c7ed112dbd25e018957=1581750256,1581762434; " \
                      "Hm_lvt_98aead8c22e727e602de6fc4d2508c8d=1581750257,1581762434; " \
                      "acw_tc=65c86a0915817502549618088e36602d386732bb6817cffa566e45f4a79a4f; " \
                      "PHPSESSID=sdcril77rf41v6jo5k6foa0g6q; SERVERID=0e9d1282e06030e751ce657b3033714f|" + str(
            self.getLocalTime()) + "|1581750254; Hm_lpvt_8a1d0cf914523c7ed112dbd25e018957=" + str(
            self.getLocalTime()) + "; Hm_lpvt_98aead8c22e727e602de6fc4d2508c8d=" + str(
            self.getLocalTime() + 1) + "; online-uuid=" + str(uuid.uuid4())
        self.listHeaders = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Cookie": self.Cookie,
            "Host": "moocs.unipus.cn",
            "Referer": "http://moocs.unipus.cn/",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0"
        }
        self.courseHeaders = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Cookie": self.Cookie,
            "Host": "moocs.unipus.cn",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0"
        }
        self.jingPinListHeaders = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Cookie": self.Cookie,
            "Host": "moocs.unipus.cn",
            "Referer": "http://moocs.unipus.cn/course/explore?tag%5Btags%5D%5B11%5D=71&tag%5BselectedTag%5D%5Bgroup%5D=11&tag%5BselectedTag%5D%5Btag%5D=71&filter%5Btype%5D=all&filter%5Bprice%5D=all&filter%5BcurrentLevelId%5D=all&orderBy=recommendedSeq",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0"
        }
        self.error_list = []

    @retry(stop_max_attempt_number=5)
    def deep_page(self, course_info_list, href, total_list):
        session3 = HTMLSession()
        r3 = session3.get("http://moocs.unipus.cn" + href, headers=self.courseHeaders)
        self.updateCookie(r3.cookies.get_dict())
        course_list_info = CourseListInfo()
        course_list_info.url = "http://moocs.unipus.cn" + href
        course_list_info.course_name = r3.html.find("div.course-detail__title", first=True).text.replace("（暑假小学期）", '').replace(" ", '').replace(" ",'')
        term1 = r3.html.find("div.col-lg-3.col-md-4.course-sidebar.hidden-xs", first=True)
        course_list_info.term = term1.find("div.panel.panel-default>div.panel-heading>h3.panel-title", first=True).text
        course_list_info.term_id = int(re.findall(r'\d+', course_list_info.term)[0])
        course_list_info.term = "第" + str(course_list_info.term_id) + "期"
        course_list_info.platform = "中国高校外语慕课平台"
        if len(r3.html.find("span.gray-dark.mrl")) == 2:
            course_list_info.school = r3.html.find("span.gray-dark.mrl", first=True).text
            crowd = r3.html.find("div.mb5:nth-child(3) > span:nth-child(2)", first=True).text
        else:
            course_list_info.school = "无"
            crowd = r3.html.find("div.mb5:nth-child(2) > span:nth-child(2)", first=True).text
        course_list_info.crowd = re.findall(r"\d+", crowd)[0]
        course_list_info.save_time = datetime.now()
        course_list_info.isquality = 0
        team1 = r3.html.find("div.clearfix.js-teahcer-section")[1]
        team2 = team1.find("div.media.media-default")
        for teacher in team2:
            course_list_info.team = course_list_info.team + teacher.find("a.link-dark.link-dark", first=True).text + ','
        course_list_info.teacher = course_list_info.team.split(',')[0]
        course_list_info.team = course_list_info.team[0:-1]
        datetime1 = r3.html.find("div.panel-body.clearfix")[1]
        begin_end = datetime1.find("p")
        begin_time_text = begin_end[0].text
        begin_time = re.findall(r"(\d{4}-\d{2}-\d{2})", begin_time_text)
        course_list_info.start_date = datetime.strptime(begin_time[0], '%Y-%m-%d')
        end_time_text = begin_end[1].text
        end_time = re.findall(r"(\d{4}-\d{2}-\d{2})", end_time_text)
        course_list_info.end_date = datetime.strptime(end_time[0], '%Y-%m-%d')
        if course_list_info.save_time < course_list_info.start_date:
            course_list_info.status = 2
        elif (course_list_info.save_time < course_list_info.end_date) and (
                course_list_info.save_time > course_list_info.start_date):
            course_list_info.status = 1
        elif course_list_info.save_time > course_list_info.end_date:
            course_list_info.status = 0
        course_list_info.introduction = r3.html.find("div.lheight30.piece-body.p-lg.clearfix", first=True).text.replace(
            "\n", '').replace('\xa0', '').replace("\u3000", '')
        scoring_standard1 = r3.html.find("div.es-piece")
        scoring_standard2 = scoring_standard1[len(scoring_standard1)-1].find("div.piece-body.p-lg.lheight30.clearfix")
        if len(scoring_standard2) != 0:
            course_list_info.scoring_standard = scoring_standard2[0].text.replace("\n", '')
        else:
            course_list_info.scoring_standard = '无'
        r3.close()
        session3.close()
        course_list_info.crowd_num = int(course_list_info.crowd)
        course_list_info.term_number = course_list_info.term_id
        list = []
        list = list + course_info_list
        list = list + total_list
        for item in list:
            if item["course_name"] == course_list_info.course_name:
                if item["school"] == course_list_info.school:
                    if item["teacher"] == course_list_info.teacher or item["introduction"] == course_list_info.introduction:
                        if item["term_number"] > course_list_info.term_number:
                            course_list_info.term_number = item["term_number"]
                            print("------------------第一种情况-----------------------")
                            print(course_list_info.__dict__)
                            print("-----------------------------------------")
                        else:
                            item["term_number"] = course_list_info.term_number
                            print("-------------------第二种情况-----------------------")
                            print(course_list_info.__dict__)
                            print("-----------------------------------------")
                elif (item["course_name"] == course_list_info.course_name and item["teacher"] == course_list_info.teacher):
                    if item["term_number"] > course_list_info.term_number:
                        course_list_info.term_number = item["term_number"]
                        print("------------------第一种情况-----------------------")
                        print(course_list_info.__dict__)
                        print("-----------------------------------------")
                    else:
                        item["term_number"] = course_list_info.term_number
                        print("-------------------第二种情况-----------------------")
                        print(course_list_info.__dict__)
                        print("-----------------------------------------")
        course_info_list.append(course_list_info.__dict__)
        pass

    def fetch_one_page(self, i, total_list) -> list:
        course_info_list = []
        session2 = HTMLSession()
        r2 = session2.get("http://moocs.unipus.cn/course/explore?page=" + str(i), headers=self.listHeaders)
        time.sleep(0.5)
        courseList1 = r2.html.find("div.course-list.cmfs-course-list", first=True)
        courseList2 = courseList1.find("div.row>div.col-lg-12.col-md-12.col-xs-12")
        self.updateCookie(r2.cookies.get_dict())
        for item in courseList2:
            href = item.find("div.course-img.pull-left>a", first=True).attrs["href"]
            self.deep_page(course_info_list, href, total_list)
        r2.close()
        session2.close()
        return course_info_list
        pass

    def update_jingpin_per_page(self, i, total_list):
        session5 = HTMLSession()
        listUrl = "http://moocs.unipus.cn/course/explore?tag%5BselectedTag%5D%5Bgroup%5D=11&tag%5BselectedTag%5D" \
                  "%5Btag%5D=71&filter%5Btype%5D=all&filter%5Bprice%5D=all&filter%5BcurrentLevelId%5D=all&orderBy" \
                  "=recommendedSeq&page=" + str(i)
        r5 = session5.get(listUrl, headers=self.jingPinListHeaders)
        time.sleep(0.5)
        courseList1 = r5.html.find("div.course-list.cmfs-course-list", first=True)
        courseList2 = courseList1.find("div.row>div.col-lg-12.col-md-12.col-xs-12")
        self.updateCookie(r5.cookies.get_dict())
        for item in courseList2:
            head = item.find("div.course-info__header>p.link-dark", first=True)
            term = head.find("span.label.label-primary", first=True).text
            courseName = head.text.replace(term, '')
            print(courseName + term)
            for course in total_list:
                if (courseName == course["course_name"]) and (term == course["term"]):
                    course["isquality"] = 1
                    print(courseName + term + "为精品课程")
        r5.close()
        session5.close()
        pass

    @retry(stop_max_attempt_number=5)
    def get_jingPin_pages(self):
        session4 = HTMLSession()
        listUrl = "http://moocs.unipus.cn/course/explore?tag%5BselectedTag%5D%5Bgroup%5D=11&tag%5BselectedTag%5D" \
                  "%5Btag%5D=71&filter%5Btype%5D=all&filter%5Bprice%5D=all&filter%5BcurrentLevelId%5D=all&orderBy" \
                  "=recommendedSeq&page=1"
        r4 = session4.get(listUrl, headers=self.jingPinListHeaders)
        self.updateCookie(r4.cookies.get_dict())
        liList = r4.html.find("ul.pagination>li")
        lastPageHref = liList[len(liList) - 1].find("a", first=True).attrs["href"]
        listItem = lastPageHref.split("&")
        Pages = re.findall(r'\d+', listItem[len(listItem)-1])[0]
        r4.close()
        session4.close()
        return int(Pages)

        pass

    def update_jingpin_all_page(self, total_list):
        pages = self.get_jingPin_pages()
        page = 1
        for i in range(1, pages + 1):
            print("-----更新至第：" + str(page) + "/" + str(pages) + "页-----")
            self.update_jingpin_per_page(i, total_list)
            page = page + 1
        pass

    @retry(stop_max_attempt_number=5)
    def get_page_num(self):
        # "Cookie: Hm_lvt_8a1d0cf914523c7ed112dbd25e018957=1581750256,1581762434; Hm_lvt_98aead8c22e727e602de6fc4d2508c8d=1581750257,1581762434 PHPSESSID=sdcril77rf41v6jo5k6foa0g6q; SERVERID=0e9d1282e06030e751ce657b3033714f|1581766074|1581750254; Hm_lpvt_8a1d0cf914523c7ed112dbd25e018957=1581766074; Hm_lpvt_98aead8c22e727e602de6fc4d2508c8d=1581766075; online-uuid=FF5C0C42-EA2B-310B-D82B-44420770CBF0"
        # Cookie = {
        #     "acw_tc": "65c86a0915817502549618088e36602d386732bb6817cffa566e45f4a79a4f",
        #     "Hm_lpvt_8a1d0cf914523c7ed112dbd25e018957": str(self.getLocalTime()),
        #     "Hm_lpvt_98aead8c22e727e602de6fc4d2508c8d": str(self.getLocalTime() + 1),
        #     "Hm_lvt_8a1d0cf914523c7ed112dbd25e018957": "1581750256,1581762434",
        #     "Hm_lvt_98aead8c22e727e602de6fc4d2508c8d": "1581750257,1581762434",
        #     "online-uuid": uuid.uuid4(),
        #     "PHPSESSID": "sdcril77rf41v6jo5k6foa0g6q",
        #     "SERVERID": "0e9d1282e06030e751ce657b3033714f|" + str(self.getLocalTime()) + "|1581750254"
        # }
        session1 = HTMLSession()
        r1 = session1.get("http://moocs.unipus.cn/course/explore", headers=self.listHeaders)
        # print(r1.cookies.get_dict())
        self.updateCookie(r1.cookies.get_dict())
        liList = r1.html.find("ul.pagination>li")
        lastPageHref = liList[len(liList) - 1].find("a", first=True).attrs["href"]
        Pages = re.findall(r'\d+', lastPageHref)[0]
        r1.close()
        session1.close()
        return int(Pages)
        pass

    def fetch_all_page(self):
        total_list = []
        pages = self.get_page_num()
        page = 1
        for i in range(1, pages + 1):
            print("-----将爬取第：" + str(page) + "/" + str(pages) + "页-----")
            total_list = total_list + self.fetch_one_page(i, total_list)
            page = page + 1
        self.update_jingpin_all_page(total_list)
        return total_list

    def getLocalTime(self):
        now = time.time()
        return int(now)

    def updateCookie(self, Cookies):
        self.Cookie = "Hm_lvt_8a1d0cf914523c7ed112dbd25e018957=1581750256,1581762434; " \
                      "Hm_lvt_98aead8c22e727e602de6fc4d2508c8d=1581750257,1581762434; " \
                      "acw_tc=65c86a0915817502549618088e36602d386732bb6817cffa566e45f4a79a4f; " \
                      "PHPSESSID=sdcril77rf41v6jo5k6foa0g6q; online=1; Hm_lpvt_8a1d0cf914523c7ed112dbd25e018957=" + str(
            self.getLocalTime()) + "; Hm_lpvt_98aead8c22e727e602de6fc4d2508c8d=" + str(
            self.getLocalTime() + 1) + "; online-uuid=" + str(uuid.uuid4())
        self.listHeaders["Cookie"] = self.Cookie
        self.courseHeaders["Cookie"] = self.Cookie
        self.jingPinListHeaders["Cookie"] = self.Cookie
        pass

    @retry(stop_max_attempt_number=3)
    def run(self):
        total_list2 = self.fetch_all_page()
        self.get_groupid(total_list2)
        set_list = self.distinct(total_list2)
        # with open("gaoxiaowaiyumuke.pkl", "wb") as f:
        #     pickle.dump(set_list, f)
        return {
            "course_list_info": set_list,
            "error_list": self.error_list
        }

    def get_groupid(self, total_list: List):
        course_group_ids = []
        for item in total_list:
            if item["term_id"] == 1:
                item["course_group_id"] = item["url"].replace("http://moocs.unipus.cn/course/", '')
                course_group_ids.append(item)
        for item1 in course_group_ids:
            for item2 in total_list:
                if item1["course_name"] == item2["course_name"]:
                    if item1["school"] == item2["school"]:
                        if item1["teacher"] == item2["teacher"] or item1["introduction"] == item2["introduction"]:
                            item2["course_group_id"] = item1["course_group_id"]
                    elif item1["course_name"] == item2["course_name"] and item1["teacher"] == item2["teacher"]:
                        item2["course_group_id"] = item1["course_group_id"]
            pass


if __name__ == '__main__':
    fetcher = ListFetcher()
    fetcher.run()
