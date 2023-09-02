import pickle

import selenium
from requests_html import HTMLSession
from requests_html import HTML
import time
from crawler.fetcher import BaseFetcher
from persistence.model.basic_info import CourseListInfo
from selenium.webdriver.firefox.options import Options
import re
from datetime import datetime
import logging
from retrying import retry
import requests


class ListFetcher(BaseFetcher):
    def __init__(self):
        super().__init__()
        self.course_number = 0
        self.error_list = []

    @retry(stop_max_attempt_number=5)
    def request_again(self, cid, course_info_list, course_list_info, headers):
        course_term_list = CourseListInfo()
        headers["Referer"] = course_list_info.url
        r4 = requests.get("http://www.uooc.net.cn/course/getCycleInfo?cid=" + cid, headers=headers)
        time.sleep(0.5)
        json_data = r4.json()
        course_term_list.term = json_data["data"]["name"]
        start_end = json_data["data"]["start_end_time"]
        begin_end = re.findall(r"(\d{4}.\d{2}.\d{2})", start_end)
        course_term_list.start_date = datetime.strptime(begin_end[0], '%Y.%m.%d')
        course_term_list.end_date = datetime.strptime(begin_end[1], '%Y.%m.%d')
        course_term_list.save_time = datetime.now()
        team = json_data["data"]["team"]
        for teacher in team:
            course_term_list.team = course_term_list.team + teacher["name"] + ","
        course_term_list.team = course_term_list.team[0:-1]
        doc = json_data["data"]["check_rule_intro"]
        if doc != "":
            r5 = HTML(html=doc)
            course_term_list.scoring_standard = r5.text.replace('\n', '').replace(
                '\xa0', '').replace("\u3000", '')
        else:
            course_term_list.scoring_standard = "无"
        certification = json_data["data"]["certify_list"]
        if len(certification) != 0:
            for school in certification:
                course_term_list.certification = course_term_list.certification + school["name"] + ","
            course_term_list.certification = course_term_list.certification[0:-1]
        else:
            course_term_list.certification = "无学校认证"
        r4.close()
        if json_data["data"]["attribute_name"] == "MOOC公开课":
            course_term_list.ispublic = 1
        else:
            course_term_list.ispublic = 0
        course_term_list.url = course_list_info.url
        course_term_list.platform = course_list_info.platform
        course_term_list.course_name = course_list_info.course_name
        course_term_list.isquality = course_list_info.isquality
        if course_term_list.save_time < course_term_list.start_date:
            course_term_list.status = 2
        elif (course_term_list.save_time < course_term_list.end_date) and (
                course_term_list.save_time > course_term_list.start_date):
            course_term_list.status = 1
        elif course_term_list.save_time > course_term_list.end_date:
            course_term_list.status = 0
        course_term_list.teacher = course_list_info.teacher
        course_term_list.school = course_list_info.school
        course_term_list.introduction = course_list_info.introduction
        course_term_list.extra = course_list_info.extra
        course_term_list.term_id = course_list_info.term_id
        course_term_list.total_crowd = course_list_info.total_crowd
        course_term_list.total_crowd_num = int(course_term_list.total_crowd)
        course_term_list.term_number = -1
        course_info_list.append(course_term_list.__dict__)
        print(course_term_list.__dict__)
        pass

    @retry(stop_max_attempt_number=5)
    def deep_page(self, course_info_list, course_list_info, headers):
        session3 = HTMLSession()
        r3 = session3.get(course_list_info.url, headers=headers)
        time.sleep(0.8)
        termId_crowd = r3.html.find("div.info-box.clearfix>div.info-box-item")
        termId = termId_crowd[len(termId_crowd) - 2].text
        crowd = termId_crowd[len(termId_crowd) - 1].text
        course_list_info.term_id = int(re.findall(r"\d+", termId)[0])
        course_list_info.total_crowd = re.findall(r"\d+", crowd)[0]
        course_list_info.ispublic = 0
        self.course_number = self.course_number + 1
        print("正在爬第" + str(self.course_number) + "门课")
        if course_list_info.status == 0:
            r3.close()
            course_list_info.total_crowd_num = int(course_list_info.total_crowd)
            course_list_info.save_time = datetime.now()
            print(course_list_info.__dict__)
            course_info_list.append(course_list_info.__dict__)
            return 0
        else:
            id = r3.html.search_all('"id":{},"name":"')
            r3.close()
            for item in id:
                if item[0].isdigit():
                    print("cid:" + item[0])
                    self.request_again(item[0], course_info_list, course_list_info, headers)
        pass

    def fetch_one_page(self, page, headers):
        course_info_list = []
        session2 = HTMLSession()
        r2 = session2.get("http://www.uooc.net.cn/league/union/course?page=" + str(page))
        time.sleep(0.8)
        total_courses = r2.html.find("span.course-header-total", first=True).text
        course_list_section = r2.html.find("div.course-list-section", first=True)
        course_list = course_list_section.find("div.course-item")
        for item in course_list:
            course_list_info = CourseListInfo()
            url = item.find("h2.course-item-title", first=True)
            course_list_info.url = list(url.absolute_links)[0]
            course_list_info.platform = '优课联盟'
            course_title = item.find("h2.course-item-title", first=True)
            course_list_info.course_name = course_title.find("a", first=True).text
            quality = course_title.find(
                "h2.course-item-title>span.course-item-title-tag.course-item-title-tag-boutique")
            if len(quality) == 1:
                if quality[0].text == "国家精品课":
                    course_list_info.isquality = 1
                else:
                    course_list_info.isquality = 0
            else:
                course_list_info.isquality = 0
            teacher_school = item.find("div.course-item-info>span.course-item-info-i>span")
            course_list_info.teacher = teacher_school[1].text
            course_list_info.school = teacher_school[0].text
            course_list_info.introduction = item.find("div.course-item-details", first=True).text.replace("\n",
                                                                                                          '').replace(
                '\xa0', '').replace("\u3000", '')
            course_list_info.extra = total_courses
            status = item.find("span.course-item-info-i.noafter", first=True).text
            if status == "已结束":
                course_list_info.status = 0
            r2.close()
            self.deep_page(course_info_list, course_list_info, headers)
            # print(course_list_info.__dict__)
            # course_info_list.append(course_list_info.__dict__)
        return course_info_list

    def get_all_pages(self):
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Connection": "keep-alive",
            "Cookie": "Hm_lvt_d1a5821d95582e27154fc4a1624da9e3=1576301238,1578330846,1578657062,1578727235; formpath=/course/1049075890; formhash=; examRemindNum_1446917483=1; cerRemindNum_1446917483=1; JSESSID=pquc7e6qdt60qccf1kkictb827; Hm_lpvt_d1a5821d95582e27154fc4a1624da9e3=1578735576",
            "Host": "www.uooc.net.cn",
            "Referer": "http://www.uooc.net.cn/league/union/course",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0"
        }
        session = HTMLSession()
        r1 = session.get("http://www.uooc.net.cn/league/union/course", headers=headers)
        page_number_point = r1.html.find("ul.s-pagination>li")
        page_number_text = page_number_point[len(page_number_point) - 2].text
        r1.close()
        return {"page_number": int(page_number_text), "headers": headers}
        pass

    def fetch_all_page(self):
        total_list = []
        basic_info = self.get_all_pages()
        print("共：" + str(basic_info["page_number"]) + "页")
        page = 0
        for i in range(1, basic_info["page_number"] + 1):
            page = page + 1
            print("将爬取第:" + str(i) + "/" + str(basic_info["page_number"]) + "页")
            total_list = total_list + self.fetch_one_page(i, basic_info["headers"])
        print(len(total_list))
        return total_list
        pass

    def run(self):
        total_list = self.fetch_all_page()
        set_list = self.distinct(total_list)
        # with open("youkelianmeng.pkl", "wb") as f:
        #     pickle.dump(set_list, f)
        dic = {
            "course_list_info": set_list,
            "error_list": self.error_list
        }
        return dic


if __name__ == '__main__':
    fetcher = ListFetcher()
    fetcher.run()
