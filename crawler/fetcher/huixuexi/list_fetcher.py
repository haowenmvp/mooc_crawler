import pickle

import selenium
from requests_html import HTMLSession
import requests
import time

from selenium.webdriver.firefox.options import Options
from selenium.webdriver.remote.webdriver import WebDriver
from crawler.fetcher import BaseFetcher
from persistence.model.basic_info import CourseListInfo
import re
from datetime import datetime


class ListFetcher(BaseFetcher):

    def __init__(self):
        super().__init__()
        self.Cookie = ""
        self.listHeaders = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Connection": "keep-alive",
            "Content-Length": "65",
            "Content-Type": "application/x-www-form-urlencoded",
            "Cookie": self.Cookie,
            "Host": "huixuexi.crtvup.com.cn",
            "Origin": "https://huixuexi.crtvup.com.cn",
            "Referer": "https://huixuexi.crtvup.com.cn/index/auth/course",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0",
            "X-Requested-With": "XMLHttpRequest"
        }
        self.courseHeader = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Connection": "keep-alive",
            "Cookie": self.Cookie,
            "Host": "huixuexi.crtvup.com.cn",
            "Referer": "https://huixuexi.crtvup.com.cn/index/auth/course",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0"
        }
        self.error_list = []

    def term_info(self, course_info_list, term_list, term_number):
        for item in term_list:
            course_list_info = CourseListInfo()
            course_list_info.url = "https://huixuexi.crtvup.com.cn" + item["url"]
            session3 = HTMLSession()
            self.Cookie = "PHPSESSID=vripbj7tl1md0kq9o3euivlj4l; Hm_lvt_9e244dc9eecff182dcf0194802b10840=" + str(
                self.getLocalTime) + "; Hm_lpvt_9e244dc9eecff182dcf0194802b10840=" + str(self.getLocalTime)
            self.courseHeader["Cookie"] = self.Cookie
            r3 = session3.get(course_list_info.url, headers=self.courseHeader, timeout=30)
            basicInfo = r3.html.find("div.informationBlock.brief", first=True)
            courseName = basicInfo.find("div.rt>h3", first=True)
            course_list_info.course_name = courseName.text
            course_list_info.platform = "国家开放大学出版社有限公司（荟学习网）"
            teachers = basicInfo.find("div.rt>div.teachers>span", first=True).text
            uselessInfo = basicInfo.find("div.teachers>span>b", first=True).text
            teachers2 = teachers.replace(uselessInfo, '')
            course_list_info.team = teachers2.replace(' ', '')
            course_list_info.teacher = teachers2.split("、")[0].split(" ")[1].replace("等", "")
            timesCrowd = basicInfo.find("div.rt>div.current>div")
            course_list_info.course_attributes = timesCrowd[1].find("p")[0].text.replace("课程类型：", '')
            begin_end = timesCrowd[0].text
            begin_end2 = re.findall(r'\d+年\d+月\d+日', begin_end)
            course_list_info.start_date = datetime.strptime(begin_end2[0], '%Y年%m月%d日')
            course_list_info.end_date = datetime.strptime(begin_end2[1], '%Y年%m月%d日')
            course_list_info.save_time = datetime.now()
            if course_list_info.save_time < course_list_info.start_date:
                course_list_info.status = 2
            elif (course_list_info.save_time < course_list_info.end_date) and (
                    course_list_info.save_time > course_list_info.start_date):
                course_list_info.status = 1
            elif course_list_info.save_time > course_list_info.end_date:
                course_list_info.status = 0
            crowd = timesCrowd[1].find("p")[1].text
            crowdText = re.findall(r'\d+', crowd)
            if len(crowdText):
                course_list_info.crowd = crowdText[0]
            else:
                course_list_info.crowd = '0'
            otherInfo = r3.html.find("div.informationBlock.lessonMessage", first=True)
            introduction = otherInfo.find("div.lf>div.lessonBlock>div.blocks.lessonIntroduction>dl>dd", first=True).text
            course_list_info.introduction = introduction.replace("\n", '').replace('\xa0', '').replace("\u3000", '')
            school = otherInfo.find("div.rt>div")[0].find("div.rt.schoolName")
            if len(school):
                course_list_info.school = school[0].find("h3", first=True).text
            else:
                course_list_info.school = '无'
            course_list_info.scoring_standard = otherInfo.find("div.rt>div")[1].text
            course_list_info.crowd_num = int(course_list_info.crowd)
            isfree = r3.html.find("a#submitBtn")
            if len(isfree) != 0:
                if isfree == "立即购买":
                    course_list_info.isfree = 0
                else:
                    course_list_info.isfree = 1
            else:
                if r3.html.find("a.ended", first=True).text == "非公开课":
                    course_list_info.ispublic = 0
            course_list_info.term = item["term"]
            course_list_info.term_id = int(re.findall(r"\d+", course_list_info.term)[0])
            course_list_info.term_number = term_number
            course_info_list.append(course_list_info.__dict__)
            r3.close()
            session3.close()
            print(course_list_info.__dict__)
        pass

    def deep_page(self, course_info_list, courseId):
        hasterm = False
        isSPOC = False
        term_list = list()
        course_list_info = CourseListInfo()
        course_list_info.url = "https://huixuexi.crtvup.com.cn/index/auth/detail/id/" + str(courseId)
        session3 = HTMLSession()
        self.Cookie = "PHPSESSID=vripbj7tl1md0kq9o3euivlj4l; Hm_lvt_9e244dc9eecff182dcf0194802b10840=" + str(
            self.getLocalTime) + "; Hm_lpvt_9e244dc9eecff182dcf0194802b10840=" + str(self.getLocalTime)
        self.courseHeader["Cookie"] = self.Cookie
        r3 = session3.get(course_list_info.url, headers=self.courseHeader, timeout=30)
        basicInfo = r3.html.find("div.informationBlock.brief", first=True)
        courseName = basicInfo.find("div.rt>h3", first=True)
        course_list_info.course_name = courseName.text
        course_list_info.platform = "国家开放大学出版社有限公司（荟学习网）"
        teachers = basicInfo.find("div.rt>div.teachers>span", first=True).text
        uselessInfo = basicInfo.find("div.teachers>span>b", first=True).text
        teachers2 = teachers.replace(uselessInfo, '')
        course_list_info.team = teachers2.replace(' ', '')
        course_list_info.teacher = teachers2.split("、")[0].split(" ")[1].replace("等", "")
        timesCrowd = basicInfo.find("div.rt>div.current>div")
        course_list_info.course_attributes = timesCrowd[1].find("p")[0].text.replace("课程类型：", '').replace("SPOC课程", '')\
            .replace(" ", '')
        course_list_info.save_time = datetime.now()
        try:
            begin_end = timesCrowd[0].text
            begin_end2 = re.findall(r'\d+年\d+月\d+日', begin_end)
            course_list_info.start_date = datetime.strptime(begin_end2[0], '%Y年%m月%d日')
            course_list_info.end_date = datetime.strptime(begin_end2[1], '%Y年%m月%d日')
            if course_list_info.save_time < course_list_info.start_date:
                course_list_info.status = 2
            elif (course_list_info.save_time < course_list_info.end_date) and (
                    course_list_info.save_time > course_list_info.start_date):
                course_list_info.status = 1
            elif course_list_info.save_time > course_list_info.end_date:
                course_list_info.status = 0
        except IndexError:
            if len(re.findall(r'总计开课', timesCrowd[0].text)):
                if len(r3.html.find("div#opencourse")):
                    hasterm = True
                    terms = r3.html.find("div.rt>div.current>div>div.cycle>div.lf", first=True)
                    terms2 = terms.find("p")
                    course_list_info.term_number = len(terms2)
                    course_list_info.term = "第" + str(len(terms2)) + "期"
                    course_list_info.term_id = len(terms2)
                    begin_end = r3.html.find("div#opencourse>p")
                    begin_end2 = begin_end[len(begin_end) - 1].text
                    begin_end3 = re.findall(r'\d+年\d+月\d+日', begin_end2)
                    course_list_info.start_date = datetime.strptime(begin_end3[0], '%Y年%m月%d日')
                    course_list_info.end_date = datetime.strptime(begin_end3[1], '%Y年%m月%d日')
                    if course_list_info.save_time < course_list_info.start_date:
                        course_list_info.status = 2
                    elif (course_list_info.save_time < course_list_info.end_date) and (
                            course_list_info.save_time > course_list_info.start_date):
                        course_list_info.status = 1
                    elif course_list_info.save_time > course_list_info.end_date:
                        course_list_info.status = 0
                    for i in range(0, len(terms2) - 1):
                        term_info = dict()
                        term_info["url"] = terms2[i].find("a", first=True).attrs["href"]
                        term_info["term"] = re.search(r'第\d+期', terms2[i].text).group()
                        term_list.append(term_info)
                else:
                    isSPOC = True
                    terms3 = r3.html.find("div.rt>div.current>div>div.cycle>div.lf", first=True)
                    terms4 = terms3.find("p")
                    if len(terms4):
                        hasterm = True
                        for i in range(0, len(terms4)):
                            term_info = dict()
                            term_info["url"] = terms4[i].find("a", first=True).attrs["href"]
                            term_info["term"] = re.search(r'第\d+期', terms4[i].text).group()
                            term_list.append(term_info)
            pass
        crowd = timesCrowd[1].find("p")[1].text
        crowdText = re.findall(r'\d+', crowd)
        if len(crowdText):
            course_list_info.crowd = crowdText[0]
        else:
            course_list_info.crowd = '0'
        otherInfo = r3.html.find("div.informationBlock.lessonMessage", first=True)
        introduction = otherInfo.find("div.lf>div.lessonBlock>div.blocks.lessonIntroduction>dl>dd", first=True).text
        course_list_info.introduction = introduction.replace("\n", '').replace('\xa0', '').replace("\u3000", '')
        school = otherInfo.find("div.rt>div")[0].find("div.rt.schoolName")
        if len(school):
            course_list_info.school = school[0].find("h3", first=True).text
        else:
            course_list_info.school = '无'
        try:
            course_list_info.scoring_standard = otherInfo.find("div.rt>div")[1].text
        except IndexError:
            course_list_info.scoring_standard = "无"
        course_list_info.crowd_num = int(course_list_info.crowd)
        isfree = r3.html.find("a#submitBtn")
        if len(isfree) != 0:
            if isfree == "立即购买":
                course_list_info.isfree = 0
            else:
                course_list_info.isfree = 1
        else:
            if len(r3.html.find("a.ended")) != 0:
                if r3.html.find("a.ended", first=True).text == "非公开课":
                    course_list_info.ispublic = 0
        if hasterm:
            print(term_list)
            if not isSPOC:
                course_info_list.append(course_list_info.__dict__)
                print(course_list_info.__dict__)
                self.term_info(course_info_list, term_list, course_list_info.term_number)
            else:
                self.term_info(course_info_list, term_list, len(term_list))
        else:
            if not isSPOC:
                course_list_info.term_id = 1
                course_list_info.term_number = 1
                course_list_info.term = "第" + str(course_list_info.term_id) + "期"
                course_info_list.append(course_list_info.__dict__)
                print(course_list_info.__dict__)
        r3.close()
        session3.close()
        pass

    def fetch_one_page(self, i):
        course_info_list = []
        self.Cookie = "PHPSESSID=vripbj7tl1md0kq9o3euivlj4l; Hm_lvt_9e244dc9eecff182dcf0194802b10840=" + str(
            self.getLocalTime) + "; Hm_lpvt_9e244dc9eecff182dcf0194802b10840=" + str(self.getLocalTime)
        self.listHeaders["Cookie"] = self.Cookie
        data = {
            "course_classify": 0,
            "stage": "all",
            "order": "order",
            "per_page": 4,
            "current_page": i
        }
        r2 = requests.post("https://huixuexi.crtvup.com.cn/index/auth/course", data=data, headers=self.listHeaders)
        courseList = r2.json()
        for course in courseList["data"]:
            self.deep_page(course_info_list, course["id"])
        r2.close()
        return course_info_list
        pass

    def get_all_pages(self):
        self.Cookie = "PHPSESSID=vripbj7tl1md0kq9o3euivlj4l; Hm_lvt_9e244dc9eecff182dcf0194802b10840=" + str(
            self.getLocalTime) + "; Hm_lpvt_9e244dc9eecff182dcf0194802b10840=" + str(self.getLocalTime)
        self.listHeaders["Cookie"] = self.Cookie
        data = {
            "course_classify": 0,
            "stage": "all",
            "order": "order",
            "per_page": 4,
            "current_page": 1
        }
        r1 = requests.post("https://huixuexi.crtvup.com.cn/index/auth/course", data=data, headers=self.listHeaders)
        BasicData = r1.json()
        pages = BasicData["code"]
        print(pages)
        r1.close()
        return pages
        pass

    def fetch_all_page(self):
        total_list = []
        pages = self.get_all_pages()
        page = 1
        for i in range(1, pages + 1):
            print("-----将爬取第：" + str(page) + "/" + str(pages) + "页-----")
            total_list = total_list + self.fetch_one_page(i)
            page = page + 1
        print(len(total_list))
        return total_list
        pass

    def getLocalTime(self):
        now = time.time()
        return int(now)

    def run(self):
        total_list = self.fetch_all_page()
        set_list = self.distinct(total_list)
        # with open("huixuexi.pkl", "wb") as f:
        #     pickle.dump(set_list, f)
        return {
            "course_list_info": set_list,
            "error_list": self.error_list
        }


if __name__ == '__main__':
    fetcher = ListFetcher()
    fetcher.run()
