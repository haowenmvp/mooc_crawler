import pickle
import random
import re
import string
import time
from concurrent.futures.thread import ThreadPoolExecutor

import selenium
from requests_html import HTML
from selenium.webdriver.firefox.options import Options

from crawler.fetcher import BaseFetcher
from persistence.model.basic_info import CourseListInfo
from datetime import datetime
from retrying import retry
import requests
import threading
from utils.utils import process_course_list_info
from selenium.webdriver.remote.webdriver import WebDriver


class ListFetcher(BaseFetcher):
    def __init__(self):
        super().__init__()
        self.thread_lock = threading.Lock()
        self.total_list = []
        self.error_list = []

    @retry(stop_max_attempt_number=5)
    def deep_page(self, cid, oid, headers):
        term_list = []
        for i in range(1, int(oid) + 1):
            course_list_info = CourseListInfo()
            r3 = requests.get("https://mooc.icve.com.cn/portal/Course/getMoocCourseDetail?courseId=" + cid + "&courseOpenId=" + str(i) + "&courseType=0&isNewApi=true", headers)
            time.sleep(0.5)
            course_json = r3.json()
            course_list_info.url = "https://mooc.icve.com.cn/course.html?cid=" + cid + "#oid=" + str(i)
            course_list_info.term_id = i
            course_list_info.term = "MOOC学院第" + str(course_list_info.term_id) + "次开课"
            course_list_info.course_name = course_json["courseName"].replace("\u2022", '').replace("\u22ef", '')
            course_list_info.introduction = course_json["introduction"].replace("\n", '').replace("\u2022", '').replace("\u22ef", '')
            course_list_info.platform = '智慧职教'
            course_list_info.subject = course_json["majorName"]
            course_list_info.school = course_json["schoolName"]
            course_list_info.start_date = datetime.strptime(course_json["openClassTime"], '%Y年%m月%d日')
            course_list_info.end_date = datetime.strptime(course_json["openClassEndTime"], '%Y年%m月%d日')
            course_list_info.save_time = datetime.now()
            if course_list_info.save_time < course_list_info.start_date:
                course_list_info.status = 2
            elif (course_list_info.start_date < course_list_info.save_time) and (
                    course_list_info.save_time < course_list_info.end_date):
                course_list_info.status = 1
            elif course_list_info.save_time > course_list_info.end_date:
                course_list_info.status = 0
            course_list_info.crowd = course_json["chooseThisCourseCount"]
            course_list_info.total_crowd = course_json["chooseAllCourseCount"]
            course_list_info.clicked = course_json["logThisCount"]
            course_list_info.teacher = course_json["mainTeacherName"]
            team_list = course_json["teachTeam"]
            for teacher in team_list:
                course_list_info.team = course_list_info.team + teacher["displayName"] + ","
            course_list_info.team = course_list_info.team[0:-1]
            course_list_info.term_number = int(oid)
            course_list_info.total_crowd_num = int(course_list_info.total_crowd)
            course_list_info.crowd_num = int(course_list_info.crowd)
            course_list_info.block = "MOOC学院"
            term_list.append(course_list_info.__dict__)
            r3.close()
        return term_list
        pass

    def add_error_list(self, cid, oid):
        for i in range(1, int(oid) + 1):
            url = "https://mooc.icve.com.cn/course.html?cid=" + cid + "#oid=" + str(i)
            error_course = {
                "platform": "智慧职教",
                "error_url": url
            }
            self.error_list.append(error_course)
        pass

    def start_thread(self, cid, oid, headers,):
        try:
            get_course_list = self.deep_page(cid, oid, headers)
        except:
            get_course_list = []
            self.add_error_list(cid, oid)
            pass
        self.thread_lock.acquire()
        for item in get_course_list:
            print(item)
        self.total_list = self.total_list + get_course_list
        print("----------")
        self.thread_lock.release()
        pass

    def fetch_one_page(self, page, headers):
        r2 = requests.get("https://mooc.icve.com.cn/portal/Course/getMoocCourseList?isFinished=4&isCertificate=&sort=-studentCount&page=" + str(page) + "&pageSize=25", headers)
        time.sleep(0.5)
        list = r2.json()["list"]
        r2.close()
        for i in range(0, len(list) + 1, 3):
            threads = []
            for x in range(0, 3):
                if (i+x) < len(list):
                    cid = list[i+x]["cid"]
                    oid = list[i+x]["oid"]
                    t = threading.Thread(target=self.start_thread, args=(cid, oid, headers,))
                    threads.append(t)
            for item_start in threads:
                item_start.start()
            for item_end in threads:
                item_end.join()
        pass

    def get_basic_info(self):
        headers = {
            "Accept": "application/json,text/javascript,*/*;q=0.01",
            "Accept-Encoding": "gzip,deflate,br",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Connection": "keep-alive",
            "Cookie": "bl_uid=0mk6F4gj5CC7kCdw7jILs1XnmL4b;acw_tc=76b20fe615790557642937738e1bb80aaa86a911818d7bd8b4411483f20ac9",
            "Host": "mooc.icve.com.cn",
            "Referer": "https://mooc.icve.com.cn/courseList.html",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0"
        }
        r1 = requests.get("https://mooc.icve.com.cn/portal/Course/getMoocCourseList?isFinished=4&isCertificate=&sort=-studentCount&page=1&pageSize=25")
        time.sleep(0.5)
        json_data = r1.json()
        r1.close()
        return {"totalCount": json_data["pagination"]["totalCount"], "headers": headers}
        pass

    def fetch_all_page(self):
        basic_info = self.get_basic_info()
        extra = "共：" + str(basic_info["totalCount"]) + "门课程"
        print(extra)
        page_number = int(basic_info["totalCount"]/25 + 1)
        print("共" + str(page_number) + "页")
        page = 0
        for i in range(1, page_number + 1):
            page = page + 1
            print("将爬取第:" + str(i) + "/" + str(page_number) + "页")
            self.fetch_one_page(i, basic_info["headers"])
        for item3 in self.total_list:
            item3["extra"] = extra
        pass

    def run(self):
        self.total_list = []
        self.fetch_all_page()
        set_list = self.distinct(self.total_list)
        dict = {
            "course_list_info": set_list,
            "error_list": self.error_list
        }
        zi_yuan_ku = Fetcher_ziyuanku()
        data_ziyuanku = zi_yuan_ku.run()
        shu_zi_ke_cheng = Fetcher_shuzikecheng()
        data_shuzikecheng = shu_zi_ke_cheng.run()
        dict["course_list_info"] = dict["course_list_info"] + data_ziyuanku["course_list_info"] + data_shuzikecheng["course_list_info"]
        dict["error_list"] = dict["error_list"] + data_ziyuanku["error_list"] + data_shuzikecheng["error_list"]
        # with open("zhihuizhijiao.pkl", "wb") as f:
        #     pickle.dump(dict["course_list_info"], f)
        return dict


class Fetcher_ziyuanku(BaseFetcher):
    def __init__(self):
        super().__init__()
        self.Listheaders = {
            "Host": "www.icve.com.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Accept-Encoding": "gzip, deflate, br",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "X-Tingyun-Id": "ijTrt7Wy9Fs;r=361105351",
            "Content-Length": "45",
            "Origin": "https://www.icve.com.cn",
            "Connection": "keep-alive",
            "Referer": "https://www.icve.com.cn/portal_new/course/course.html?keyvalue=",
            "Cookie": "Cookie: acw_tc=2f624a0f15853651852968001e7078ebcdc9daf3c8cc5d9f791842ec5d2c6d; TY_SESSION_ID=a479e086-67b8-4da3-8bd8-af2a309fa2a2; verifycode=78BAE10E411B48E370C9497148A74BAF@637209908099651416",
            "Cache-Control": "max-age=0"
        }
        self.courseHeaders = {
            "Host": "www.icve.com.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Accept-Encoding": "gzip, deflate, br",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "X-Tingyun-Id": "ijTrt7Wy9Fs;r=365426225",
            "Content-Length": "31",
            "Origin": "https://www.icve.com.cn",
            "Connection": "keep-alive",
            "Referer": "https://www.icve.com.cn/portal_new/courseinfo/courseinfo.html?courseid=icpzahgrxp9n1tcrivuc9g",
            "Cookie": "acw_tc=2f624a0f15853651852968001e7078ebcdc9daf3c8cc5d9f791842ec5d2c6d; TY_SESSION_ID=a479e086-67b8-4da3-8bd8-af2a309fa2a2; verifycode=EA1DD9700AF28D1D21CDADF429D8072B@637209910208841501",
            "Cache-Control": "max-age=0"
        }
        self.total_list = []
        self.thread_lock = threading.Lock()
        self.error_list = []
        pass

    def get_page_num(self):
        url = "https://www.icve.com.cn/portal/course/getNewCourseInfo?page=1"
        data = {"kczy": "",
                "order": "DatePublish",
                "printstate": "",
                "keyvalue": "",}
        # json_data = json.dumps(data)
        self.Listheaders["X-Tingyun-Id"] = "ijTrt7Wy9Fs;r=" + str(random.randint(100000000, 999999999))
        r1 = requests.post(url, data=data, headers=self.Listheaders)
        totalCount = r1.json()["pagination"]["totalCount"]
        r1.close()
        return totalCount
        pass

    def fetch_all(self):
        totalCount = self.get_page_num()
        extra = "共：" + str(totalCount) + "门课程"
        print(extra)
        if int(totalCount % 8):
            page_number = int(totalCount/8 + 1)
        else:
            page_number = int(totalCount/8)
        print("共" + str(page_number) + "页")
        page = 0
        for i in range(1, page_number + 1):
            page = page + 1
            print("将爬取第:" + str(i) + "/" + str(page_number) + "页")
            self.fetch_one_page(i)
        for item3 in self.total_list:
            item3["extra"] = extra
        pass

    @retry(stop_max_attempt_number=5)
    def fetch_one_page(self, i):
        url = "https://www.icve.com.cn/portal/course/getNewCourseInfo?page=" + str(i)
        data = {"order": "DatePublish",
                "kczy": "",
                "keyvalue": "",
                "printstate": ""}
        # json_data = json.dumps(data)
        self.Listheaders["X-Tingyun-Id"] = "ijTrt7Wy9Fs;r=" + str(random.randint(100000000, 999999999))
        r2 = requests.post(url, data=data, headers=self.Listheaders)
        course_list = r2.json()["list"]
        r2.close()
        with ThreadPoolExecutor(max_workers=8) as t:
            for course in course_list:
                t.submit(self.startThread, course)
        pass

    def startThread(self, part_info):
        self.thread_lock.acquire()
        try:
            course_list_info = self.deep_page(part_info)
        except:
            self.error_list.append({
                "platform": "智慧职教",
                "error_url": "https://www.icve.com.cn/portal_new/courseinfo/courseinfo.html?courseid=" + part_info["Id"]
            })
            course_list_info = {}
        self.total_list.append(course_list_info)
        self.thread_lock.release()
        pass

    @retry(stop_max_attempt_number=5)
    def deep_page(self, part_info):
        url = "https://www.icve.com.cn/Portal/courseinfo/getCourseInfo"
        data = {"courseid": part_info["Id"]}
        # json_data = json.dumps(data)
        self.courseHeaders["Referer"] = "https://www.icve.com.cn/portal_new/courseinfo/courseinfo.html?courseid=" + \
                                        part_info["Id"]
        n = ''.join(random.sample(string.ascii_letters + string.digits, 32))
        self.courseHeaders[
            "Cookie"] = "acw_tc=2f624a0f15853651852968001e7078ebcdc9daf3c8cc5d9f791842ec5d2c6d; TY_SESSION_ID=a479e086-67b8-4da3-8bd8-af2a309fa2a2; verifycode=" + n + "@637209910208841501"
        self.courseHeaders["X-Tingyun-Id"] = "ijTrt7Wy9Fs;r=" + str(random.randint(100000000, 999999999))
        r3 = requests.post(url, data=data, headers=self.courseHeaders, timeout=5)
        course_info = r3.json()
        course_list_info = CourseListInfo()
        course_list_info.course_name = course_info["info"]["Title"].replace("\n", '').replace("\u2022", '').replace(
            "\u22ef", '')
        course_list_info.school = part_info["UnitName"]
        course_list_info.teacher = part_info["TeacherDisplayname"]
        course_list_info.block = "资源库"
        course_list_info.url = "https://www.icve.com.cn/portal_new/courseinfo/courseinfo.html?courseid=" + part_info[
            "Id"]
        course_list_info.term_id = 1
        url2 = "https://www.icve.com.cn/Portal/courseinfo/getLeftMenu"
        data2 = {"courseid": part_info["Id"]}
        n = ''.join(random.sample(string.ascii_letters + string.digits, 32))
        self.courseHeaders[
            "Cookie"] = "acw_tc=2f624a0f15853651852968001e7078ebcdc9daf3c8cc5d9f791842ec5d2c6d; TY_SESSION_ID=a479e086-67b8-4da3-8bd8-af2a309fa2a2; verifycode=" + n + "@637209910208841501"
        self.courseHeaders["X-Tingyun-Id"] = "ijTrt7Wy9Fs;r=" + str(random.randint(100000000, 999999999))
        r4 = requests.post(url2, data=data2, headers=self.courseHeaders)
        first_member = r4.json()["list"]
        for item in first_member:
            course_list_info.team = course_list_info.team + item["TeacherName"] + ","
        second_members = r4.json()["teacherlist"]
        for item2 in second_members:
            course_list_info.team = course_list_info.team + item2["TeacherDisplayName"] + ","
        course_list_info.team = course_list_info.team[0:-1]
        course_list_info.platform = "智慧职教"
        course_list_info.start_date = datetime.strptime(part_info["DatePublish"], "%Y/%m/%d")
        course_list_info.save_time = datetime.now()
        if course_list_info.save_time > course_list_info.start_date:
            course_list_info.status = 1
        elif course_list_info.save_time < course_list_info.start_date:
            course_list_info.status = 2
        course_list_info.total_crowd = part_info["Num"]
        course_list_info.total_crowd_num = int(course_list_info.total_crowd)
        introduction = course_info["info"]["Content"]
        try:
            if introduction != "":
                r5 = HTML(html=introduction)
                course_list_info.introduction = r5.text.replace(
                    "\n", '').replace('\xa0', '').replace("\u3000", '').replace("&nbsp", '').replace("\u2022", '').replace("\u22ef", '').replace("\uff64", '').replace("\uff61", '')
        except:
            course_list_info.introduction = "存在非法字符"
        url3 = "https://www.icve.com.cn/Portal/courseinfo/getHeadInfo"
        data3 = {"courseid": part_info["Id"]}
        n = ''.join(random.sample(string.ascii_letters + string.digits, 32))
        self.courseHeaders[
            "Cookie"] = "acw_tc=2f624a0f15853651852968001e7078ebcdc9daf3c8cc5d9f791842ec5d2c6d; TY_SESSION_ID=a479e086-67b8-4da3-8bd8-af2a309fa2a2; verifycode=" + n + "@637209910208841501"
        self.courseHeaders["X-Tingyun-Id"] = "ijTrt7Wy9Fs;r=" + str(random.randint(100000000, 999999999))
        r6 = requests.post(url3, data=data3, headers=self.courseHeaders)
        head_info = r6.json()
        course_classfication = head_info["list"]["CourseClassfication"]
        r6.close()
        r7 = HTML(html=course_classfication)
        subject = r7.text
        course_list_info.subject = subject
        course_list_info.course_attributes = head_info["list"]["TypeName"]
        r4.close()
        r3.close()
        print(course_list_info.__dict__)
        return course_list_info.__dict__

    def run(self) -> dict:
        self.fetch_all()
        print("去重前数量：" + str(len(self.total_list)))
        set_list = self.distinct(self.total_list)
        print("去重后的数量：" + str(len(set_list)))
        dict = {
            "course_list_info": set_list,
            "error_list": self.error_list
        }
        return dict
        pass

class Fetcher_shuzikecheng(BaseFetcher):
    def __init__(self):
        super().__init__()
        self.Listheaders = {
            "Host": "www.icve.com.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Accept-Encoding": "gzip, deflate, br",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "X-Tingyun-Id": "ijTrt7Wy9Fs;r=361105351",
            "Content-Length": "45",
            "Origin": "https://www.icve.com.cn",
            "Connection": "keep-alive",
            "Referer": "https://www.icve.com.cn/portal_new/opencourse/gjs.html",
            "Cookie": "Cookie: acw_tc=2f624a0f15853651852968001e7078ebcdc9daf3c8cc5d9f791842ec5d2c6d; TY_SESSION_ID=a479e086-67b8-4da3-8bd8-af2a309fa2a2; verifycode=78BAE10E411B48E370C9497148A74BAF@637209908099651416",
            "Cache-Control": "max-age=0"
        }
        self.courseHeaders = {
            "Host": "www.icve.com.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Accept-Encoding": "gzip, deflate, br",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "X-Tingyun-Id": "ijTrt7Wy9Fs;r=365426225",
            "Content-Length": "31",
            "Origin": "https://www.icve.com.cn",
            "Connection": "keep-alive",
            "Referer": "https://www.icve.com.cn/portal_new/courseinfo/courseinfo.html?courseid=icpzahgrxp9n1tcrivuc9g",
            "Cookie": "acw_tc=2f624a0f15853651852968001e7078ebcdc9daf3c8cc5d9f791842ec5d2c6d; TY_SESSION_ID=a479e086-67b8-4da3-8bd8-af2a309fa2a2; verifycode=EA1DD9700AF28D1D21CDADF429D8072B@637209910208841501",
            "Cache-Control": "max-age=0"
        }
        self.total_list = []
        self.thread_lock = threading.Lock()
        self.error_list = []
        pass

    def fetch_all(self):
        url = "https://www.icve.com.cn/portal/GJS/getBodyInfo"
        data = {"projectId": "",
                "page": "1",
                "printstate": "",
                "keyvalue": ""}
        r1 = requests.post(url, data=data, headers=self.Listheaders)
        totalCount = r1.json()["pagination"]["totalCount"]
        extra = "共：" + str(totalCount) + "门课程"
        print(extra)
        course_list = r1.json()["list"]
        for course in course_list:
            with ThreadPoolExecutor(max_workers=8) as t:
                t.submit(self.startThread, course)
        # for course in course_list:
        #     self.startThread(course)
        for item3 in self.total_list:
            item3["extra"] = extra
        pass

    def startThread(self, part_info):
        self.thread_lock.acquire()
        try:
            course_list_info = self.deep_page(part_info)
        except:
            self.error_list.append({
                "platform": "智慧职教",
                "error_url": "https://www.icve.com.cn/portal_new/courseinfo/courseinfo.html?courseid=" + part_info["Id"]
            })
            course_list_info = {}
        self.total_list.append(course_list_info)
        self.thread_lock.release()
        return True
        pass

    @retry(stop_max_attempt_number=5)
    def deep_page(self, part_info):
        url = "https://www.icve.com.cn/Portal/courseinfo/getCourseInfo"
        data = {"courseid": part_info["Id"]}
        # json_data = json.dumps(data)
        self.courseHeaders["Referer"] = "https://www.icve.com.cn/portal_new/courseinfo/courseinfo.html?courseid=" + \
                                        part_info["Id"]
        n = ''.join(random.sample(string.ascii_letters + string.digits, 32))
        self.courseHeaders[
            "Cookie"] = "acw_tc=2f624a0f15853651852968001e7078ebcdc9daf3c8cc5d9f791842ec5d2c6d; TY_SESSION_ID=a479e086-67b8-4da3-8bd8-af2a309fa2a2; verifycode=" + n + "@637209910208841501"
        self.courseHeaders["X-Tingyun-Id"] = "ijTrt7Wy9Fs;r=" + str(random.randint(100000000, 999999999))
        r3 = requests.post(url, data=data, headers=self.courseHeaders, timeout=5)
        course_info = r3.json()
        course_list_info = CourseListInfo()
        course_list_info.course_name = course_info["info"]["Title"].replace("\n", '').replace("\u2022", '').replace(
            "\u22ef", '')
        course_list_info.school = part_info["UnitName"]
        course_list_info.teacher = part_info["TeacherDisplayname"]
        course_list_info.block = "数字课程"
        course_list_info.url = "https://www.icve.com.cn/portal_new/courseinfo/courseinfo.html?courseid=" + \
                               part_info["Id"]
        course_list_info.term_id = 1
        url2 = "https://www.icve.com.cn/Portal/courseinfo/getLeftMenu"
        data2 = {"courseid": part_info["Id"]}
        n = ''.join(random.sample(string.ascii_letters + string.digits, 32))
        self.courseHeaders[
            "Cookie"] = "acw_tc=2f624a0f15853651852968001e7078ebcdc9daf3c8cc5d9f791842ec5d2c6d; TY_SESSION_ID=a479e086-67b8-4da3-8bd8-af2a309fa2a2; verifycode=" + n + "@637209910208841501"
        self.courseHeaders["X-Tingyun-Id"] = "ijTrt7Wy9Fs;r=" + str(random.randint(100000000, 999999999))
        r4 = requests.post(url2, data=data2, headers=self.courseHeaders)
        first_member = r4.json()["list"]
        for item in first_member:
            course_list_info.team = course_list_info.team + item["TeacherName"] + ","
        second_members = r4.json()["teacherlist"]
        for item2 in second_members:
            course_list_info.team = course_list_info.team + item2["TeacherDisplayName"] + ","
        course_list_info.team = course_list_info.team[0:-1]
        course_list_info.platform = "智慧职教"
        course_list_info.start_date = datetime.strptime(part_info["DatePublish"], "%Y/%m/%d")
        course_list_info.save_time = datetime.now()
        if course_list_info.save_time > course_list_info.start_date:
            course_list_info.status = 1
        elif course_list_info.save_time < course_list_info.start_date:
            course_list_info.status = 2
        course_list_info.total_crowd = str(part_info["Num"])
        course_list_info.total_crowd_num = int(course_list_info.total_crowd)
        introduction = course_info["info"]["Content"]
        try:
            if introduction != "":
                r5 = HTML(html=introduction)
                course_list_info.introduction = r5.text.replace(
                    "\n", '').replace('\xa0', '').replace("\u3000", '').replace("&nbsp", '').replace("\u2022",
                                                                                                     '').replace(
                    "\u22ef", '').replace("\uff64", '').replace("\uff61", '')
        except:
            course_list_info.introduction = "存在非法字符"
        url3 = "https://www.icve.com.cn/Portal/courseinfo/getHeadInfo"
        data3 = {"courseid": part_info["Id"]}
        n = ''.join(random.sample(string.ascii_letters + string.digits, 32))
        self.courseHeaders[
            "Cookie"] = "acw_tc=2f624a0f15853651852968001e7078ebcdc9daf3c8cc5d9f791842ec5d2c6d; TY_SESSION_ID=a479e086-67b8-4da3-8bd8-af2a309fa2a2; verifycode=" + n + "@637209910208841501"
        self.courseHeaders["X-Tingyun-Id"] = "ijTrt7Wy9Fs;r=" + str(random.randint(100000000, 999999999))
        r6 = requests.post(url3, data=data3, headers=self.courseHeaders)
        head_info = r6.json()
        course_classfication = head_info["list"]["CourseClassfication"]
        r6.close()
        r7 = HTML(html=course_classfication)
        subject = r7.text
        course_list_info.subject = subject
        course_list_info.course_attributes = head_info["list"]["TypeName"]
        r4.close()
        r3.close()
        print(course_list_info.__dict__)
        return course_list_info.__dict__
        pass

    def run(self) -> dict:
        self.fetch_all()
        print("去重前数量：" + str(len(self.total_list)))
        set_list = self.distinct(self.total_list)
        print("去重后的数量：" + str(len(set_list)))
        dict = {
            "course_list_info": self.total_list,
            "error_list": self.error_list
        }
        return dict
        pass

if __name__ == '__main__':
    fetcher = ListFetcher()
    fetcher.run()
