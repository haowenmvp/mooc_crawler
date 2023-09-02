import random
import threading
from typing import List

import requests
import time
import pickle
from fake_useragent import UserAgent
from crawler.fetcher import BaseFetcher
from crawler.fetcher.zhejiangmooc.proxy_ip import ProxyIP
from persistence.model.basic_info import CourseListInfo
import re
from datetime import datetime
import logging
from retrying import retry
import hashlib


class ListFetcher(BaseFetcher):
    def __init__(self):
        super().__init__()
        self.Cookies = ""
        self.listHeaders = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Connection": "keep-alive",
            "Cookie": self.Cookies,
            "Host": "www.zjooc.cn",
            "Referer": "https://www.zjooc.cn/course",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0",
            "X-Requested-With": "XMLHttpRequest"
        }
        self.courseHeaders = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Connection": "keep-alive",
            "Cookie": self.Cookies,
            "Host": "www.zjooc.cn",
            "Referer": "",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0",
            "X-Requested-With": "XMLHttpRequest"
        }
        self.thread_lock = threading.Lock()
        self.total_list = []
        self.error_list = []
        self.ipPool = []
        self.times = 0

    @retry(stop_max_attempt_number=5)
    def getTermInfo(self, termId, term_number, terms_number, term_list, first_term_id):
        self.Cookies = "Hm_lvt_58a9eb5d0027a08578bdc0ec5a9292c2=1581401918,1581474151,1582159733,1582172337; " \
                       "uniqueVisitorId=248d0ddf-8772-374d-4f5d-36afc8565212; " \
                       "lano.connect.sid=s%3AeMk2P0bIFAMX9WL9ElFSpKqrA6X0SAiw.InNItu" \
                       "%2Bj4gxjnCsFV5NNUCHgXu7EQaA8cnP4gvqXZVM; Hm_lpvt_58a9eb5d0027a08578bdc0ec5a9292c2=" + str(
            int(self.getLocalTime()/1000))
        self.courseHeaders["Cookie"] = self.Cookies
        time.sleep(1.2)
        ua = UserAgent()
        self.courseHeaders["User-Agent"] = ua.random
        self.courseHeaders["Referer"] = "https://www.zjooc.cn/course/" + termId
        termUrl = "https://www.zjooc.cn/ajax?time=" + self.encryption() + str(self.getLocalTime()) +"&service=%2Fjxxt%2Fapi" \
                  "%2Fcourse%2F" + termId + "%2Fget&params%5BcourseId%5D" \
                  "=" + termId + "&params%5BincludeLecturer%5D=true&params%5BincludeChapter%5D=false "
        course_list_info = CourseListInfo()
        course_list_info.url = "https://www.zjooc.cn/course/" + termId
        Proxy = self.getProxy()
        r4 = requests.get(termUrl, proxies=Proxy, headers=self.courseHeaders, timeout=60)
        courseData = r4.json()["data"]
        course_list_info.course_name = courseData["name"]
        course_list_info.platform = '浙江省高等学校在线开放课程共享平台'
        course_list_info.crowd = courseData["studentNum"]
        course_list_info.school = courseData["corpName"]
        course_list_info.teacher = courseData["teacherName"]
        course_list_info.term_id = term_number
        course_list_info.term_number = terms_number
        course_list_info.term = "第" + str(course_list_info.term_id) + "期"
        course_list_info.crowd_num = int(course_list_info.crowd)
        start_date = courseData["startDate"]
        end_date = courseData["endDate"]
        start = re.findall(r"(\d{4}-\d{2}-\d{2})", start_date)
        end = re.findall(r"(\d{4}-\d{2}-\d{2})", end_date)
        course_list_info.start_date = datetime.strptime(start[0], '%Y-%m-%d')
        course_list_info.end_date = datetime.strptime(end[0], '%Y-%m-%d')
        course_list_info.save_time = datetime.now()
        if course_list_info.save_time < course_list_info.start_date:
            course_list_info.status = 2
        elif (course_list_info.save_time < course_list_info.end_date) and (
                course_list_info.save_time > course_list_info.start_date):
            course_list_info.status = 1
        elif course_list_info.save_time > course_list_info.end_date:
            course_list_info.status = 0
        course_list_info.introduction = courseData["profile"].replace(
            "\n", '').replace('\xa0', '').replace("\u3000", '').replace("&nbsp", '')
        for teacher in courseData["courseLecturerList"]:
            course_list_info.team = course_list_info.team + teacher["name"] + ","
        course_list_info.team = course_list_info.team[0:-1]
        if courseData["courseQuality"] == 2:
            course_list_info.isquality = 1
        else:
            course_list_info.isquality = 0
        # Found = self.Deduplication(course_list_info)
        Found = 0
        if Found == 0:
            course_list_info.course_group_id = first_term_id
            term_list.append(course_list_info.__dict__)
            r4.close()
            # logging.info(course_list_info.__dict__)
        else:
            pass
        pass

    def Deduplication(self, course_list_info):
        for course in self.total_list:
            if course["course_name"] == course_list_info.course_name:
                if course["teacher"] == course_list_info.teacher:
                    if course["term_id"] == course_list_info.term_id:
                        return 1
        else:
            return 0
        pass

    def getProxy(self):
        self.thread_lock.acquire()
        item = self.ipPool[self.times]
        proxy = {item["type"]: item["address"] + ":" + item["port"]}
        self.times = self.times + 1
        if self.times > (len(self.ipPool) - 1):
            self.times = 0
        self.thread_lock.release()
        return proxy
        pass

    @retry(stop_max_attempt_number=5)
    def deep_page(self, id):
        term_list = []
        ua = UserAgent()
        self.Cookies = "Hm_lvt_58a9eb5d0027a08578bdc0ec5a9292c2=1581401918,1581474151,1582159733,1582172337; " \
                       "uniqueVisitorId=248d0ddf-8772-374d-4f5d-36afc8565212; " \
                       "lano.connect.sid=s%3AeMk2P0bIFAMX9WL9ElFSpKqrA6X0SAiw.InNItu" \
                       "%2Bj4gxjnCsFV5NNUCHgXu7EQaA8cnP4gvqXZVM; Hm_lpvt_58a9eb5d0027a08578bdc0ec5a9292c2=" + str(
            int(self.getLocalTime()/1000))
        time.sleep(1.2)
        self.courseHeaders["Cookie"] = self.Cookies
        self.courseHeaders["Referer"] = "https://www.zjooc.cn/course/" + id
        self.courseHeaders["User-Agent"] = ua.random
        Proxy = self.getProxy()
        courseUrl = "https://www.zjooc.cn/ajax?time=" + self.encryption() + str(
            self.getLocalTime()) + "&service=%2Freport%2Fapi%2Fcourse%2FcourseAnalyse%2FcourseRelated&params%5BcourseId%5D=" + id
        r3 = requests.get(courseUrl, proxies=Proxy, headers=self.courseHeaders, timeout=60)
        termList = r3.json()["data"]["childrenCourseList"]
        if len(termList) != 0:
            first_term_id = termList[len(termList)-1]["id"]
        else:
            self.add_error_list(id)
            return []
        term_number = len(termList)
        for term in termList:
            try:
                self.getTermInfo(term["id"], term_number, len(termList), term_list, first_term_id)
            except:
                self.add_error_list(term["id"])
            term_number = term_number - 1
        r3.close()
        return term_list
        pass

    def add_error_list(self, id):
        url = "https://www.zjooc.cn/course/" + id
        error_course = {
            "platform": '浙江省高等学校在线开放课程共享平台',
            "error_url": url
        }
        self.error_list.append(error_course)
        pass

    def start_thread(self, id):
        try:
            get_course_list = self.deep_page(id)
        except:
            print("-----发现出错课程----")
            print("https://www.zjooc.cn/course/" + str(id))
            self.add_error_list(id)
            get_course_list = []
        self.thread_lock.acquire()
        for item in get_course_list:
            print(item)
        self.total_list = self.total_list + get_course_list
        print("----------")
        self.thread_lock.release()
        pass

    @retry(stop_max_attempt_number=5)
    def fetch_one_page(self, i):
        ua = UserAgent()
        self.Cookies = "Hm_lvt_58a9eb5d0027a08578bdc0ec5a9292c2=1581401918,1581474151,1582159733,1582172337; " \
                       "uniqueVisitorId=248d0ddf-8772-374d-4f5d-36afc8565212; " \
                       "lano.connect.sid=s%3AeMk2P0bIFAMX9WL9ElFSpKqrA6X0SAiw.InNItu" \
                       "%2Bj4gxjnCsFV5NNUCHgXu7EQaA8cnP4gvqXZVM; Hm_lpvt_58a9eb5d0027a08578bdc0ec5a9292c2=" + str(
            int(self.getLocalTime()/1000))
        self.listHeaders["Cookie"] = self.Cookies
        self.listHeaders["User-Agent"] = ua.random
        listUrl = "https://www.zjooc.cn/ajax?time=" + self.encryption() + str(
            self.getLocalTime()) + "&service" \
                                   "=%2Fjxxt" \
                                   "%2Fapi" \
                                   "%2Fcourse%2FindexList&params%5Bname%5D=&params%5BcourseLevel%5D=0&params%5BpublishStatus%5D=&params%5BcourseCategory%5D=&params%5BminPublishStatus%5D=1&params%5BclassifyName%5D=&params%5BcourseOpenType%5D=4&params%5BcourseQuality%5D=&params%5BisFamous%5D=0&params%5BisUpShelf%5D=1&params%5BincludeTeacher%5D=true&params%5BisParent%5D=1&params%5BpageNo%5D=" + str(
            i)
        r2 = requests.get(listUrl, headers=self.listHeaders, timeout=60)
        courseList = r2.json()["data"]
        print("一共有课程")
        print(len(courseList))
        if len(courseList) == 0:
            return 0
        # self.deep_page(course["id"])
        for i in range(0, len(courseList) + 1, 3):
            threads = []
            for x in range(0, 3):
                if (i + x) < len(courseList):
                    id = courseList[i + x]["id"]
                    t = threading.Thread(target=self.start_thread, args=(id,))
                    threads.append(t)
                time.sleep(0.2)
            for item_start in threads:
                item_start.start()
            for item_end in threads:
                item_end.join()
        r2.close()
        return 1
        pass

    def get_all_pages(self):
        ua = UserAgent()
        self.Cookies = "Hm_lvt_58a9eb5d0027a08578bdc0ec5a9292c2=1581401918,1581474151,1582159733,1582172337; " \
                       "uniqueVisitorId=248d0ddf-8772-374d-4f5d-36afc8565212; " \
                       "lano.connect.sid=s%3AeMk2P0bIFAMX9WL9ElFSpKqrA6X0SAiw.InNItu" \
                       "%2Bj4gxjnCsFV5NNUCHgXu7EQaA8cnP4gvqXZVM; Hm_lpvt_58a9eb5d0027a08578bdc0ec5a9292c2=" + str(
            self.getLocalTime())
        self.listHeaders["Cookie"] = self.Cookies
        self.listHeaders["User-Agent"] = ua.random
        time.sleep(0.2)
        Proxy = self.getProxy()
        url = "https://www.zjooc.cn/ajax?time=" + self.encryption() + str(
            self.getLocalTime()) + "&service=%2Fjxxt%2Fapi%2Fcourse%2FindexList&params%5Bname%5D=&params%5BcourseLevel%5D=0&params%5BpublishStatus%5D=&params%5BcourseCategory%5D=&params%5BminPublishStatus%5D=1&params%5BclassifyName%5D=&params%5BcourseOpenType%5D=4&params%5BcourseQuality%5D=&params%5BisFamous%5D=0&params%5BisUpShelf%5D=1&params%5BincludeTeacher%5D=true&params%5BisParent%5D=1&params%5BpageNo%5D=1"
        r1 = requests.get(url, proxies=Proxy, headers=self.listHeaders, timeout=60)
        print(r1.status_code)
        total_page = r1.json()["page"]["totalPage"]
        extra = "共" + str(r1.json()["page"]["rowCount"]) + "个相关课程期次"
        r1.close()
        return {"total_page": total_page, "extra": extra}
        pass

    def deal_error_page(self):
        for item in self.error_list:
            id = item["error_url"].replace("https://www.zjooc.cn/course/", '')
            self.deep_page(id)
            dealed_course = {
                "platform": '浙江省高等学校在线开放课程共享平台',
                "error_url": "https://www.zjooc.cn/course/" + id
                            }
            self.error_list.remove(dealed_course)
        print("剩余未处理课程")
        print(self.error_list)
        pass

    def fetch_all_page(self):
        total_info = self.get_all_pages()
        for i in range(1, total_info["total_page"] + 1):
            print("-----将爬取第：" + str(i) + "/" + str(total_info["total_page"]) + "页-----")
            check = self.fetch_one_page(i)
            if check == 0:
                i = i - 1
            if i % 6 == 0:
                getIp = ProxyIP()
                self.ipPool = []
                self.ipPool = getIp.run()
        pass

    def getLocalTime(self):
        now = time.time()
        return int(now*1000)

    def encryption(self):
        m = hashlib.md5()  # 创建Md5对象
        n = str(self.getLocalTime())
        m.update(n.encode('utf-8'))  # 生成加密串，其中n是要加密的字符串
        result = m.hexdigest()
        return result

    def run(self):
        self.total_list = []
        logging.basicConfig(filename="zjwk.log", level=logging.INFO)
        getIp = ProxyIP()
        self.ipPool = getIp.run()
        self.fetch_all_page()
        set_list = self.distinct(self.total_list)
        dic = {
            "course_list_info": set_list,
            "error_list": self.error_list
        }
        # with open("zhihuizhijiao.pkl", "wb") as f:
        #     pickle.dump(dic, f)
        return dic

    def distinct(self, course_info_list: List[dict]):
        res = []
        for course_info in course_info_list:
            course_list_info = CourseListInfo()
            course_list_info.__dict__.update(course_info)
            res.append(course_list_info)
        new_course_list = []

        for course in list(set(res)):
            new_course_list.append(course.__dict__)
        return new_course_list

    @retry(stop_max_attempt_number=5)
    def run_by_url(self, url):
        id = url.replace("https://www.zjooc.cn/course/", '')
        term_list = []
        ua = UserAgent()
        self.Cookies = "Hm_lvt_58a9eb5d0027a08578bdc0ec5a9292c2=1581401918,1581474151,1582159733,1582172337; " \
                       "uniqueVisitorId=248d0ddf-8772-374d-4f5d-36afc8565212; " \
                       "lano.connect.sid=s%3AeMk2P0bIFAMX9WL9ElFSpKqrA6X0SAiw.InNItu" \
                       "%2Bj4gxjnCsFV5NNUCHgXu7EQaA8cnP4gvqXZVM; Hm_lpvt_58a9eb5d0027a08578bdc0ec5a9292c2=" + str(
            int(self.getLocalTime()/1000))
        time.sleep(1.2)
        self.courseHeaders["Cookie"] = self.Cookies
        self.courseHeaders["Referer"] = "https://www.zjooc.cn/course/" + id
        self.courseHeaders["User-Agent"] = ua.random
        Proxy = self.getProxy()
        courseUrl = "https://www.zjooc.cn/ajax?time=" + self.encryption() + str(
            self.getLocalTime()) + "&service=%2Freport%2Fapi%2Fcourse%2FcourseAnalyse%2FcourseRelated&params%5BcourseId%5D=" + id
        r3 = requests.get(courseUrl, proxies=Proxy, headers=self.courseHeaders, timeout=60)
        print(r3.json())
        termList = r3.json()["data"]["childrenCourseList"]
        if len(termList) != 0:
            first_term_id = termList[len(termList)-1]["id"]
        else:
            self.add_error_list(id)
            return []
        term_number = len(termList)
        for term in termList:
            if term["id"] == id:
                self.getTermInfo(term["id"], term_number, len(termList), term_list, first_term_id=first_term_id)
            term_number = term_number - 1
        r3.close()
        print(term_list)
        if len(term_list):
            return term_list[0]
        pass

    def run_by_urls(self, urls):
        getIp = ProxyIP()
        self.ipPool = []
        self.ipPool = getIp.run()
        term_list = []
        i = 0
        for url in urls:
            term_list.append(self.run_by_url(url))
            i = i + 1
            if i == 20:
                getIp = ProxyIP()
                self.ipPool = []
                self.ipPool = getIp.run()
                i = 0
        with open("zhejiangmooc_xiajia.pkl", "wb") as f:
            pickle.dump(term_list, f)
        return term_list
        pass

if __name__ == '__main__':
    test = ListFetcher()
    test.run()
