# -*- coding: utf-8 -*-
import json
import logging
import pickle
import re
import string
import time
from typing import List
from urllib.parse import quote

import simplejson
from fake_useragent import UserAgent
import selenium
from crawler.fetcher import BaseFetcher
from persistence.model.basic_info import CourseListInfo
from datetime import datetime
from retrying import retry
import requests
import threading
from utils.utils import process_course_list_info


class ListFetcher(BaseFetcher):
    def __init__(self):
        super().__init__()
        self.ListHeaders = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Cache-Control": "max-age=0",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Connection": "keep-alive",
            "Content-Length": "85",
            "Cookie": "login_type=P; csrftoken=Ym5pKqG9VzFQjl6ab9ys8vS1wJjNx52Y; sessionid=vv8dsdlmw4od0o68z63ee9af3zfiw1oc; k=24150569",
            "Host": "next.xuetangx.com",
            "Origin": "https://next.xuetangx.com",
            "Referer": "https://next.xuetangx.com/search?query=",
            "TE": "Trailers",
            "x-client": "web",
            "xtbz": "xt",
            "X-CSRFToken": "Ym5pKqG9VzFQjl6ab9ys8vS1wJjNx52Y"
        }
        self.basicCourseHeader = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Connection": "keep-alive",
            "Cookie": "login_type=P; csrftoken=Ym5pKqG9VzFQjl6ab9ys8vS1wJjNx52Y; sessionid=vv8dsdlmw4od0o68z63ee9af3zfiw1oc; k=24150569",
            "Host": "next.xuetangx.com",
            "Referer": "",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0",
            "x-client": "web",
            "X-CSRFToken": "Ym5pKqG9VzFQjl6ab9ys8vS1wJjNx52Y",
            "xtbz": "xt",
            "TE": "Trailers"
        }
        self.thread_lock = threading.Lock()
        self.total_list = []
        self.error_list = []

    @retry(stop_max_attempt_number=3)
    def detail_info(self, sign, classroom_id, isquality, total_crowd, term_list, basicdata, data):
        for term_id in classroom_id:
            course_list_info = CourseListInfo()
            ua = UserAgent()
            self.basicCourseHeader["User-Agent"] = ua.random
            # try:
            # term_r1 = requests.get("https://next.xuetangx.com/api/v1/lms/product/get_course_detail/?cid=" + str(term_id), headers=self.basicCourseHeader, timeout=40)
            # except UnicodeEncodeError:
            new_url = "https://next.xuetangx.com/api/v1/lms/product/get_course_detail/?cid=" + str(term_id)
            url_ = quote(new_url, safe=string.printable)
            term_r1 = requests.get(url_, headers=self.basicCourseHeader, timeout=40)
            time.sleep(0.3)
            if data["property"] == 1:
                continue
            if term_r1.json()["data"] == {}:
                course_list_info.url = "https://next.xuetangx.com/course/" + sign + "/" + str(term_id)
                course_list_info.course_name = data["name"]
                course_list_info.introduction = data["short_intro"].replace('\n', '').replace(
                '\xa0', '').replace("\u3000", '')
                if isquality != None:
                    course_list_info.isquality = isquality
                course_list_info.total_crowd = str(total_crowd)
                course_list_info.total_crowd_num = int(course_list_info.total_crowd)
                course_list_info.term_number = len(classroom_id)
                course_list_info.platform = "学堂在线"
                course_list_info.save_time = datetime.now()
                if data["org"] != {}:
                    course_list_info.school = data["org"]["name"]
                course_list_info.isfree = 0
                if len(data["teacher"]):
                    course_list_info.teacher = data["teacher"]["name"]
                term_r1.close()
                self.detail_info2(sign, term_id, course_list_info)
                self.detail_info3(sign, term_id, course_list_info, term_list)
                continue
            basic_data = term_r1.json()["data"]["basic_data"]
            course_list_info.url = "https://next.xuetangx.com/course/" + sign + "/" + str(term_id)
            course_list_info.course_name = basic_data["name"]
            course_list_info.introduction = basic_data["short_intro"].replace('\n', '').replace(
                '\xa0', '').replace("\u3000", '')
            if len(basic_data["teacher_list"]):
                for teacher in basic_data["teacher_list"]:
                    course_list_info.team = course_list_info.team + teacher["name"] + ","
                course_list_info.team = course_list_info.team[0:-1]
            if len(basic_data["teacher"]):
                course_list_info.teacher = basic_data["teacher"]["name"]
            elif len(basic_data["teacher_list"]):
                course_list_info.teacher = basic_data["teacher_list"][0]["name"]
            if isquality != None:
                course_list_info.isquality = isquality
            course_list_info.total_crowd = str(total_crowd)
            course_list_info.total_crowd_num = int(course_list_info.total_crowd)
            course_list_info.term_number = len(classroom_id)
            course_list_info.platform = "学堂在线"
            course_list_info.save_time = datetime.now()
            if data["org"] != {}:
                course_list_info.school = data["org"]["name"]
            course_list_info.isfree = 0
            term_r1.close()
            self.detail_info2(sign, term_id, course_list_info)
            self.detail_info3(sign, term_id, course_list_info, term_list)
            pass

    def detail_info2(self,sign, term_id, course_list_info):
            try:
                ua = UserAgent()
                self.basicCourseHeader["User-Agent"] = ua.random
                # try:
                # r3 = requests.get("https://next.xuetangx.com/api/v1/lms/product/sku_pay_detail/?cid=" + str(
                #     term_id) + "&sign=" + sign, headers=self.basicCourseHeader, timeout=30)
                # except UnicodeEncodeError:
                new_url = "https://next.xuetangx.com/api/v1/lms/product/sku_pay_detail/?cid=" + str(
                    term_id) + "&sign=" + sign
                url_ = quote(new_url, safe=string.printable)
                r3 = requests.get(url_, headers=self.basicCourseHeader, timeout=40)
                time.sleep(0.5)
                sku = r3.json()["data"]["sku_info"]
                if len(sku) == 2:
                    if sku[0]["name"] == "免费学习":
                        course_list_info.isfree = 1
                    if sku[1]["name"] == "认证学习":
                        course_list_info.certification = "有认证"
                elif len(sku) == 1:
                    if sku[0]["name"] == "免费学习":
                        course_list_info.isfree = 1
                    if sku[0]["name"] == "认证学习":
                        course_list_info.certification = "有认证"
                r3.close()
            except:
                pass

    def detail_info3(self, sign, term_id, course_list_info, term_list):
            ua = UserAgent()
            self.basicCourseHeader["User-Agent"] = ua.random
            # try:
            # r4 = requests.get("https://next.xuetangx.com/api/v1/lms/product/classroom/?sign=" + sign, headers=self.basicCourseHeader, timeout=40)
            # except UnicodeEncodeError:
            new_url = "https://next.xuetangx.com/api/v1/lms/product/classroom/?sign=" + sign
            url_ = quote(new_url, safe=string.printable)
            r4 = requests.get(url_, headers=self.basicCourseHeader, timeout=40)
            time.sleep(0.3)
            r4_current = r4.json()["data"]["current"]
            r4_history = r4.json()["data"]["history"]
            current_history = r4_current + r4_history
            for item in current_history:
                if term_id == item["classroom_id"]:
                    course_list_info.term = item["classroom_name"]
                    start_date = item["class_start"]
                    end_date = item["class_end"]
                    if end_date != None:
                        start_date2 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(start_date/1000)))
                        course_list_info.start_date = datetime.strptime(start_date2, '%Y-%m-%d %H:%M:%S')
                        end_date2 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(end_date/1000)))
                        course_list_info.end_date = datetime.strptime(end_date2, '%Y-%m-%d %H:%M:%S')
                        if course_list_info.save_time < course_list_info.start_date:
                            course_list_info.status = 2
                        elif (course_list_info.start_date < course_list_info.save_time) and (
                                course_list_info.save_time < course_list_info.end_date):
                            course_list_info.status = 1
                        elif course_list_info.save_time > course_list_info.end_date:
                            course_list_info.status = 0
                    else:
                        start_date2 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(start_date/1000)))
                        course_list_info.start_date = datetime.strptime(start_date2, '%Y-%m-%d %H:%M:%S')
                        if course_list_info.save_time < course_list_info.start_date:
                            course_list_info.status = 2
            r4.close()
            term_list.append(course_list_info.__dict__)
            pass

    @retry(stop_max_attempt_number=3)
    def deep_page(self, sign, classroom_id, isquality, basicdata):
        term_list = []
        new_url2 = "https://next.xuetangx.com/course/" + sign
        url_2 = quote(new_url2, safe=string.printable)
        self.basicCourseHeader["Referer"] = url_2
        ua = UserAgent()
        self.basicCourseHeader["User-Agent"] = ua.random
        # total_crowd_r = requests.get(
        #     "https://next.xuetangx.com/api/v1/lms/product/get_product_basic_info/?sign=" + sign,
        #     headers=self.basicCourseHeader, timeout=40)
        new_url = "https://next.xuetangx.com/api/v1/lms/product/get_product_basic_info/?sign=" + sign
        url_ = quote(new_url, safe=string.printable)
        total_crowd_r = requests.get(url_, headers=self.basicCourseHeader, timeout=40)
        total_crowd = total_crowd_r.json()["data"]["count"]
        data = total_crowd_r.json()["data"]
        if len(classroom_id) == 0:
            course_list_info = CourseListInfo()
            course_list_info.url = "https://next.xuetangx.com/course/" + sign
            course_list_info.course_name = data["name"]
            course_list_info.introduction = data["short_intro"].replace('\n', '').replace(
                '\xa0', '').replace("\u3000", '')
            course_list_info.total_crowd = str(total_crowd)
            course_list_info.total_crowd_num = int(total_crowd)
            course_list_info.term_id = -1
            course_list_info.term_number = -1
            course_list_info.platform = "学堂在线"
            # course_list_info.teacher = data["teacher"]["name"]
            # course_list_info.school = data["org"]["name"]
            course_list_info.save_time = datetime.now()
            # logging.info(course_list_info.__dict__)
            # term_list.append(course_list_info.__dict__)
            total_crowd_r.close()
            return term_list
        total_crowd_r.close()
        self.detail_info(sign, classroom_id, isquality, total_crowd, term_list, basicdata, data)
        return term_list
        pass

    def add_error_list(self, basicdata):
        for item in basicdata["classroom_id"]:
            error_course = {
                "platform": "学堂在线",
                "error_url": "https://next.xuetangx.com/course/" + basicdata["sign"] + "/" + str(item)
            }
            self.error_list.append(error_course)
        pass

    def dealLiveCourse(self, basicdata):
        course_list_info = CourseListInfo()
        course_list_info.url = "https://next.xuetangx.com/live/" + basicdata["sign"] + "/" + basicdata[
            "course_sign"] + "/" + str(basicdata["live_status"]["classroom_id"]) + "/" + str(basicdata["live_status"]["leaf_id"])
        course_list_info.course_name = basicdata["name"]
        course_list_info.isquality = 0
        if len(basicdata["teacher"]) != 0:
            course_list_info.teacher = basicdata["teacher"][0]["name"]
            for teacher in basicdata["teacher"]:
                course_list_info.team = course_list_info.team + teacher["name"] + ","
            course_list_info.team = course_list_info.team[0:-1]
        course_list_info.introduction = basicdata["short_intro"].replace('\n', '').replace(
                '\xa0', '').replace("\u3000", '')
        course_list_info.platform = "学堂在线"
        course_list_info.total_crowd = str(basicdata["count"])
        course_list_info.total_crowd_num = int(course_list_info.total_crowd)
        course_list_info.school = basicdata["org"]["name"]
        course_list_info.term_id = -1
        try:
            start_time = basicdata["sku_class_time"][0][0]
            start_time2 = re.sub(r'([\d|-]+)T([\d|:]+).+', r'\1 \2', start_time)
            end_time = basicdata["sku_class_time"][0][1]
            course_list_info.save_time = datetime.now()
            if end_time == None:
                course_list_info.start_date = datetime.strptime(start_time2, '%Y-%m-%d %H:%M:%S')
                if course_list_info.save_time < course_list_info.start_date:
                    course_list_info.status = 2
            else:
                end_time2 = re.sub(r'([\d|-]+)T([\d|:]+).+', r'\1 \2', end_time)
                course_list_info.start_date = datetime.strptime(start_time2, '%Y-%m-%d %H:%M:%S')
                course_list_info.end_date = datetime.strptime(end_time2, '%Y-%m-%d %H:%M:%S')
                if course_list_info.save_time < course_list_info.start_date:
                    course_list_info.status = 2
                elif (course_list_info.start_date < course_list_info.save_time) and (
                        course_list_info.save_time < course_list_info.end_date):
                    course_list_info.status = 1
                elif course_list_info.save_time > course_list_info.end_date:
                    course_list_info.status = 0
        except:
            course_list_info.save_time = datetime.now()
        return [course_list_info.__dict__]
        pass

    def start_thread(self, basicdata, sign, classroom_id, isquality, isLive):
        try:
            if isLive == "直播课":
                print("有直播课")
                get_course_list = self.dealLiveCourse(basicdata)
            else:
                get_course_list = self.deep_page(sign, classroom_id, isquality, basicdata)
                get_course_list = self.deal_data(get_course_list)
        except:
            self.add_error_list(basicdata)
            get_course_list = []
        self.thread_lock.acquire()
        for item in get_course_list:
            print(item)
        print("---------------------------")
        self.total_list = self.total_list + get_course_list
        self.thread_lock.release()
        pass

    def fetch_one_page(self, page, list_info):
        Redo = 0
        ua = UserAgent()
        self.ListHeaders["User-Agent"] = ua.random
        try:
            r2 = requests.post("https://next.xuetangx.com/api/v1/lms/get_product_list/?page=" + str(page),
                               headers=self.ListHeaders, data=list_info["payload"], timeout=60)
        except:
            while r2.status_code != 200:
                time.sleep(2)
                ua = UserAgent()
                self.ListHeaders["User-Agent"] = ua.random
                r2 = requests.post("https://next.xuetangx.com/api/v1/lms/get_product_list/?page=" + str(page),
                                   headers=self.ListHeaders, data=list_info["payload"], timeout=60)
                Redo = Redo + 1
                if Redo == 5:
                    list = {
                        "platform": "学堂在线",
                        "url": "https://next.xuetangx.com/api/v1/lms/get_product_list/?page=" + str(page)
                    }
                    self.error_list.append(list)
                    return []
        time.sleep(0.5)
        list = r2.json()["data"]["product_list"]
        r2.close()
        for i in range(0, len(list), 3):
            threads = []
            for x in range(0, 3):
                if (i + x) < len(list):
                    sign = list[i + x]["sign"]
                    try:
                        classroom_id = self.get_classroom_id(sign)
                    except simplejson.errors.JSONDecodeError:
                        continue
                    isquality = list[i + x]["tag_titles"]
                    isLive = list[i + x]["sell_type_name"]
                    if len(isquality) == 1:
                        if isquality[0] == "国家精品":
                            isquality = 1
                        elif len(list[i + x]["tags"]):
                            if list[i + x]["tags"][0]["title"] == "国家精品":
                                isquality = 1
                            else:
                                isquality = 0
                        else:
                            isquality = 0
                    else:
                        isquality = 0
                    t = threading.Thread(target=self.start_thread,
                                         args=(list[i + x], sign, classroom_id, isquality, isLive))
                    threads.append(t)
            for item_start in threads:
                item_start.start()
            for item_end in threads:
                item_end.join()
        pass

    def get_classroom_id(self, sign):
        ua = UserAgent()
        self.basicCourseHeader["User-Agent"] = ua.random
        new_url = "https://next.xuetangx.com/api/v1/lms/product/classroom/?sign=" + sign
        url_ = quote(new_url, safe=string.printable)
        r4 = requests.get(url_, headers=self.basicCourseHeader, timeout=40)
        time.sleep(0.3)
        r4_current = r4.json()["data"]["current"]
        r4_history = r4.json()["data"]["history"]
        current_history = r4_current + r4_history
        classroom_id = []
        for item in current_history:
            classroom_id.append(item["classroom_id"])
        r4.close()
        return classroom_id
        pass

    def get_basic_info(self):
        payload = json.dumps(
            {"query": "", "chief_org": [], "classify": [], "selling_type": [], "status": [], "appid": 10000})
        ua = UserAgent()
        self.ListHeaders["User-Agent"] = ua.random
        r = requests.post("https://next.xuetangx.com/api/v1/lms/get_product_list/?page=1", headers=self.ListHeaders,
                          data=payload)
        print(r.status_code)
        all_course = r.json()["data"]["count"]
        return_info = {"count": all_course, "url": "https://next.xuetangx.com/api/v1/lms/get_product_list/?page=",
                       "payload": payload}
        r.close()
        return return_info
        pass

    def fetch_all_page(self):
        basic_info = self.get_basic_info()
        extra = "共：" + str(basic_info["count"]) + "门课程"
        print(extra)
        if basic_info["count"] % 10 == 0:
            page_number = int(basic_info["count"] / 10)
        else:
            page_number = int(basic_info["count"] / 10) + 1
        print("共" + str(page_number) + "页")
        page = 0
        for i in range(1, page_number + 1):
            page = page + 1
            print("将爬取第:" + str(i) + "/" + str(page_number) + "页")
            self.fetch_one_page(i, basic_info)
        for item3 in self.total_list:
            item3["extra"] = extra
        pass

    def run(self):
        self.total_list = []
        logging.basicConfig(filename="xtzx.log", level=logging.INFO)
        self.fetch_all_page()
        set_list = self.distinct(self.total_list)
        dic = {
            "course_list_info": set_list,
            "error_list": self.error_list
        }
        # with open("xuetangx.pkl", "wb") as f:
        #     pickle.dump(self.total_list, f)
        # for item in dic["error_list"]:
        #     print(item)
        print(len(self.total_list))
        return dic

    def deal_data(self, course_list: List):
        normal_course = []
        experience_course = []
        data_list = []
        for item in course_list:
            if item["term"] == "体验课" or item["term"] == "体验版":
                item["term_id"] = -1
                item["term_number"] = -1
                experience_course.append(item)
            elif item["term"] == "自主学习" or \
               item["term"] == "自主模式" or \
               item["term"] == "Self-Paced":
                item["term_id"] = 0
                item["term_number"] = 0
                if item["status"] == 3:
                    item["status"] = 1
                experience_course.append(item)
            else:
                normal_course.append(item)
        for item2 in normal_course:
            item2["term_number"] = len(normal_course)
            item2["term_id"] = 1
            data_list.append(item2["start_date"])
        for x in range(0, len(data_list)):
            for y in range(0, len(data_list)-x-1):
                if data_list[y] > data_list[y+1]:
                    data_list[y], data_list[y+1] = data_list[y+1], data_list[y]
        for item3 in normal_course:
            for item4 in data_list:
                if item3["start_date"] == item4:
                    item3["term_id"] = data_list.index(item4) + 1
        return normal_course + experience_course
        pass

    # 根据url进行数据爬取
    def run_by_url(self, url):
        first_deal_url = url.replace("https://next.xuetangx.com/course/", '')
        second_deal_url = first_deal_url.split('/')
        if len(second_deal_url) == 2:
            term_id = []
            sign = second_deal_url[0]
            term_id.append(int(second_deal_url[1]))
            ua = UserAgent()
            self.basicCourseHeader["User-Agent"] = ua.random
            new_url = "https://next.xuetangx.com/api/v1/lms/product/get_course_detail/?cid=" + str(term_id[0])
            url_ = quote(new_url, safe=string.printable)
            term_r1 = requests.get(url_, headers=self.basicCourseHeader, timeout=40)
            time.sleep(0.3)
            if term_r1.json()["data"] != {}:
                basic_data = term_r1.json()["data"]["basic_data"]
                course_name = basic_data["name"]
                payload = json.dumps(
                    {"query": course_name, "chief_org": [], "classify": [], "selling_type": [], "status": [], "appid": 10000})
                self.ListHeaders["User-Agent"] = ua.random
                r2 = requests.post("https://next.xuetangx.com/api/v1/lms/get_product_list/?page=1",
                                   headers=self.ListHeaders, data=payload, timeout=60)
                course_list = r2.json()["data"]["product_list"]
                for course in course_list:
                    if course["sign"] == sign:
                        isquality = course["tag_titles"]
                        isLive = course["sell_type_name"]
                        if len(isquality) == 1:
                            if isquality[0] == "国家精品":
                                isquality = 1
                            elif len(course["tags"]):
                                if course["tags"][0]["title"] == "国家精品":
                                    isquality = 1
                                else:
                                    isquality = 0
                            else:
                                isquality = 0
                        else:
                            isquality = 0
                        self.start_thread(course, sign, term_id, isquality, isLive)
        print(self.total_list)
        print(self.error_list)
        if len(self.total_list):
            return self.total_list

    def run_by_urls(self, urls):
        total_list = []
        for url in urls:
            total_list = total_list + self.run_by_url(url)
            time.sleep(0.2)
        return total_list


if __name__ == '__main__':
    fetcher = ListFetcher()
    fetcher.run()
