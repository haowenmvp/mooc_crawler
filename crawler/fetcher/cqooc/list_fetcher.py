import requests
import logging
import time
import uuid
import re
import hashlib
from persistence.model.basic_info import CourseListInfo
from ..base_fetcher import BaseFetcher
from selenium.webdriver.support.wait import WebDriverWait
from datetime import datetime
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import selenium.common.exceptions


class ListFetcher(BaseFetcher):

    def run(self):
        course_id = self.get_course_id()
        term_id, course_list = self.get_term_id(course_id)
        error_list = []
        all_list = {
            'course_list_info': course_list,
            'error_list': error_list
        }
        return all_list

    def run_by_urls(self, url_list):
        id_list = []
        for url in url_list:
            tid = str(url).split('?id=', 1)[1]
            id_list.append(tid)
        term_id, course_list = self.get_term_id(id_list)
        error_list = []
        all_list = {
            'course_list_info': course_list,
            'error_list': error_list
        }
        return all_list

    def get_course_id(self):
        id_list = []
        fake_ua = UserAgent()
        for edu_level in range(1, 3):
            limit = 100
            start = 1
            total = 2
            while total >= start:
                url = 'http://www.cqooc.com/json/courses'
                headers = {
                    'Accept': '*/*',
                    'Accept-Encoding': 'gzip, deflate',
                    'Host': 'www.cqooc.com',
                    'User-Agent': fake_ua.random,
                    'Referer': 'http://www.cqooc.com/course',
                }
                params = {
                    'select': 'id,title,courseType,openType,viewNum,sNum,startDate,endDate,school,courseManager,courseBasicTypeDisplay,subject3Display,belongMajor,brief,isExcellent,schema,coursePicUrl,editStatus',
                    'schoolLevel': '2',
                    'status': '1',
                    'limit': str(limit),
                    'sortby': 'pubSeq',
                    'reverse': 'true',
                    'eduLevel': str(edu_level),
                    'start': str(start),
                    'ts': str(int(time.mktime(datetime.now().timetuple())*1000))
                }
                c_counter = 0
                session = requests.session()
                session.keep_alive = False
                try:
                    resp = session.get(url, headers=headers, params=params, timeout=10)
                except:
                    while c_counter != 5:
                        time.sleep(3)
                        resp = session.get(url, headers=headers, params=params, timeout=10)
                        c_counter = c_counter + 1
                        if resp.status_code == 200:
                            break
                if c_counter == 5:
                    continue
                resp_json = resp.json()
                meta_json = resp_json['meta']
                data_json = resp_json['data']
                total = int(meta_json['total'])
                start = start + limit
                for item_json in data_json:
                    id_list.append(str(item_json['id']))
                print('[get data:' + str(len(id_list)) + ']')
        return id_list

    def get_term_id(self, course_id):
        id_list = []
        course_list = []
        fake_ua = UserAgent()
        counter = 0
        term = 1
        for item in course_id:
            counter = counter + 1
            url = 'http://www.cqooc.com/json/course'
            headers = {
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate',
                'Host': 'www.cqooc.com',
                'User-Agent': fake_ua.random,
            }
            params = {
                'id': str(item),
                'ts': str(int(time.mktime(datetime.now().timetuple()) * 1000))
            }
            c_counter = 0
            session = requests.session()
            session.keep_alive = False
            try:
                resp = session.get(url, headers=headers, params=params, timeout=10)
            except:
                while c_counter != 5:
                    time.sleep(3)
                    resp = session.get(url, headers=headers, params=params, timeout=10)
                    c_counter = c_counter + 1
                    if resp.status_code == 200:
                        break
            if c_counter == 5:
                continue
            resp_json = resp.json()
            total_term_num = 1
            if 'isCopy' in resp_json:
                is_copy = int(resp_json['isCopy'])
                if is_copy == 1:
                    copy_id = str(resp_json['copyId'])
                    new_list = [str(copy_id), '1']
                    id_list.append(new_list)
                    url = 'http://www.cqooc.com/json/courses'
                    headers = {
                        'Accept': '*/*',
                        'Accept-Encoding': 'gzip, deflate',
                        'Host': 'www.cqooc.com',
                        'User-Agent': fake_ua.random,
                        'Referer': 'http://www.cqooc.com/course',
                    }
                    params = {
                        'sortby': 'id',
                        'copyId': str(copy_id),
                        'select': 'id',
                        'limit': '99',
                        'status': '1',
                        'ts': str(int(time.mktime(datetime.now().timetuple()) * 1000))
                    }
                    c_counter = 0
                    session = requests.session()
                    session.keep_alive = False
                    try:
                        term_resp = session.get(url, headers=headers, params=params, timeout=10)
                    except:
                        while c_counter != 5:
                            time.sleep(3)
                            term_resp = session.get(url, headers=headers, params=params, timeout=10)
                            c_counter = c_counter + 1
                            if term_resp.status_code == 200:
                                break
                    if c_counter == 5:
                        continue
                    term_resp_json = term_resp.json()
                    if 'data' in term_resp_json:
                        data_json = term_resp_json['data']
                        term_num = 1
                        for term_item in data_json:
                            total_term_num = int(len(data_json)) + 1
                            term_num = term_num + 1
                            if 'id' in term_item:
                                term_id = str(term_item['id'])
                                if term_id == str(item):
                                    term = str(term_num)
                                else:
                                    new_list = [str(term_id), str(term_num)]
                                    id_list.append(new_list)
                else:
                    term = '1'
                    total_term_num = 1
                    copy_id = str(item)
                course_info = self.get_course_info(resp_json, term, total_term_num, copy_id)
                course_list.append(course_info)
            print('[stage1][completed:{:.2%}]'.format(counter/int(len(course_id))) + '[current:' + str(counter) + '][total:' + str(len(course_id)) + ']' + str(course_info))
        return id_list, course_list

    def get_course_info(self, json, term_num, total_term_num, copy_id):
        course_list_info = CourseListInfo()
        course_list_info.platform = "重庆高校在线开放课程平台（重庆市教育委员会）"
        course_list_info.platform_course_id = str(copy_id)
        group = course_list_info.platform + course_list_info.platform_course_id
        group_hash = hashlib.md5(group.encode('utf-8'))
        course_list_info.course_group_id = group_hash.hexdigest()
        course_list_info.save_time = datetime.now()
        course_list_info.term_number = int(total_term_num)
        if term_num != '':
            course_list_info.term_id = str(term_num)
            course_list_info.term = "第" + str(term_num) + "次开课"
        if 'id' in json:
            cid = json['id']
            if cid is not None:
                course_list_info.url = 'http://www.cqooc.com/course/online/detail?id=' + str(cid)
                course_list_info.team = self.get_team_info(cid)
                course_list_info.course_score = self.get_score_info(cid)
                course_list_info.platform_term_id = str(cid)
        if 'title' in json:
            title = json['title']
            if title is not None:
                course_list_info.course_name = str(title)
        if 'school' in json:
            school = json['school']
            if school is not None:
                course_list_info.school = str(school)
        if 'courseManager' in json:
            teacher = json['courseManager']
            if teacher is not None:
                course_list_info.teacher = str(teacher)
        if 'startDate' in json:
            timeStamp = json['startDate']
            if timeStamp is not None:
                course_list_info.start_date = datetime.fromtimestamp(int(timeStamp)/1000.0)
        if 'endDate' in json:
            end_timeStamp = json['endDate']
            if end_timeStamp is not None:
                course_list_info.end_date = datetime.fromtimestamp(int(end_timeStamp)/1000.0)
        if course_list_info.start_date != datetime(1999, 1, 1) and course_list_info.end_date != datetime(1999, 1, 1):
            start = course_list_info.start_date
            end = course_list_info.end_date
            if start < datetime.now() < end:
                course_list_info.status = course_list_info.kStatusOn
            elif datetime.now() < start:
                course_list_info.status = course_list_info.kStatusLaterOn
            elif end < datetime.now():
                course_list_info.status = course_list_info.kStatusEnd
        else:
            course_list_info.status = course_list_info.kStatusOn
        if 'sNum' in json:
            s_num = str(json['sNum'])
            if s_num != 'None':
                course_list_info.crowd = str(s_num)
                course_list_info.crowd_num = int(s_num)
            else:
                course_list_info.crowd = str("0")
                course_list_info.crowd_num = int(0)
        if 'viewNum' in json:
            view_num = str(json['viewNum'])
            if view_num is not None:
                course_list_info.clicked = str(view_num)
        if 'isNatExc' in json:
            tag = json['isNatExc']
            if tag is not None:
                if int(tag) == 1:
                    course_list_info.isquality = course_list_info.kIsQuality
                else:
                    course_list_info.isquality = course_list_info.kNotQuality
        if 'subjectDisplay' in json:
            course_list_info.subject = str(json['subjectDisplay'])
        if 'brief' in json:
            course_list_info.introduction = str(json['brief'])
        return course_list_info.__dict__

    def get_course_more(self, term):
        course_list = []
        fake_ua = UserAgent()
        counter = 0
        for item in term:
            counter = counter + 1
            url = 'http://www.cqooc.com/json/course'
            headers = {
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate',
                'Host': 'www.cqooc.com',
                'User-Agent': fake_ua.random,
            }
            params = {
                'id': str(item[0]),
                'ts': str(int(time.mktime(datetime.now().timetuple()) * 1000))
            }
            c_counter = 0
            session = requests.session()
            session.keep_alive = False
            try:
                resp = session.get(url, headers=headers, params=params, timeout=10)
            except:
                while c_counter != 5:
                    time.sleep(3)
                    resp = session.get(url, headers=headers, params=params, timeout=10)
                    c_counter = c_counter + 1
                    if resp.status_code == 200:
                        break
            if c_counter == 5:
                continue
            resp_json = resp.json()
            course_info = self.get_course_info(resp_json, item[1])
            course_list.append(course_info)
            print('[stage2][completed:{:.2%}]'.format(counter / int(len(term))) + '[current:' + str(counter) + '][total:' + str(len(term)) + ']' + str(course_info))
        return course_list

    def get_team_info(self, pid):
        team = ''
        fake_ua = UserAgent()
        url = 'http://www.cqooc.com/json/teams'
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.cqooc.com',
            'User-Agent': fake_ua.random,
        }
        params = {
            'sortby:': 'sortNum',
            'pid': str(pid),
            'limit': '99',
            'ts': str(int(time.mktime(datetime.now().timetuple()) * 1000))
        }
        c_counter = 0
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=10)
        except:
            while c_counter != 5:
                time.sleep(3)
                resp = requests.get(url, headers=headers, params=params, timeout=10)
                c_counter = c_counter + 1
                if resp.status_code == 200:
                    break
        if c_counter == 5:
            return team
        resp_json = resp.json()
        if 'data' in resp_json:
            data_json = resp_json['data']
            for item in data_json:
                if 'name' in item:
                    team_item = item['name']
                    if team_item is not None:
                        team = team + "，" + str(team_item)
        if "，" in team:
            team = team.split("，", 1)[1]
        return team

    def get_score_info(self, cid):
        score = ''
        # TODO
        return score

