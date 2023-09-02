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
        course_url = self.get_course_url()
        term_url, total_num = self.get_term_url(course_url)
        course_list = []
        error_list = []
        err_num = 0
        while len(term_url):
            temp_course_list, term_url = self.get_course_info(term_url, total_num)
            if len(temp_course_list):
                err_num = 0
                course_list = course_list + temp_course_list
            else:
                err_num = err_num + 1
                print('fail to get info : ' + str(len(term_url)))
            if err_num == 3:
                error_list = self.get_error_list(term_url)
                err_file = open(r"./icourse163_error_list.txt", "w", encoding='UTF-8')
                for item in error_list:
                    err_file.write(str(item) + '\n')
                err_file.close()
                break
        all_list = {
            'course_list_info': course_list,
            'error_list': error_list
        }
        return all_list

    def get_course_url(self):
        url_list = []
        more_text = ""
        onlineStatusNum = 0
        currentPageNum = 0
        fake_ua = UserAgent()
        while more_text != "已全部加载":
            currentPageNum = currentPageNum + 1
            url = 'http://www.icourses.cn/web//sword/portal/openSearchPage'
            data = {
                'kw': '',
                'onlineStatus': str(onlineStatusNum),
                'currentPage': str(currentPageNum),
                'catagoryId': ''
            }
            headers = {
                'Accept': 'text/html, */*; q=0.01',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
                'Content-Length': str(44 + len(str(currentPageNum))),
                'Content-Type': 'application/x-www-form-urlencoded',
                'Host': 'www.icourses.cn',
                'Origin': 'http://www.icourses.cn',
                'Referer': 'http://www.icourses.cn/imooc/',
                'User-Agent': fake_ua.random,
                'X-Requested-With': 'XMLHttpRequest'
            }
            counter = 0
            try:
                resp = requests.post(url, headers=headers, data=data, timeout=10)
            except:
                while resp.status_code != 200:
                    time.sleep(3)
                    resp = requests.post(url, headers=headers, data=data, timeout=10)
                    counter = counter + 1
                    if counter == 5:
                        break
            if counter == 5:
                continue
            whole_soup = BeautifulSoup(str(resp.text), "html.parser")
            for course_item in whole_soup.find_all("li", attrs={"class": "pull-left"}):
                item_soup = BeautifulSoup(str(course_item), "html.parser")
                course_url = item_soup.find('a').get('href')
                block_text = item_soup.find("span", attrs={"class": "icourse-desc-school pull-left"})
                block_soup = BeautifulSoup(str(block_text), "html.parser")
                block = block_soup.get_text()
                if course_url is not None and block != '':
                    new_list = [str(course_url), str(block)]
                    url_list.append(new_list)
                    print(str(new_list))
            a_sign = whole_soup.find_all("a")
            more_text = str(a_sign[len(a_sign) - 1].string)
        return url_list

    def get_term_url(self, course_url):
        url_list = []
        counter = 0
        for item in course_url:
            url = item[0]
            block = item[1]
            counter = counter + 1
            fake_ua = UserAgent()
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Host': 'www.icourse163.org',
                'Referer': 'http://www.icourses.cn/home/',
                'User-Agent': fake_ua.random,
            }
            c_counter = 0
            session = requests.session()
            session.keep_alive = False
            try:
                resp = session.get(url, headers=headers, timeout=10)
            except:
                while c_counter != 5:
                    time.sleep(3)
                    resp = requests.get(url, headers=headers, timeout=10)
                    c_counter = c_counter + 1
                    if resp.status_code == 200:
                        break
            if c_counter == 5:
                continue
            term_info = re.findall(r"window.termInfoList = (.+?);", resp.text, re.M | re.S)
            if len(term_info):
                term_id = re.findall(r'id : "(.+?)"', str(term_info[0]), re.M | re.S)
                term_num = int(len(term_id))
                for tid in term_id:
                    term_url = str(url) + '?tid=' + str(tid)
                    new_list = [str(term_url), int(term_num), str(block)]
                    print('[completed：{:.2%}] '.format(counter / int(len(course_url))) + '[current:' + str(counter) + '] '+ '[total:' + str(len(course_url)) + '] ' + str(new_list))
                    url_list.append(new_list)
        total_num = str(len(url_list))
        return url_list, total_num

    def get_course_info(self, term_url, total_num):
        course_list = []
        error_list = []
        n = 0
        err_n = 0
        fake_ua = UserAgent()
        for item in term_url:
            url = item[0]

            term_num = int(item[1])
            block = str(item[2])
            n = n + 1
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Host': 'www.icourse163.org',
                'Referer': 'http://www.icourses.cn/home/',
                'User-Agent': fake_ua.random,
            }
            session = requests.session()
            session.keep_alive = False
            try:
                resp = session.get(url, headers=headers, timeout=10)
            except:
                err_n = err_n + 1
                new_list = [str(url), int(term_num), str(block)]
                error_list.append(new_list)
                print('[fail:' + str(url) + ']')
                continue
            html = resp.text
            whole_soup = BeautifulSoup(html, "html.parser")
            try_item = whole_soup.find("span", attrs={"class": "course-title f-ib f-vam"})
            if try_item is None:
                err_n = err_n + 1
                new_list = [str(url), int(term_num), str(block)]
                error_list.append(new_list)
                print('[fail:' + str(url) + ']')
                continue
            cookie = requests.utils.dict_from_cookiejar(resp.cookies)
            csrfKey = str(cookie['NTESSTUDYSI'])
            course_list_info = CourseListInfo()
            if '#/info' in str(url):
                url = str(url).split('#/info', 1)[0]
            course_list_info.url = str(url)
            course_list_info.extra = str(total_num)
            course_list_info.term_number = int(term_num)
            course_list_info.block = str(block)
            course_list_info.course_name = str(whole_soup.find("span", attrs={"class": "course-title f-ib f-vam"}).string)
            if "tid=" in url:
                tid = url.split("tid=", 1)[1]
                term_info = re.findall(r"window.termInfoList = (.+?)];", resp.text, re.M | re.S)
                if len(term_info):
                    termid = re.findall(r'id : "(.+?)"', str(term_info[0]), re.M | re.S)
                    for k in range(len(termid)):
                        if tid == termid[k]:
                            term_id = k + 1
                            course_list_info.term_id = int(term_id)
                            course_list_info.term = "第" + str(term_id) + "次开课"
                            date_text = re.findall(r'text : "(.+?)"', str(term_info[0]), re.M | re.S)[k]
                            if "-" in date_text:
                                start_date, end_date = str(date_text).split("-", 1)
                                if "年" and "月" and "日" in start_date:
                                    year = int(start_date.split("年", 1)[0])
                                    md_text = start_date.split("年", 1)[1]
                                    month = int(md_text.split("月", 1)[0])
                                    d_text = md_text.split("月", 1)[1]
                                    date = int(d_text.split("日", 1)[0])
                                    course_list_info.start_date = datetime(year, month, date)
                                if "年" and "月" and "日" in end_date:
                                    year = int(end_date.split("年", 1)[0])
                                    md_text = end_date.split("年", 1)[1]
                                    month = int(md_text.split("月", 1)[0])
                                    d_text = md_text.split("月", 1)[1]
                                    date = int(d_text.split("日", 1)[0])
                                    course_list_info.end_date = datetime(year, month, date)
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
                                course_list_info.status = course_list_info.kStatusLaterOn
            course_list_info.save_time = datetime.now()
            course_list_info.platform = "爱课程(中国大学MOOC)"
            school_info = re.findall(r"window.schoolDto = {(.+?)}", resp.text, re.M | re.S)
            if len(school_info):
                course_list_info.school = str(re.findall(r'name:"(.+?)"', str(school_info[0]), re.M | re.S)[0])
            crowd_re = re.findall(r'enrollCount : "(.+?)"', resp.text, re.M | re.S)
            if len(crowd_re):
                crowd = crowd_re[0]
                if 'bigPhoto' in crowd:
                    err_n = err_n + 1
                    new_list = [str(url), int(term_num), str(block)]
                    error_list.append(new_list)
                    print('[fail:' + str(url) + ']')
                    continue
                else:
                    course_list_info.crowd = crowd
                    course_list_info.crowd_num = int(crowd)
            chief_teacher = re.findall(r"window.chiefLector = {(.+?)}", resp.text, re.M | re.S)
            if len(chief_teacher):
                course_list_info.teacher = str(
                    re.findall(r'lectorName : "(.+?)"', str(chief_teacher[0]), re.M | re.S)[0])
            staff_teachers = re.findall(r"window.staffLectors = (.+?)];", resp.text, re.M | re.S)
            teachers = re.findall(r'lectorName : "(.+?)"', str(staff_teachers[0]), re.M | re.S)
            team = course_list_info.teacher
            course_list_info.team = team
            if len(teachers):
                for teacher in teachers:
                    team = team + "，" + teacher
                if course_list_info.teacher == '' and "，" in team:
                    course_list_info.team = team.split("，", 1)[1]
                    course_list_info.teacher = team.split("，", 2)[1]
                else:
                    course_list_info.team = team
            categories_info = re.findall(r"window.categories = (.+?)];", resp.text, re.M | re.S)
            categories_type = re.findall(r'name : "(.+?)"', str(categories_info[0]), re.M | re.S)
            if len(categories_type) > 1:
                course_list_info.isquality = course_list_info.kIsQuality
            else:
                course_list_info.isquality = course_list_info.kNotQuality
            if len(categories_type):
                course_list_info.subject = categories_type[len(categories_type) - 1]
            if len(whole_soup.find_all("div", attrs={"class": "category-content j-cover-overflow"})):
                intro_info = whole_soup.find("div", attrs={"class": "category-content j-cover-overflow"})
                intro_soup = BeautifulSoup(str(intro_info), "html.parser")
                course_list_info.introduction = str(intro_soup.get_text())
            if len(whole_soup.find_all("span", attrs={"id": "j-tag"})):
                course_list_info.isquality = course_list_info.kIsQuality
            if len(whole_soup.find_all("div", attrs={"class": "category-content j-cover-overflow certRope"})):
                cert_info = whole_soup.find_all(
                    "div", attrs={"class": "category-content j-cover-overflow certRope"})
                cert_soup = BeautifulSoup(str(cert_info), "html.parser")
                course_list_info.certification = str(cert_soup.get_text())
            course_info = re.findall(r"window.courseDto = (.+?)];", resp.text, re.M | re.S)
            if len(course_info):
                course_id_list = re.findall(r'id:"(.+?)"', str(course_info[0]), re.M | re.S)
                if len(course_id_list):
                    course_id = course_id_list[0]
                    data = {
                        'courseId': course_id
                    }
                    s_url = 'http://www.icourse163.org/web/j/mocCourseV2RpcBean.getEvaluateAvgAndCount.rpc?csrfKey=' + csrfKey
                    try:
                        resp = session.post(s_url, data=data, timeout=10)
                    except:
                        err_n = err_n + 1
                        new_list = [str(url), int(term_num), str(block)]
                        error_list.append(new_list)
                        print('[fail:' + str(url) + ']')
                        continue
                    try:
                        mark_json = resp.json()
                    except:
                        err_n = err_n + 1
                        new_list = [str(url), int(term_num), str(block)]
                        error_list.append(new_list)
                        print('[fail:' + str(url) + ']')
                        continue
                    result_json = mark_json['result']
                    if result_json is not None:
                        if 'avgMark' in result_json:
                            course_list_info.course_score = result_json['avgMark']
            print('[completed：{:.2%}] '.format(n/int(len(term_url))) + '[current:' + str(n) + '] ' + '[total:' + str(len(term_url)) + '] ' + '[fail:' + str(err_n) + '] ' + str(course_list_info.__dict__))
            course_list.append(course_list_info.__dict__)
        return course_list, error_list

    def get_error_list(self, err_list):
        error_list = []
        for item in err_list:
            new_list = [{
                'error_url': str(item[0]),
                'platform': "爱课程(中国大学MOOC)"
            }]
            error_list = error_list + new_list
        return error_list

    def run_by_url(self, url):
        term_list = []
        error_list = []
        total_num = ''
        new_list = [str(url), 0, '']
        term_list.append(new_list)
        course_list, term_url = self.get_course_info(term_list, total_num)
        all_list = {
            'course_list_info': course_list,
            'error_list': error_list
        }
        return all_list

    def run_by_urls(self, url_list):
        course_list = []
        error_list = []
        term_url = []
        err_num = 0
        total_num = ''
        for url in url_list:
            new_list = [str(url), 0, '']
            term_url.append(new_list)
        while len(term_url):
            temp_course_list, term_url = self.get_course_info(term_url, total_num)
            if len(temp_course_list):
                err_num = 0
                course_list = course_list + temp_course_list
            else:
                err_num = err_num + 1
                print('fail to get info : ' + str(len(term_url)))
            if err_num == 3:
                error_list = self.get_error_list(term_url)
                err_file = open(r"./icourse163_url_error_list.txt", "w", encoding='UTF-8')
                for item in error_list:
                    err_file.write(str(item) + '\n')
                err_file.close()
                break
        all_list = {
            'course_list_info': course_list,
            'error_list': error_list
        }
        return all_list

