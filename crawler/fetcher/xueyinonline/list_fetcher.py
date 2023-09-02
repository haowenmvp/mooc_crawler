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
        page_number = self.get_page_number()
        course_url = self.get_course_url(page_number)
        term_url, total_num = self.get_term_url(course_url)
        course_list = []
        error_list = []
        err_num = 0
        while len(term_url):
            temp_course_list, term_url = self.get_course_info(term_url, total_num)
            if len(temp_course_list):
                course_list = course_list + temp_course_list
                err_num = 0
            else:
                err_num = err_num + 1
                print('fail to get info : ' + str(len(term_url)))
            if err_num == 3:
                error_list = self.get_error_list(term_url)
                err_file = open(r"./xueyinonline_error_list.txt", "w", encoding='UTF-8')
                for item in error_list:
                    err_file.write(str(item) + '\n')
                err_file.close()
                break
        all_list = {
            'course_list_info': course_list,
            'error_list': error_list
        }
        return all_list

    def run_by_urls(self, url_list):
        term_url = []
        for url in url_list:
            new_list = [str(url), 1]
            term_url.append(new_list)
        course_list = []
        error_list = []
        err_num = 0
        while len(term_url):
            temp_course_list, term_url = self.get_course_info(term_url, str(len(url_list)))
            if len(temp_course_list):
                course_list = course_list + temp_course_list
                err_num = 0
            else:
                err_num = err_num + 1
                print('fail to get info : ' + str(len(term_url)))
            if err_num == 3:
                error_list = self.get_error_list(term_url)
                err_file = open(r"./xueyinonline_error_list.txt", "w", encoding='UTF-8')
                for item in error_list:
                    err_file.write(str(item) + '\n')
                err_file.close()
                break
        all_list = {
            'course_list_info': course_list,
            'error_list': error_list
        }
        return all_list

    def get_page_number(self):
        page_number = []
        page = 1
        url = 'http://www.xueyinonline.com/portal/jinke/more'
        for type_num in range(0, 4):
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
                'Host': 'www.xueyinonline.com',
                'Referer': 'http://www.xueyinonline.com/portal/jinke/more?type=' + str(type_num) + '&page=' + str(page),
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
                'Upgrade-Insecure-Requests': '1'
            }
            params = {
                'type': str(type_num),
                'page': str(page)
            }
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            page_num = re.findall(r"pageNum: (.+?),", resp.text, re.M | re.S)[0]
            page_number.append(page_num)
        return page_number

    def get_course_url(self, page_number):
        url_list = []
        url = 'http://www.xueyinonline.com/portal/jinke/more'
        block_list = ["学银慕课", "国家级金课", "省级金课", "校级金课"]
        for type_num in range(0, 4):
            for page_num in range(1, int(page_number[type_num]) + 1):
                headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'Accept-Encoding': 'gzip, deflate',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Connection': 'keep-alive',
                    'Host': 'www.xueyinonline.com',
                    'Referer': 'http://www.xueyinonline.com/portal/jinke/more?type=' + str(type_num) + '&page=' + str(page_num),
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
                    'Upgrade-Insecure-Requests': '1'
                }
                params = {
                    'type': str(type_num),
                    'page': str(page_num)
                }
                counter = 0
                try:
                    resp = requests.get(url, headers=headers, params=params, timeout=10)
                except:
                    while resp.status_code != 200:
                        time.sleep(3)
                        resp = requests.get(url, headers=headers, params=params, timeout=10)
                        counter = counter + 1
                        if counter == 5:
                            break
                if counter == 5:
                    continue
                whole_soup = BeautifulSoup(str(resp.text), "html.parser")
                for course_item in whole_soup.find_all("li", attrs={"class": "vocat_row"}):
                    item_soup = BeautifulSoup(str(course_item), "html.parser")
                    course_href = item_soup.find('a').get('href')
                    if course_href:
                        course_url = 'http://www.xueyinonline.com' + str(course_href)
                        print('get course url: [type: ' + str(type_num) + ', page: ' + str(page_num) + ', url: ' + course_url + ']')
                        url_list.append(course_url)
        return url_list

    def get_term_url(self, course_url):
        url_list = []
        course_counter = 0
        total_num = str(len(course_url))
        for url in course_url:
            course_counter = course_counter + 1
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
                'Host': 'www.xueyinonline.com',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
                'Upgrade-Insecure-Requests': '1'
            }
            counter = 0
            try:
                resp = requests.get(url, headers=headers, timeout=10)
            except:
                while resp.status_code != 200:
                    time.sleep(3)
                    resp = requests.get(url, headers=headers, timeout=10)
                    counter = counter + 1
                    if counter == 5:
                        break
            if counter == 5:
                continue
            whole_soup = BeautifulSoup(str(resp.text), "html.parser")
            mg_term_text = whole_soup.find("span", attrs={"class": "mgCard_dijiqi_con"})
            if mg_term_text is not None:
                term_soup = BeautifulSoup(str(mg_term_text), "html.parser")
                href_items = term_soup.find_all('a')
                if len(href_items):
                    for item in href_items:
                        term_href = item.get('href')
                        term_url = 'http://www.xueyinonline.com' + str(term_href)
                        term_num = int(len(href_items))
                        new_list = [str(term_url), int(term_num)]
                        url_list.append(new_list)
                        print('get term url: [course num: ' + str(course_counter) + ', total num: ' + total_num + ', url: ' + term_url + ']')
                else:
                    term_url = resp.url
                    term_num = 1
                    new_list = [str(term_url), int(term_num)]
                    url_list.append(new_list)
                    print('get term url: [course num: ' + str(course_counter) + ', total num: ' + total_num + ', url: ' + term_url + ']')
            else:
                mk_term_text = whole_soup.find("span", attrs={"class": "mkjoin_dijiqi_con"})
                if mk_term_text is not None:
                    term_soup = BeautifulSoup(str(mk_term_text), "html.parser")
                    href_items = term_soup.find_all('a')
                    if len(href_items):
                        for item in href_items:
                            term_href = item.get('href')
                            term_url = 'http://www.xueyinonline.com' + str(term_href)
                            term_num = int(len(href_items))
                            new_list = [str(term_url), int(term_num)]
                            url_list.append(new_list)
                            print('get term url: [course num: ' + str(course_counter) + ', total num: ' + total_num + ', url: ' + term_url + ']')
                    else:
                        term_url = resp.url
                        term_num = 1
                        new_list = [str(term_url), int(term_num)]
                        url_list.append(new_list)
                        print('get term url: [course num: ' + str(course_counter) + ', total num: ' + total_num + ', url: ' + term_url + ']')
        total_num = str(len(url_list))
        return url_list, total_num

    def get_course_info(self, term_url, total_num):
        course_list = []
        err_list = []
        term_counter = 0
        err_n = 0
        ua = UserAgent()
        for t_item in term_url:
            url = str(t_item[0])
            term_num = int(t_item[1])
            term_counter = term_counter + 1
            courseId = ''
            orgCourseId = ''
            jinkeType = ''
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                'Host': 'www.xueyinonline.com',
                'User-Agent': ua.random,
                'Upgrade-Insecure-Requests': '1',
                'Referer': 'http://www.xueyinonline.com/mooc/course',
            }
            try:
                resp = requests.get(url, headers=headers, timeout=10)
            except:
                err_n = err_n + 1
                new_list = [str(url), int(term_num)]
                err_list.append(new_list)
                print('[fail:' + str(url) + ']')
                continue
            course_list_info = CourseListInfo()
            course_list_info.term_number = int(term_num)
            course_list_info.extra = str(total_num)
            whole_soup = BeautifulSoup(str(resp.text), "html.parser")
            mg_term_text = whole_soup.find("span", attrs={"class": "mgCard_dijiqi_con"})
            if mg_term_text is not None:
                course_list_info.url = resp.url
                course_list_info.platform = "北京学银在线教育科技有限公司"
                if 'detail/' in course_list_info.url:
                    tid = course_list_info.url.split('detail/', 1)[1]
                    course_list_info.platform_term_id = str(tid)
                mg_term_soup = BeautifulSoup(str(mg_term_text), "html.parser")
                term_item = mg_term_soup.find_all('a')
                if len(term_item):
                    group_info = str(term_item[0]['href'])
                    if 'detail/' in group_info:
                        cid = group_info.split('detail/', 1)[1]
                        course_list_info.platform_course_id = str(cid)
                        group = course_list_info.platform + cid
                        group_hash = hashlib.md5(group.encode('utf-8'))
                        course_list_info.course_group_id = group_hash.hexdigest()
                else:
                    course_list_info.platform_course_id = course_list_info.url.split('detail/', 1)[1]
                    group = course_list_info.platform + course_list_info.platform_course_id
                    group_hash = hashlib.md5(group.encode('utf-8'))
                    course_list_info.course_group_id = group_hash.hexdigest()
                    course_list_info.valid = 0
                mg_deta = whole_soup.find("dl", attrs={"class": "mgCard_deta"})
                mg_soup = BeautifulSoup(str(mg_deta), "html.parser")
                course_list_info.course_name = str(mg_soup.find("dt").string).strip()
                ts_text = str(mg_soup.find("dd", attrs={"class": "mgCard_deta_yi"}).string)
                if "：" and " " in ts_text:
                    ts_spilt = ts_text.split("：", 1)[1]
                    course_list_info.teacher = ts_spilt.split(" ", 1)[0].strip()
                if "/" in ts_text:
                    course_list_info.school = ts_text.split("/")[-1].strip()
                term_text = mg_soup.find("a", attrs={"class": "mgCard_dijiqi_name"})
                term_soup = BeautifulSoup(str(term_text), "html.parser")
                term = term_soup.get_text()
                if term is not None:
                    course_list_info.term = str(term).strip()
                    if "第" and "期" in term:
                        term_split = term.split("第", 1)[1]
                        course_list_info.term_id = int(term_split.split("期", 1)[0].strip())
                fl_text = mg_soup.find_all("span", attrs={"class": "fl"})
                for item in fl_text:
                    if "起止日期" in str(item):
                        time_text = item.get_text()
                        if "：" and "至" in str(time_text):
                            all_time_text = str(time_text).split("：", 1)[1]
                            start_date, end_date = all_time_text.split("至", 1)
                            if "-" in start_date:
                                year, month, date = start_date.split("-", 2)
                                course_list_info.start_date = datetime(int(year), int(month), int(date))
                            if "-" in end_date:
                                year, month, date = end_date.split("-", 2)
                                course_list_info.end_date = datetime(int(year), int(month), int(date))
                            course_list_info.save_time = datetime.now()
                            if course_list_info.start_date != datetime(1999, 1, 1) and course_list_info.end_date != datetime(1999, 1, 1):
                                start = course_list_info.start_date
                                end = course_list_info.end_date
                                if start < datetime.now() < end:
                                    course_list_info.status = course_list_info.kStatusOn
                                elif datetime.now() < start:
                                    course_list_info.status = course_list_info.kStatusLaterOn
                                elif end < datetime.now():
                                    course_list_info.status = course_list_info.kStatusEnd
                        break
                intro_info = whole_soup.find("div", attrs={"class": "mgCard_text"})
                intro_soup = BeautifulSoup(str(intro_info), "html.parser")
                intro = intro_soup.get_text()
                course_list_info.introduction = "".join(intro.split())
            else:
                mk_term_text = whole_soup.find("span", attrs={"class": "mkjoin_dijiqi_con"})
                if mk_term_text is not None:
                    course_list_info.url = resp.url
                    print(course_list_info.url)
                    course_list_info.platform = "北京学银在线教育科技有限公司"
                    if 'detail/' in course_list_info.url:
                        tid = course_list_info.url.split('detail/', 1)[1]
                        course_list_info.platform_term_id = str(tid)
                    mk_term_soup = BeautifulSoup(str(mk_term_text), "html.parser")
                    term_item = mk_term_soup.find_all('a')
                    if len(term_item):
                        group_info = str(term_item[0]['href'])
                        if 'detail/' in group_info:
                            cid = group_info.split('detail/', 1)[1]
                            course_list_info.platform_course_id = str(cid)
                            group = course_list_info.platform + cid
                            group_hash = hashlib.md5(group.encode('utf-8'))
                            course_list_info.course_group_id = group_hash.hexdigest()
                    else:
                        course_list_info.platform_course_id = course_list_info.url.split('detail/', 1)[1]
                        group = course_list_info.platform + course_list_info.platform_course_id
                        group_hash = hashlib.md5(group.encode('utf-8'))
                        course_list_info.course_group_id = group_hash.hexdigest()
                        course_list_info.valid = 0
                    course_list_info.course_name = whole_soup.find("h3").get_text().strip()
                    jkCard_text = whole_soup.find("ul", attrs={"class": "jkCard_introduce"})
                    jkCard_soup = BeautifulSoup(str(jkCard_text), "html.parser")
                    school_text = jkCard_soup.find("li", attrs={"class": "jkCard_school fl"}).get_text()
                    if "：" in str(school_text):
                        course_list_info.school = str(school_text).split("：", 1)[1]
                    teacher_text = jkCard_soup.find("li", attrs={"class": "jkCard_speaker fl"}).get_text()
                    if "：" in str(teacher_text):
                        course_list_info.teacher = str(teacher_text).split("：", 1)[1]
                    term_text = jkCard_soup.find("a", attrs={"class": "mkjoin_dijiqi_name"})
                    term_soup = BeautifulSoup(str(term_text), "html.parser")
                    term = term_soup.get_text()
                    if term is not None:
                        course_list_info.term = str(term).strip()
                        if "第" and "期" in term:
                            term_split = term.split("第", 1)[1]
                            course_list_info.term_id = int(term_split.split("期", 1)[0].strip())
                    fl_text = jkCard_soup.find_all("li", attrs={"class": "fl"})
                    time_text = fl_text[int(len(fl_text)) - 1].get_text()
                    if "：" and "至" in time_text:
                        all_time_text = time_text.split("：", 1)[1]
                        start_date, end_date = all_time_text.split("至", 1)
                        if "-" in start_date:
                            year, month, date = start_date.strip().split("-", 2)
                            course_list_info.start_date = datetime(int(year), int(month), int(date))
                        if "-" in end_date:
                            year, month, date = end_date.strip().split("-", 2)
                            course_list_info.end_date = datetime(int(year), int(month), int(date))
                        course_list_info.save_time = datetime.now()
                        if course_list_info.start_date != datetime(1999, 1, 1) and course_list_info.end_date != datetime(1999, 1, 1):
                            start = course_list_info.start_date
                            end = course_list_info.end_date
                            if start < datetime.now() < end:
                                course_list_info.status = course_list_info.kStatusOn
                            elif datetime.now() < start:
                                course_list_info.status = course_list_info.kStatusLaterOn
                            elif end < datetime.now():
                                course_list_info.status = course_list_info.kStatusEnd
                    intro_info = whole_soup.find("div", attrs={"class": "jkDeta_ban_text"})
                    intro_soup = BeautifulSoup(str(intro_info), "html.parser")
                    intro = intro_soup.get_text()
                    course_list_info.introduction = "".join(intro.split())
                else:
                    err_n = err_n + 1
                    new_list = [str(url), int(term_num)]
                    err_list.append(new_list)
                    print('[fail:' + str(url) + ']')
                    continue
            intro_data = re.findall(r"getCourseIntroduce[(][)] {(.+?)}[)]", resp.text, re.M | re.S)
            if len(intro_data):
                data_text = re.findall(r"data: {(.+?)}", intro_data[0], re.M | re.S)
                if len(data_text):
                    courseId_text = re.findall(r"courseId: '(.+?)'", intro_data[0], re.M | re.S)
                    if len(courseId_text):
                        courseId = courseId_text[0]
                    orgCourseId_text = re.findall(r"orgCourseId: '(.+?)'", intro_data[0], re.M | re.S)
                    if len(orgCourseId_text):
                        orgCourseId = orgCourseId_text[0]
                    jinkeType_text = re.findall(r"jinkeType : '(.+?)'", intro_data[0], re.M | re.S)
                    if len(jinkeType_text):
                        jinkeType = jinkeType_text[0]
            if courseId != '' and orgCourseId != '' and jinkeType != '':
                intro_url = 'http://www.xueyinonline.com/detail/introduce'
                headers = {
                    'Accept': 'text/html, */*; q=0.01',
                    'Accept-Encoding': 'gzip, deflate',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Connection': 'keep-alive',
                    'Host': 'www.xueyinonline.com',
                    'User-Agent': ua.random,
                    'X-Requested-With': 'XMLHttpRequest',
                    'Referer': url,
                }
                params = {
                    'courseId': courseId,
                    'orgCourseId': orgCourseId,
                    'jinkeType': jinkeType,
                }
                try:
                    resp = requests.get(intro_url, headers=headers, params=params, timeout=10)
                except:
                    err_n = err_n + 1
                    new_list = [str(url), int(term_num)]
                    err_list.append(new_list)
                    print('[fail:' + str(url) + ']')
                    continue
                whole_soup = BeautifulSoup(str(resp.text), "html.parser")
                team_ts = whole_soup.find_all('dt')
                team = ''
                for team_item in team_ts:
                    item_text = team_item.get_text()
                    if ' ' in item_text:
                        team_teacher = str(item_text).split(' ', 1)[0]
                        team = team + "，" + team_teacher
                if "，" in team:
                    course_list_info.team = team.split("，", 1)[1]
                else:
                    course_list_info.team = course_list_info.teacher
            if courseId != '':
                data_url = 'http://www.xueyinonline.com/statistics/api/stattistics-data'
                headers = {
                    'Accept': 'text/html, */*; q=0.01',
                    'Accept-Encoding': 'gzip, deflate',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Connection': 'keep-alive',
                    'Host': 'www.xueyinonline.com',
                    'User-Agent': ua.random,
                    'X-Requested-With': 'XMLHttpRequest',
                    'Referer': url,
                }
                params = {
                    'courseId': courseId,
                }
                try:
                    resp = requests.get(data_url, headers=headers, params=params, timeout=10)
                except:
                    err_n = err_n + 1
                    new_list = [str(url), int(term_num)]
                    err_list.append(new_list)
                    print('[fail:' + str(url) + ']')
                    continue
                try:
                    data_json = resp.json()
                except:
                    err_n = err_n + 1
                    new_list = [str(url), int(term_num)]
                    err_list.append(new_list)
                    print('[fail:' + str(url) + ']')
                    continue
                if 'chooseCourseCount' in data_json:
                    course_list_info.total_crowd = data_json['chooseCourseCount']
                    course_list_info.total_crowd_num = int(data_json['chooseCourseCount'])
                if 'viewTimes' in data_json:
                    course_list_info.clicked = data_json['viewTimes']
            print('[completed：{:.2%}] '.format(term_counter/int(len(term_url))) + '[current:' + str(term_counter) + '] ' + '[total:' + str(len(term_url)) + '] ' + '[fail:' + str(err_n) + '] ' + str(course_list_info.__dict__))
            course_list.append(course_list_info.__dict__)
        return course_list, err_list

    def get_error_list(self, err_list):
        error_list = []
        for item in err_list:
            new_list = [{
                'error_url': str(item[0]),
                'platform': "北京学银在线教育科技有限公司"
            }]
            error_list = error_list + new_list
        return error_list
