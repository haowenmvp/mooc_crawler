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
        course_id_subject = self.get_course_url()
        term_id_subject, total_num = self.get_term_url(course_id_subject)
        err_num = 0
        course_list = []
        error_list = []
        while len(term_id_subject):
            temp_course_list, term_id_subject = self.get_course_info(term_id_subject, total_num)
            if len(temp_course_list):
                err_num = 0
                course_list = course_list + temp_course_list
            else:
                err_num = err_num + 1
                print('fail to get info : ' + str(len(temp_course_list)))
            if err_num == 3:
                error_list = self.get_error_list(temp_course_list)
                err_file = open(r"./ehuixue_error_list.txt", "w", encoding='UTF-8')
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
        tis_list = []
        for url in url_list:
            tid = str(url).split('?cid=', 1)[1]
            new_list = [str(tid), '', 1]
            tis_list.append(new_list)
        err_num = 0
        course_list = []
        error_list = []
        while len(tis_list):
            temp_course_list, tis_list = self.get_course_info(tis_list, str(len(url_list)))
            if len(temp_course_list):
                err_num = 0
                course_list = course_list + temp_course_list
            else:
                err_num = err_num + 1
                print('fail to get info : ' + str(len(temp_course_list)))
            if err_num == 3:
                error_list = self.get_error_list(temp_course_list)
                err_file = open(r"./ehuixue_error_list.txt", "w", encoding='UTF-8')
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
        info_list = []
        fake_ua = UserAgent()
        request_status = 100
        page = 0
        while request_status == 100:
            page = page + 1
            url = 'http://www.ehuixue.cn/index/Orgclist/getcoursepage.html'
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Host': 'www.ehuixue.cn',
                'Origin': 'http://www.ehuixue.cn',
                'Referer': 'http://www.ehuixue.cn/index/orgclist/index.html',
                'User-Agent': fake_ua.random,
                'X-Requested-With': 'XMLHttpRequest'
            }
            data = {
                'category': '',
                'startstatus': '0',
                'page': str(page),
                'coursetype': '',
                'pagesize': '100'
            }
            resp = requests.post(url, headers=headers, data=data, timeout=10)
            resp_json = resp.json()
            request_status = int(resp_json['status'])
            if request_status == 100:
                returndata_json = resp_json['returndata']
                couselist = returndata_json['couselist']
                for item in couselist:
                    course_id = str(item['Course_ID'])
                    category = str(item['Category_Name'])
                    new_list = [course_id, category]
                    print(new_list)
                    info_list.append(new_list)
        return info_list

    def get_term_url(self, course_id_subject):
        info_list = []
        fake_ua = UserAgent()
        counter = 0
        for item in course_id_subject:
            counter = counter + 1
            url = 'http://www.ehuixue.cn/index/Orgclist/course'
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Host': 'www.ehuixue.cn',
                'Origin': 'http://www.ehuixue.cn',
                'User-Agent': fake_ua.random,
            }
            params = {
                'cid': str(item[0])
            }
            c_counter = 0
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=10)
            except:
                while c_counter != 5:
                    time.sleep(3)
                    resp = requests.get(url, headers=headers, params=params, timeout=10)
                    counter = c_counter + 1
                    if resp.status_code == 200:
                        break
            if c_counter == 5:
                continue
            html = str(resp.text)
            whole_soup = BeautifulSoup(html, "html.parser")
            term_nav = whole_soup.find("dl", attrs={"class": "term-nav-child"})
            if term_nav is not None:
                term_soup = BeautifulSoup(str(term_nav), "html.parser")
                term_a = term_soup.find_all("dd")
                for term in term_a:
                    term_num = int(len(term_a))
                    href_soup = BeautifulSoup(str(term), "html.parser")
                    term_href = str(href_soup.a['href'])
                    cid = re.search(r'cid=(.*)', str(term_href), re.M | re.S).group(1)
                    new_list = [cid, item[1], int(term_num)]
                    print("completed：{:.2%}".format(counter/int(len(course_id_subject))) + "    data：" + str(new_list))
                    info_list.append(new_list)
            else:
                new_list = [item[0], item[1], 1]
                print("completed：{:.2%}".format(counter/int(len(course_id_subject))) + "    data：" + str(new_list))
                info_list.append(new_list)
        total_num = str(len(info_list))
        return info_list, total_num

    def get_course_info(self, term_id_subject, total_num):
        course_list = []
        error_list = []
        fake_ua = UserAgent()
        counter = 0
        err_n = 0
        for item in term_id_subject:
            counter = counter + 1
            term_num = int(item[2])
            url = 'http://www.ehuixue.cn/index/Orgclist/course'
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Host': 'www.ehuixue.cn',
                'Origin': 'http://www.ehuixue.cn',
                'User-Agent': fake_ua.random,
            }
            params = {
                'cid': str(item[0])
            }
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=10)
            except:
                err_n = err_n + 1
                new_list = [str(item[0]), str(item[1]), int(item[2])]
                error_list.append(new_list)
                print('[fail:' + str(url) + ']')
                continue
            course_list_info = CourseListInfo()
            html = str(resp.text)
            whole_soup = BeautifulSoup(html, "html.parser")
            if len(whole_soup.find_all("span", attrs={"class": "cname"})):
                course_list_info.platform = "安徽省网络课程学习中心平台"
                course_list_info.url = resp.url
                if '?cid=' in course_list_info.url:
                    tid = course_list_info.url.split('?cid=', 1)[1]
                    course_list_info.platform_term_id = str(tid)
                course_list_info.course_name = str(whole_soup.find("span", attrs={"class": "cname"}).string)
                term_check = whole_soup.find("i", attrs={"id": "showTerm"})
                if term_check is not None:
                    group_info = whole_soup.find("dl", attrs={"class": "term-nav-child"})
                    group_soup = BeautifulSoup(str(group_info), "html.parser")
                    group_item = group_soup.find_all('a')
                    group_detail = str(group_item[0]['href'])
                    if '?cid=' in group_detail:
                        cid = group_detail.split('?cid=', 1)[1]
                        course_list_info.platform_course_id = str(cid)
                        group = course_list_info.platform + cid
                        group_hash = hashlib.md5(group.encode('utf-8'))
                        course_list_info.course_group_id = group_hash.hexdigest()
                    term_info = whole_soup.find("li", attrs={"class": "term-nav-item"})
                    term_soup = BeautifulSoup(str(term_info), "html.parser")
                    term = str(term_soup.find("a").string)
                    course_list_info.term = term
                    if "第" and "次" in term:
                        term_split = term.split("第", 1)[1]
                        course_list_info.term_id = int(term_split.split("次", 1)[0])
                else:
                    course_list_info.platform_course_id = str(course_list_info.url.split('?cid=', 1)[1])
                    group = course_list_info.platform + course_list_info.platform_course_id
                    group_hash = hashlib.md5(group.encode('utf-8'))
                    course_list_info.course_group_id = group_hash.hexdigest()
                course_list_info.subject = item[1]
                course_list_info.term_number = int(term_num)
                course_list_info.extra = str(total_num)
                ts_info = whole_soup.find_all("a", attrs={"class": "tename"})
                course_list_info.teacher = str(ts_info[0].string.strip())
                course_list_info.school = str(ts_info[1].string.strip())
                time_info = whole_soup.find_all("span", attrs={"class": "cteacher"})[3].string.strip()
                crowd_info = whole_soup.find_all("span", attrs={"class": "cteacher"})[4].string.strip()
                if "至" in time_info:
                    start_date_info, end_date_info = time_info.split("至", 1)
                    if "-" in start_date_info:
                        year, month, date = start_date_info.split("-", 2)
                        course_list_info.start_date = datetime(int(year), int(month), int(date))
                    if "-" in end_date_info:
                        year, month, date = end_date_info.split("-", 2)
                        course_list_info.end_date = datetime(int(year), int(month), int(date))
                course_list_info.save_time = datetime.now()
                if "：" in crowd_info:
                    crowd_split = crowd_info.split("：", 1)[1]
                    if "人" in crowd_split:
                        crowd = crowd_split.split("人", 1)[0]
                        course_list_info.crowd = crowd
                        course_list_info.crowd_num = int(crowd)
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
                    course_list_info.status = -1
                intro_info = whole_soup.find("div", attrs={"class": "layui-tab-item layui-show"})
                intro_soup = BeautifulSoup(str(intro_info), "html.parser")
                course_list_info.introduction = intro_soup.get_text()
                team_info = whole_soup.find("div", attrs={"class": "layui-tab-item dianming"})
                team_soup = BeautifulSoup(str(team_info), "html.parser")
                team_item = team_soup.find_all("ul")
                team = ''
                for n in range(0, len(team_item)):
                    item_soup = BeautifulSoup(str(team_item[n]), "html.parser")
                    item_text = item_soup.find("a").string
                    teacher = item_text.split("\xa0", 1)[0].strip()
                    team = team + "，" + teacher
                if "，" in team:
                    course_list_info.team = team.split("，", 1)[1].strip()
                else:
                    course_list_info.team = course_list_info.teacher
                print('[completed：{:.2%}] '.format(counter/int(len(term_id_subject))) + '[current:' + str(counter) + '] ' + '[total:' + str(len(term_id_subject)) + '] ' + str(course_list_info.__dict__))
                course_list.append(course_list_info.__dict__)
        return course_list, error_list

    def get_error_list(self, err_list):
        error_list = []
        url = 'http://www.ehuixue.cn/index/Orgclist/course?cid='
        for item in err_list:
            new_list = [{
                'error_url': url + str(item[0]),
                'platform': "安徽省网络课程学习中心平台"
            }]
            error_list = error_list + new_list
        return error_list

