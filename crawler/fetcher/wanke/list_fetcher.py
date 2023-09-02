import requests
import time
from persistence.model.basic_info import CourseListInfo
from ..base_fetcher import BaseFetcher
from datetime import datetime
from bs4 import BeautifulSoup



class ListFetcher(BaseFetcher):

    def run(self):
        course_list = self.get_course_info()
        url_error = []
        result = {
            "course_list_info": course_list,
            "error_list": url_error
        }
        return result

    def get_course_info(self):
        course_list = []
        url = 'http://www.wanke001.com/course/getList'
        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Host': 'www.wanke001.com',
            'Origin': 'http://www.wanke001.com',
            'Referer': 'http://www.wanke001.com/courseList',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }
        data = {
            'courseName': '',
            'courseType': '-1',
            'clientType': '1'
        }
        resp = requests.post(url, headers=headers, data=data, timeout=10)
        resp_json = resp.json()
        rows_json = resp_json['rows']
        mooc_json = rows_json['teacherCourseClassList']
        course_json = rows_json['courseList']
        for mooc_item in mooc_json:
            course_id = str(mooc_item['courseID'])
            class_id = str(mooc_item['classID'])
            school = ''
            if 'partnerName' in mooc_item:
                p_name = mooc_item['partnerName']
                if p_name is not None:
                    school = str(p_name)
            mooc_list = self.get_mooc_term(str(course_id), str(class_id), school)
            course_list = course_list + mooc_list
        for c_item in course_json:
            c_id = str(c_item['courseId'])
            c_list = self.get_course_detail(c_id)
            course_list = course_list + c_list
        return course_list

    def get_course_detail(self, c_id):
        course_list = []
        url = 'http://www.wanke001.com/courseDetail/' + str(c_id)
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'max-age=0',
            'Host': 'www.wanke001.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
        }
        c_counter = 0
        try:
            resp = requests.get(url, headers=headers, timeout=10)
        except:
            while c_counter != 5:
                time.sleep(3)
                resp = requests.get(url, headers=headers, timeout=10)
                c_counter = c_counter + 1
                if resp.status_code == 200:
                    break
        if c_counter == 5:
            print("fail:" + str(url))
            return course_list
        html = resp.text
        whole_soup = BeautifulSoup(html, "html.parser")
        course_list_info = CourseListInfo()
        course_list_info.url = str(url)
        course_list_info.platform = "玩课网"
        course_list_info.school = ""
        course_list_info.save_time = datetime.now()
        course_list_info.status = course_list_info.kStatusOn
        name = whole_soup.find("p", attrs={"class": "course-title"})
        if name is not None:
            name_soup = BeautifulSoup(str(name), "html.parser")
            c_name = name_soup.get_text()
            if c_name is not None:
                course_list_info.course_name = str(c_name)
        price_text = whole_soup.find("div", attrs={"class": "price"})
        price_soup = BeautifulSoup(str(price_text), "html.parser")
        score_div = price_soup.find("div", attrs={"class": "pull-right"})
        score_soup = BeautifulSoup(str(score_div), "html.parser")
        score_text = str(score_soup.get_text())
        if "分" in score_text:
            course_list_info.course_score = score_text.split("分", 1)[0].strip()
        item_l = whole_soup.find_all("div", attrs={"class": "item-l"})
        if len(item_l) > 0:
            lc_soup = BeautifulSoup(str(item_l[0]), "html.parser")
            crowd_text = lc_soup.find("p")
            if crowd_text is not None:
                crowd_soup = BeautifulSoup(str(crowd_text), "html.parser")
                course_list_info.crowd = str(crowd_soup.get_text()).strip()
                course_list_info.crowd_num = int(course_list_info.crowd)
        if len(item_l) > 2:
            ls_soup = BeautifulSoup(str(item_l[2]), "html.parser")
            sub_text = ls_soup.find("p")
            if sub_text is not None:
                sub_soup = BeautifulSoup(str(sub_text), "html.parser")
                course_list_info.subject = str(sub_soup.get_text).strip()
        oc_text = whole_soup.find_all("p", attrs={"class": "overview-content"})
        for n in range(0, 2):
            oc_soup = BeautifulSoup(str(oc_text[n]), "html.parser")
            if n == 0:
                course_list_info.introduction = str(oc_soup.get_text())
            if n == 1:
                course_list_info.course_target = str(oc_soup.get_text())
        teacher_items = whole_soup.find_all("div", attrs={"class": "teacher-item"})
        team = ""
        t_counter = 0
        for teacher_item in teacher_items:
            teacher_soup = BeautifulSoup(str(teacher_item), "html.parser")
            teacher = str(teacher_soup.find("p").string).strip()
            team = team + "，" + teacher
            if t_counter == 0:
                course_list_info.teacher = teacher
        if "，" in team:
            team = team.split("，", 1)[1]
            course_list_info.team = team
        course_list_info.term_id = 1
        course_list_info.term_number = 1
        print(course_list_info.__dict__)
        course_list.append(course_list_info.__dict__)
        return course_list

    def get_mooc_term(self, c_id, t_id, school):
        course_list = []
        params_list = []
        url = 'http://www.wanke001.com/moocDetail/' + str(c_id) + '/' + str(t_id)
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'max-age=0',
            'Host': 'www.wanke001.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
        }
        c_counter = 0
        try:
            resp = requests.get(url, headers=headers, timeout=10)
        except:
            while c_counter != 5:
                time.sleep(3)
                resp = requests.get(url, headers=headers, timeout=10)
                c_counter = c_counter + 1
                if resp.status_code == 200:
                    break
        if c_counter == 5:
            print("fail:" + str(url))
        html = resp.text
        whole_soup = BeautifulSoup(html, "html.parser")
        term_text = whole_soup.find_all("option")
        for term_item in term_text:
            term_id = str(term_item['value'])
            term_num = int(len(term_text))
            new_list = [str(c_id), str(term_id), int(term_num)]
            params_list.append(new_list)
        if len(params_list):
            course_list = self.get_mooc_info(params_list, school)
        return course_list

    def get_mooc_info(self, params_list, school):
        course_list = []
        for item in params_list:
            url = 'http://www.wanke001.com/moocDetail/' + str(item[0]) + '/' + str(item[1])
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Cache-Control': 'max-age=0',
                'Host': 'www.wanke001.com',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
            }
            c_counter = 0
            try:
                resp = requests.get(url, headers=headers, timeout=10)
            except:
                while c_counter != 5:
                    time.sleep(3)
                    resp = requests.get(url, headers=headers, timeout=10)
                    c_counter = c_counter + 1
                    if resp.status_code == 200:
                        break
            if c_counter == 5:
                print("fail:" + str(url))
                return course_list
            html = resp.text
            whole_soup = BeautifulSoup(html, "html.parser")
            course_list_info = CourseListInfo()
            course_list_info.term_number = item[2]
            course_list_info.url = str(url)
            course_list_info.platform = "玩课网"
            course_list_info.save_time = datetime.now()
            course_list_info.school = str(school)
            name = whole_soup.find("p", attrs={"class": "course-title"})
            if name is not None:
                name_soup = BeautifulSoup(str(name), "html.parser")
                c_name = name_soup.get_text()
                if c_name is not None:
                    course_list_info.course_name = str(c_name)
            text_info = str(whole_soup.find("p", attrs={"class": "pull-left"}).string)
            if "开课时间：" in text_info:
                date_info = text_info.split("开课时间：", 1)[1].strip()
                if "~" in date_info:
                    start_date, end_date = date_info.split("~", 1)
                    if "-" in start_date:
                        s_year, s_month, s_date = start_date.split("-", 2)
                        course_list_info.start_date = datetime(int(str(s_year).strip()), int(str(s_month).strip()), int(str(s_date).strip()))
                    if "-" in end_date:
                        e_year, e_month, e_date = end_date.split("-", 2)
                        course_list_info.end_date = datetime(int(str(e_year).strip()), int(str(e_month).strip()), int(str(e_date).strip()))
            crowd_text = whole_soup.find("p", attrs={"style": "color:#999;"})
            crowd_soup = BeautifulSoup(str(crowd_text), "html.parser")
            crowd_num = crowd_soup.find("span")
            if crowd_num is not None:
                num_soup = BeautifulSoup(str(crowd_num), "html.parser")
                course_list_info.crowd = str(num_soup.get_text()).strip()
                course_list_info.crowd_num = int(course_list_info.crowd)
            if course_list_info.start_date != datetime(1999, 1, 1) and course_list_info.end_date != datetime(1999, 1, 1):
                start = course_list_info.start_date
                end = course_list_info.end_date
                if start < datetime.now() < end:
                    course_list_info.status = course_list_info.kStatusOn
                elif datetime.now() < start:
                    course_list_info.status = course_list_info.kStatusLaterOn
                elif end < datetime.now():
                    course_list_info.status = course_list_info.kStatusEnd
            oc_text = whole_soup.find_all("p", attrs={"class": "overview-content"})
            for n in range(0, 2):
                oc_soup = BeautifulSoup(str(oc_text[n]), "html.parser")
                if n == 0:
                    course_list_info.introduction = str(oc_soup.get_text())
                if n == 1:
                    course_list_info.course_target = str(oc_soup.get_text())
            teacher_items = whole_soup.find_all("div", attrs={"class": "teacher-item"})
            team = ""
            t_counter = 0
            for teacher_item in teacher_items:
                teacher_soup = BeautifulSoup(str(teacher_item), "html.parser")
                teacher = str(teacher_soup.find("p").string).strip()
                team = team + "，" + teacher
                if t_counter == 0:
                    course_list_info.teacher = teacher
            if "，" in team:
                team = team.split("，", 1)[1]
                course_list_info.team = team
            term_text = whole_soup.find_all("option")
            for term_item in term_text:
                term_id = str(term_item['value'])
                if int(term_id) == int(item[1]):
                    term_soup = BeautifulSoup(str(term_item), "html.parser")
                    course_list_info.term = str(term_soup.get_text()).strip()
                    if "第" and "期" in course_list_info.term:
                        term_split = course_list_info.term.split("第", 1)[1]
                        course_list_info.term_id = int(term_split.split("期", 1)[0])
            print(course_list_info.__dict__)
            course_list.append(course_list_info.__dict__)
        return course_list
