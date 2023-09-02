import requests
import time
from persistence.model.basic_info import CourseListInfo
from ..base_fetcher import BaseFetcher
from datetime import datetime
from bs4 import BeautifulSoup


class ListFetcher(BaseFetcher):

    def run(self):
        url_list = self.get_course_url()
        course_list = self.get_course_info(url_list)
        url_error = []
        result = {
            "course_list_info": course_list,
            "error_list": url_error
        }
        return result

    def get_course_url(self):
        url_list = []
        sub_id = ['10081', '10104', '10064', '10101', '10102', '10103', '10062', '10063']
        sub_text = ["理工", "文学", "医学", "管理学", "经济学", "教育学", "农林", "综合"]
        for i in range(0, 8):
            current_page = 0
            total_page = 1
            while current_page != total_page:
                current_page = current_page + 1
                dicipline_id = sub_id[i]
                subject = sub_text[i]
                url = 'http://opencourse.umooc.com.cn/custom.do'
                headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'Accept-Encoding': 'gzip, deflate',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Host': 'opencourse.umooc.com.cn',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
                }
                params = {
                    'diciplineId': str(dicipline_id),
                    'sortColumn': 'id',
                    'sortDirection': '-1',
                    'pagingNumberPer': '100',
                    'pagingPage': str(current_page)
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
                    print("fail:" + str(dicipline_id))
                    continue
                html = resp.text
                whole_soup = BeautifulSoup(html, "html.parser")
                page_info = whole_soup.find("td", attrs={"class": "classicLookSummary Summary"})
                page_soup = BeautifulSoup(str(page_info), "html.parser")
                page_info_num = page_soup.find_all("b")
                href_text = whole_soup.find("ul", attrs={"class": "open-elacourseUl"})
                href_soup = BeautifulSoup(str(href_text), "html.parser")
                href_items = href_soup.find_all("a")
                for item in href_items:
                    href = item['href']
                    c_url = 'http://opencourse.umooc.com.cn' + str(href)
                    new_list = [str(subject), str(c_url)]
                    url_list.append(new_list)
                    print(str(new_list))
                if len(page_info_num) > 2:
                    current_page = int(page_info_num[1].string)
                    total_page = int(page_info_num[2].string)
        return url_list

    def get_course_info(self, url_list):
        course_list = []
        for item in url_list:
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Host': 'opencourse.umooc.com.cn',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
            }
            c_counter = 0
            try:
                resp = requests.get(str(item[1]), headers=headers, timeout=10)
            except:
                while c_counter != 5:
                    time.sleep(3)
                    resp = requests.get(str(item[1]), headers=headers, timeout=10)
                    c_counter = c_counter + 1
                    if resp.status_code == 200:
                        break
            if c_counter == 5:
                print("fail:" + str(str(item[1])))
                continue
            html = resp.text
            whole_soup = BeautifulSoup(html, "html.parser")
            course_list_info = CourseListInfo()
            course_list_info.term_id = 1
            course_list_info.term_number = 1
            url = str(item[1])
            url = url.split(';')[0] +'?' + url.split('?')[-1]
            course_list_info.url = url
            course_list_info.platform = "优慕课"
            course_list_info.save_time = datetime.now()
            course_list_info.subject = str(item[0])
            title_text = whole_soup.find("div", attrs={"class": "course-title"})
            title_soup = BeautifulSoup(str(title_text), "html.parser")
            if title_text is not None:
                title_info = str(title_soup.get_text()).strip()
                if '\n' in title_info:
                    course_list_info.course_name = str(title_info.split('\n', 1)[0]).strip()
                    status = str(title_info.split('\n', 1)[1]).strip()
                    if "已结束" in status:
                        course_list_info.status = course_list_info.kStatusEnd
                    if "即将开课" in status:
                        course_list_info.status = course_list_info.kStatusLaterOn
                    if "进行中" in status:
                        course_list_info.status = course_list_info.kStatusOn
                else:
                    course_list_info.course_name = str(title_info)
            tea_info = whole_soup.find("ul", attrs={"class": "open-tea-infoCont"})
            info_soup = BeautifulSoup(str(tea_info), "html.parser")
            li_items = info_soup.find_all('li')
            for li_item in li_items:
                li_soup = BeautifulSoup(str(li_item), "html.parser")
                li_text = str(li_soup.get_text())
                if "开课学校：" in li_text:
                    school = li_text.strip().split("开课学校：", 1)[1]
                    if school is not None:
                        course_list_info.school = str(school)
                if "开始时间：" and "结束时间：" in li_text:
                    start_text, end_date = li_text.split("结束时间：", 1)
                    start_date = str(start_text.split("开始时间：", 1)[1]).strip()
                    if "-" in start_date:
                        s_year, s_month, s_date = start_date.split("-", 2)
                        if s_year != '' and s_month != '' and s_date != '':
                            course_list_info.start_date = datetime(int(str(s_year).strip()), int(str(s_month).strip()), int(str(s_date).strip()))
                    if "-" in end_date:
                        e_year, e_month, e_date = str(end_date).strip().split("-", 2)
                        if e_year != '' and e_month != '' and e_date != '':
                            course_list_info.end_date = datetime(int(str(e_year).strip()), int(str(e_month).strip()), int(str(e_date).strip()))
                if "已报名人数：" in li_text:
                    crowd_text = li_text.strip().split("已报名人数：", 1)[1]
                    if "人" in crowd_text:
                        course_list_info.crowd = str(crowd_text.split("人", 1)[0])
                        course_list_info.crowd_num = int(course_list_info.crowd)
            if course_list_info.status == course_list_info.kStatusUnknown:
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
            teachers_text = whole_soup.find_all("h3", attrs={"class": "teaName"})
            t_counter = 0
            team = ""
            for teacher_text in teachers_text:
                teacher_soup = BeautifulSoup(str(teacher_text), "html.parser")
                teacher = teacher_soup.get_text()
                if t_counter == 0:
                    course_list_info.teacher = str(teacher)
                team = team + "，" + str(teacher)
            if "，" in team:
                course_list_info.team = team.split("，", 1)[1]
            print(course_list_info.__dict__)
            course_list.append(course_list_info.__dict__)
        return course_list

