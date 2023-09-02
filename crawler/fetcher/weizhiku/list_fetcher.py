from persistence.model.basic_info import CourseListInfo
from ..base_fetcher import BaseFetcher
from bs4 import BeautifulSoup
from datetime import datetime
import math
import requests
from lxml import etree

import uuid
# 微知库数字校园学习平台
class ListFetcher(BaseFetcher):

    def get_courses_ids(self):
        headers = {
            'Accept': 'text/html',
            'Accept-Encoding': 'Accept-Encoding',
            'Accept-Language': 'zh-CN,zh;q=0.9,und;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Host': 'wzk.36ve.com',
            'Referer': 'http://wzk.36ve.com/CourseCenter/course/course-list',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
            'X-PJAX': 'true',
            'X-PJAX-Container': '#new_html',
            'X-Requested-With': 'XMLHttpRequest'
        }
        url = "http://wzk.36ve.com/CourseCenter/course/course-list?page=1&_pjax=%23new_html&_pjax=%23new_html"
        r = requests.get(url, headers=headers)
        r.encoding = r.apparent_encoding
        dom = etree.HTML(r.text)
        total = dom.xpath('/html/body/div/div[1]/div[2]/span/em/text()')[0]
        loops = math.ceil(float(total)/15)
        # print(total,loops)
        print('开始获取所有课程id......')
        ids = []
        for i in range(int(loops)):
            url = "http://wzk.36ve.com/CourseCenter/course/course-list?page="+ str(i+1) + "&_pjax=%23new_html&_pjax=%23new_html"
            r = requests.get(url, headers=headers)
            r.encoding = r.apparent_encoding
            dom = etree.HTML(r.text)
            names = dom.xpath('/html/body/div/div[3]/div[1]/div[2]/ul/li/div[2]/h5/a/text()')
            courseids = dom.xpath('/html/body/div/div[3]/div[1]/div[2]/ul/li/div[2]/h5/@courseid')
            bcourseids = dom.xpath('/html/body/div/div[3]/div[1]/div[2]/ul/li/div[2]/h5/@bcourseid')
            one_page_ids = []
            for i in range(len(courseids)):
                temp = dict()
                temp['name'] = names[i]
                temp['courseid'] = courseids[i]
                temp['bcourseid'] = bcourseids[i]
                one_page_ids.append(temp)
            ids = ids + one_page_ids

        print('获取完毕，共获取', len(ids), '门课')
        return ids

    def get_courses_info(self, ids):
        course_list = []
        url_error = []
        for item in ids:
            try:
                course = CourseListInfo()
                url = 'http://wzk.36ve.com/index.php/CourseCenter/course/course-list-info?courseId=' + item['courseid']+'&bcourseId='+ item['bcourseid']
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'}
                r = requests.get(url, headers=headers)
                r.encoding = r.apparent_encoding
                dom = etree.HTML(r.text)

                course_name = item['name']
                crowd = dom.xpath('//*[@id="peo"]/text()')[0]
                platform = '微知库数字校园学习平台'
                school = dom.xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[1]/span[3]/text()')[0].split('：')[-1]
                try:
                    teacher = dom.xpath('/html/body/div[2]/div/div[3]/div[1]/ul/li[1]/div/div[2]/text()')[0].strip()
                except Exception:
                    teacher = ''
                start_time = dom.xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/span[1]/text()')[0].split(': ')[-1]
                teachers = dom.xpath('/html/body/div[2]/div/div[3]/div[1]/ul/li/div/div[2]/text()')
                team = ''
                for t in teachers:
                    t = t.strip()
                    team = team + t + ','
                team = team[0:-1]
                details = dom.xpath('/html/body/div[2]/div/div[2]/div[1]/p/text()')[0]

                course.crowd = str(crowd)
                course.crowd_num = int(crowd)
                course.extra = len(ids)
                course.term_number = 1
                course.term_id =1
                course.url = str(url)
                course.course_name = str(course_name)
                course.platform = str(platform)
                course.school = str(school)
                course.status = 1
                course.teacher = str(teacher)
                course.save_time = datetime.now()
                course.start_date = datetime(int(start_time.strip().split('-')[0]), int(start_time.strip().split('-')[1])
                                             , int(start_time.strip().split('-')[2]))
                course.team = team
                course.introduction = details.strip()
                if course.start_date > course.save_time:
                    course.status = 2
                course_list.append(course.__dict__)
                print(course.__dict__)
            except Exception as e:
                temp = dict()
                temp['error_url'] = url
                temp['platform'] = platform
                url_error.append(temp)

        return course_list, url_error

    def run(self):
        # url_error记录错误爬取的网址
        ids = self.get_courses_ids()
        print(ids)
        course_list, url_error = self.get_courses_info(ids)
        result = {
            "course_list_info": course_list,
            "error_list": url_error
        }

        return result