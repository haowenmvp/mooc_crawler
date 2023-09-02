import requests
import time
from persistence.model.basic_info import CourseListInfo
from ..base_fetcher import BaseFetcher
from datetime import datetime
from lxml import etree

class ListFetcher(BaseFetcher):

    def run(self):
        ids = self.get_courses_ids()
        course_list = self.get_courses_info(ids)
        url_error = []
        result = {
            "course_list_info": course_list,
            "error_list": url_error
        }
        return result

    def run_by_urls(self, urls):
        ids = self.get_courses_ids()
        new_ids = self.fliter_ids(ids, urls)
        course_list = self.get_courses_info(new_ids)
        url_error = []
        result = {
            "course_list_info": course_list,
            "error_list": url_error
        }
        return result

    def run_by_url(self, query_url):
        urls = []
        urls.append(query_url)
        ids = self.get_courses_ids()
        new_ids = self.fliter_ids(ids, urls)
        course_list = self.get_courses_info(new_ids)
        url_error = []
        result = {
            "course_list_info": course_list,
            "error_list": url_error
        }
        return result
    def fliter_ids(self, ids, urls):
        #url = 'http://www.chinesemooc.org/mooc/' + str(id[0])
        new_ids = []
        for url in urls:
            courseid = url.split('/')[-1]
            for item in ids:
                if courseid == str(item[0]):
                    new_ids.append(item)
                    break
        return new_ids
    def get_courses_ids(self):
        url = 'http://www.chinesemooc.org/api/sphinx_search.php'
        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Host': 'www.chinesemooc.org',
            'Origin': 'http://www.chinesemooc.org',
            'Referer': 'http://www.chinesemooc.org/kvideo.php?do=search',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }
        data = {
            'classid': 'all',
            'order': 'newest',
            'page': '1',
            'kvideo_mode': 'all'
        }
        r = requests.post(url, headers=headers, data=data, timeout=10)
        d= r.json()
        msg=d['msg']
        loops = msg['page_total']

        ids=[]
        for i in range(loops):
            data = {
                'classid': 'all',
                'order': 'newest',
                'page': str(i+1),
                'kvideo_mode': 'all'
            }
            r = requests.post(url, headers=headers, data=data, timeout=10)
            d = r.json()
            clist = d['msg']['list']
            for item in clist:
                temp = []
                id = item['kvideoid']
                status = item['kvideo_mode']
                temp.append(id)
                temp.append(status)
                ids.append(temp)
        print('共爬取:', len(ids))
        return ids

    def get_courses_info(self, ids):
        courselist = []
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
        }
        for id in ids:
            url = 'http://www.chinesemooc.org/mooc/' + str(id[0])
            r = requests.get(url, headers=headers)
            r.encoding = r.apparent_encoding
            dom = etree.HTML(r.text)
            terms1 = dom.xpath('//*[@id="top-select"]/div/div/select/option/text()')
            terms=[]
            for t in terms1:
                if '预约' not in t:
                    terms.append(t)
            course_name = dom.xpath('//*[@id="banner"]/div/div[2]/div/h1/text()')[0].strip()
            school = dom.xpath('//*[@id="banner"]/div/div[2]/div/div[1]/div/ul/li[1]/p[2]/text()')[0].strip()
            total_crowd = dom.xpath('//*[@id="people_stat_count"]/text()')[0].strip()
            introduction = dom.xpath('//*[@id="page"]/div[3]/div[1]/p[2]/text()')[0].strip()
            teachers = dom.xpath('//*[@id="banner"]/div/div[2]/div/div[1]/div/ul/li/p[1]/a[1]/text()')
            team = ''
            for t in teachers:
                team = team + t + ','
            team = team[0:-1]
            teacher = teachers[0]

            for i,term in enumerate(terms):
                course = CourseListInfo()
                course.platform = '华文慕课'
                course.extra = len(ids)
                course.save_time = datetime.now()
                course.term_id = i+1
                course.term = str(term.strip())
                course.term_number = len(terms)
                course.url = str(url)
                course.course_name = str(course_name)
                course.team = str(team)
                course.teacher = str(teacher)
                course.school = str(school)
                course.introduction = str(introduction)
                course.total_crowd = str(total_crowd)
                course.total_crowd_num = int(total_crowd)
                status = id[1]
                if status is not None:
                    if int(status) == 0:
                        course.status = course.kStatusLaterOn
                    if int(status) == 1:
                        course.status = course.kStatusOn
                    if int(status) == 2:
                        course.status = course.kStatusEnd
                print(course.__dict__)
                courselist.append(course.__dict__)
        return courselist


