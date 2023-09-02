from datetime import datetime
import requests
import json
from lxml import etree
import re
from fake_useragent import UserAgent
import hashlib
from crawler.fetcher.base_fetcher import BaseFetcher
from datetime import datetime
from persistence.model.forum import ForumNumInfo


class ForumNumFetcher(BaseFetcher):
    def __init__(self):
        super().__init__()
        self.session = requests.session()
        self.cookie = ""

    def login(self, username, password):
        ua = UserAgent()
        md5 = hashlib.md5()
        md5.update(password.encode('utf-8'))
        pw = md5.hexdigest()
        # print(pw)
        url = "http://www.coursegate.cn/center/center/login_login.action?d=1591757845105"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': ua.random
        }
        body = {
            'loginId': username,
            'passwd': pw,
            'auto': 'false',
            'into': 0,
            'unionId': '',
            'third': ''
        }
        req = self.session.post(url=url, headers=headers, data=body)
        cookies = requests.utils.dict_from_cookiejar(req.cookies)
        self.cookie = self.cookieToStr(cookies)

    def cookieToStr(self, cookie_dict):
        cookie_str = ""
        for item in cookie_dict.keys():
            cookie_str = cookie_str + item+ "=" + cookie_dict[item] + ";"
        return cookie_str

    def collect_course(self, course_id):
        url = "http://www.coursegate.cn/spoc/auth/userCourse/saveCollectCourse.json"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
            'cookie': self.cookie
        }
        body = {'params.courseId': course_id}
        self.session.post(url, headers=headers, data=body)

    def crawl(self, course_url, term):
        url = "http://learnspace.coursegate.cn/learnspace/learn/answer/blue/questions_list.action?params.pageSize=10&params.curPage=1"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
            'cookie': self.cookie
        }
        body = {
            'params.expectType': 'list',
            'params.courseId': 'ff80808155a5f7800155e78aae860076',
            'params.selected': 1,
            'params.opt': 1,
            'params.query':''
        }
        course_id = re.findall(r'params\.courseId=[0-9a-zA-Z]+', course_url)[0].split("=")[1]
        # print(course_id)
        body['params.courseId'] = course_id
        resp = self.session.post(url, headers=headers, data=body)
        content = resp.content.decode()
        if '您没有该课程的访问权限' in content:
            self.collect_course(course_id)
            # print("报名课程")
            resp = self.session.post(url, headers=headers, data=body)
            content = resp.content.decode()
        # print(content)
        dom = etree.HTML(content)
        # print(content)
        page_totalCount = dom.xpath('//*[@id="page_totalCount"]/@value')[0]
        page = 1
        post_num = 0
        while (page-1)*10 <= int(page_totalCount):
            url = "http://learnspace.coursegate.cn/learnspace/learn/answer/blue/questions_list.action?params.pageSize=10&params.curPage=" + str(page)
            resp = self.session.post(url, headers=headers, data=body)
            content = resp.content.decode()
            dom = etree.HTML(content)
            post_name_num = len(dom.xpath('//*[@class="dy_name"]'))
            reply_name_num = len(dom.xpath('//*[@name="reuserName"]'))
            # print(post_name_num, reply_name_num)
            post_num = post_num + post_name_num + reply_name_num
            page = page + 1
        # print(post_num)
        return post_num

    def run(self, data=None, login_info=None):
        self.login("1814791349@qq.com", "19980120Xj")
        res = {}
        all_forum_num_info = []
        print(len(data))
        for one_course in data:
            course_id = one_course[0]
            course_url = one_course[1]
            term = one_course[2]
            try:
                forum_num = self.crawl(course_url, term)
                # print(course_url, forum_num)
            except:
                print(course_url, "get post_num error")
                continue
            forum_num_info = ForumNumInfo()
            forum_num_info.platform = '中科云教育'
            forum_num_info.course_id = course_id
            forum_num_info.forum_num = forum_num
            forum_num_info.save_time = datetime.now()
            all_forum_num_info.append(forum_num_info.__dict__)
        res["forum_num_info"] = all_forum_num_info
        return res


if __name__ == '__main__':
    forum_num_fetcher = ForumNumFetcher()
    forum_num_fetcher.login("1814791349@qq.com", "19980120Xj")
    forum_num_fetcher.crawl("http://www.coursegate.cn/spoc/open/course_detail.action?params.courseId=ff80808170755d1401708596ace51157", 1)