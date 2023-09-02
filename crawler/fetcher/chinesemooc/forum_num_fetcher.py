from fake_useragent import UserAgent
import requests
import re
from crawler.fetcher.base_fetcher import BaseFetcher
from datetime import datetime
from persistence.model.forum import ForumNumInfo


class ForumNumFetcher(BaseFetcher):
    def __init__(self):
        super().__init__()
        self.session = requests.session()
        self.platform = "华文慕课"

    def login(self, username, passwd):
        ua = UserAgent()
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'User-Agent': ua.random
        }
        payload = {'username': username,
                   'password': passwd,
                   'cookietime': '315360000'}
        url = 'http://www.chinesemooc.org/do.php?ac=83cab67b810879e7e6e93b3244d4696b&op=checksubmit'
        self.session.post(url=url, data=payload, headers=headers)

    def crawl(self, url, term):
        ua = UserAgent()
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'User-Agent': ua.random
        }
        print(url, term)
        videoid = re.findall('\d+', url)[-1]
        response = requests.get(url=url, headers=headers).content.decode('utf8')
        # print(response)
        select = re.findall(r'<select([\s\S]*?)</select>', response)[0]
        term_dic = {}
        options = re.findall(r'<option value="(\d+)">([\s\S]*?)</option>', select)
        # print(options)
        for classid, term_option in options:
            term_dic[term_option.replace('\r', '').replace('\n', '').strip()] = classid
        select_term = re.findall(r'<option>([\s\S]*?)</option>', select)[-1].replace('\r', '').replace('\n', '').strip()
        select_id = re.findall(r'<input type="hidden" name="classesid" id="classesid" value="(\d+)">', response)[-1]
        term_dic[select_term] = select_id
        # print(term_dic)
        post_num = 0
        classid = term_dic[term]
        param = {
            'do': 'kvideo_interact',
            'kvideoid': videoid,
            'classesid': classid
        }
        url = 'http://www.chinesemooc.org/kvideo.php'
        course_page = self.session.post(url, headers=headers, params=param).content.decode('utf8')
        # print(course_page)
        num = list(map(int, re.findall(r'帖数：(\d+)', course_page)))
        post_num += sum(num)
        print(post_num)
        return post_num

    def run(self, data=None, login_info=None):
        self.login(login_info["username"], login_info["password"])
        res = {}
        forum_num_list = []
        for course_id, url, term in data:
            print(course_id, url, term)
            forum_num_info = ForumNumInfo()
            forum_num_info.platform = self.platform
            forum_num_info.save_time = datetime.now()
            forum_num_info.course_id = course_id
            try:
                post_num = self.crawl(url, term)
                forum_num_info.forum_num = post_num
                forum_num_list.append(forum_num_info.__dict__)
            except Exception as KeyError:
                print("Not exist")
                continue
        res["forum_num_info"] = forum_num_list
        return res


if __name__ == '__main__':
    fm = ForumNumFetcher()

    from persistence.db.course_list_info_repo import CourseListInfoRepository

    repo = {
        "host": "192.168.232.254",
        "port": 3307,
        "username": "root",
        "password": "123qweASD!@#",
        "database": "mooc_test"
    }
    conn = CourseListInfoRepository(**repo)
    courses = conn.get_on_course_list("华文慕课")
    data = fm.run(data=courses, login_info={"username": '1596061278@qq.com', "password": 'sun123456'})
    from pipeline.pipelines.data_pipeline import DataPipeline
    from persistence.model.pipeline_config import PipelineConfig

    config = PipelineConfig()
    config.task_info_repo = {
        "host": "192.168.232.254",
        "port": 3307,
        "username": "root",
        "password": "123qweASD!@#",
        "database": "mooc_test"
    }
    config.data_repo = {
        "host": "192.168.232.254",
        "port": 3307,
        "username": "root",
        "password": "123qweASD!@#",
        "database": "mooc_test"
    }
    dp = DataPipeline(config)
    dp.processing('24', data)
