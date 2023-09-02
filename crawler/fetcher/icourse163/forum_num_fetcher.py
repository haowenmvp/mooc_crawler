from fake_useragent import UserAgent
import requests
import re
from datetime import datetime
from crawler.fetcher.base_fetcher import BaseFetcher
from persistence.model.forum import ForumNumInfo


class ForumNumFetcher(BaseFetcher):
    def __init__(self):
        super().__init__()
        self.session = requests.session()
        self.platform = "爱课程(中国大学MOOC)"

    @staticmethod
    def crawl(url):
        term_id = re.findall(r'tid=([0-9a-z]+)', url)[0]
        ua = UserAgent()
        headers = {
            'Content-Type': 'text/javascript;charset=UTF-8',
            'User-Agent': ua.random
        }
        post_num = 0
        page = 1
        while True:
            payload = "callCount=1\nscriptSessionId=${scriptSessionId}190\nhttpSessionId=725e41d0b7ba46468e06435fc80dcc20\n" \
                      "c0-scriptName=PostBean\nc0-methodName=getAllPostsPagination\nc0-id=0\nc0-param0=number:" + term_id + "\nc0-param1=string:\n" \
                                                                                                                            "c0-param2=number:1\nc0-param3=string:" + str(
                page) + "\nc0-param4=number:50\nc0-param5=boolean:false\nc0-param6=null:null\nbatchId=1591109758168"
            url = 'http://www.icourse163.org/dwr/call/plaincall/PostBean.getAllPostsPagination.dwr'
            response = requests.post(url=url, data=payload, headers=headers).content.decode('unicode_escape')
            res = list(map(int, re.findall(r'countReply=(\d+)', response)))
            main_post = len(res)
            # print(res)
            post_num += main_post
            post_num += sum(res)
            if main_post < 50:
                break
            page += 1
        print(term_id + ' ' + str(post_num))
        return post_num

    def run(self, data=None, login_info=None):
        res = {}
        forum_num_list = []
        for course_id, url, term in data:
            print(course_id, url, term)
            forum_num_info = ForumNumInfo()
            forum_num_info.course_id = course_id
            try:
                post_num = self.crawl(url)
                forum_num_info.forum_num = post_num
                forum_num_info.platform = self.platform
                forum_num_info.save_time = datetime.now()
                forum_num_list.append(forum_num_info.__dict__)
            except:
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
    courses = conn.get_on_course_list('爱课程(中国大学MOOC)')
    data = fm.run(data=courses)
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
    dp.processing(23, data)
