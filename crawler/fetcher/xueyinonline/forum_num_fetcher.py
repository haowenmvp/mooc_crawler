from datetime import datetime
import requests
import json
import re
from lxml import etree
from crawler.fetcher.base_fetcher import BaseFetcher
from datetime import datetime
from persistence.model.forum import ForumNumInfo


class ForumNumFetcher(BaseFetcher):
    def __init__(self):
        super().__init__()
        pass

    def login(self, login_info):
        pass

    def crawl(self, course_url):
        course_id = course_url.split("/")[-1]
        url = "http://www.xueyinonline.com/statistics/api/stattistics-data?courseId=" + course_id
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
        }
        s = requests.session()
        r = s.get(url, headers=headers)
        content = r.content.decode()
        dict = json.loads(content)
        nums = dict["bbsAllCount"]
        # print(nums)
        return int(nums)

    def run(self, data=None, login_info=None):
        res = {}
        all_forum_num_info = []
        print(len(data))
        for one_course in data:
            course_id = one_course[0]
            course_url = one_course[1]
            term = one_course[2]
            try:
                accumulate_forum_num = self.crawl(course_url)
                # print(course_url, accumulate_forum_num)
            except:
                print(course_url, "crawl forum_num error")
                continue
            forum_num_info = ForumNumInfo()
            forum_num_info.platform = '北京学银在线教育科技有限公司'
            forum_num_info.course_id = course_id
            forum_num_info.accumulate_forum_num = accumulate_forum_num
            forum_num_info.save_time = datetime.now()
            all_forum_num_info.append(forum_num_info.__dict__)
        res["forum_num_info"] = all_forum_num_info
        return res


if __name__ == '__main__':
    forum_num_fetcher = ForumNumFetcher()
    forum_num_fetcher.crawl("http://www.xueyinonline.com/detail/205642187")
