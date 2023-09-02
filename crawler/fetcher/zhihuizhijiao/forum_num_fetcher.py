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
        url = "https://mooc.icve.com.cn/portal/Course/getMoocCourseDetail?courseId=WDJWH366438&courseOpenId=&courseType=0&isNewApi=true"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
        }
        body = {
            'courseId': 'WDJWH366438',
            'courseOpenId': '',
            'courseType': 0,
            'isNewApi': 'true'
        }
        course_id = re.findall(r'cid=[0-9a-zA-Z]+', course_url)[0].split("=")[1]
        try:
            term = re.findall(r'oid=[0-9a-zA-Z]+', course_url)[0].split("=")[1]
        except:
            term = "1"
        body['courseOpenId'] = term
        body['courseId'] = course_id
        resp = requests.session().post(url, data=body)
        content = resp.content.decode()
        dic = json.loads(content)
        chooseAllInteractCount = dic['chooseAllInteractCount']
        chooseThisInteractCount = dic['chooseThisInteractCount']
        # print(chooseThisInteractCount, chooseAllInteractCount)
        return chooseAllInteractCount, chooseThisInteractCount

    def run(self, data=None, login_info=None):
        res = {}
        all_forum_num_info = []
        print(len(data))
        for one_course in data:
            course_id = one_course[0]
            course_url = one_course[1]
            term = one_course[2]
            try:
                accumulate_forum_num, forum_num = self.crawl(course_url)
                # print(course_url, accumulate_forum_num, forum_num)
            except:
                print(course_url, "crawl forum num error")
                continue
            forum_num_info = ForumNumInfo()
            forum_num_info.platform = '智慧职教'
            forum_num_info.course_id = course_id
            forum_num_info.accumulate_forum_num = accumulate_forum_num
            forum_num_info.forum_num = forum_num
            forum_num_info.save_time = datetime.now()
            all_forum_num_info.append(forum_num_info.__dict__)
        res["forum_num_info"] = all_forum_num_info
        return res


if __name__ == '__main__':
    forum_num_fetcher = ForumNumFetcher()
    forum_num_fetcher.crawl("https://mooc.icve.com.cn/course.html?cid=BLXCZ760510#oid=5")