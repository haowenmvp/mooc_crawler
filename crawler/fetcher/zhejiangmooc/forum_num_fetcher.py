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

    def get_term_id(self, course_url, term):
        term_seq = int(term[1])
        course_id = course_url.split("/")[-1]
        url = "https://www.zjooc.cn/ajax?time=End6i44pYCfc72pW5jZT474Fn5sDidiP1591813025000&service=%2Freport%2Fapi%2Fcourse%2FcourseAnalyse%2FcourseRelated&params%5BcourseId%5D=" + course_id
        resp = requests.session().get(url)
        # print(resp.content.decode())
        data_json = json.loads(resp.content.decode())
        data = data_json['data']
        childrenCourseList = data["childrenCourseList"]
        childItemId = {}
        seq = len(childrenCourseList)
        for item in childrenCourseList:
            # print(item['id'])
            childItemId[item['batchName']] = item['id']
            if seq == term_seq:
                return item['id']
            seq = seq - 1
        # print(childItemId)
        # print(childItemId)
        return childItemId[term]

    def crawl(self, course_url, term):
        course_id = self.get_term_id(course_url, term)
        url = "https://www.zjooc.cn/ajax?time=End6i44pYCfc72pW5jZT474Fn5sDidiP1591813025000&service=%2Freport%2Fapi%2Fcourse%2FcourseAnalyse%2FcourseRelated&params%5BcourseId%5D=" + course_id
        resp = requests.session().get(url)
        content = resp.content.decode()
        data_json = json.loads(content)
        item = data_json['data']
        # print(item)
        # print(item['currentCourseInteractiveNum'])
        # print(item['totalCourseInteractiveNum'])
        return item['totalCourseInteractiveNum'], item['currentCourseInteractiveNum']

    def run(self, data=None, login_info=None):
        res = {}
        all_forum_num_info = []
        print(len(data))
        for one_course in data:
            course_id = one_course[0]
            course_url = one_course[1]
            term = one_course[2]
            try:
                accumulate_forum_num, forum_num = self.crawl(course_url, term)
                # print(course_url, term, accumulate_forum_num, forum_num)
            except:
                # print(course_url, term, "crawl forum_num error")
                continue
            forum_num_info = ForumNumInfo()
            forum_num_info.platform = '浙江省高等学校在线开放课程共享平台'
            forum_num_info.course_id = course_id
            forum_num_info.accumulate_forum_num = accumulate_forum_num
            forum_num_info.forum_num = forum_num
            forum_num_info.save_time = datetime.now()
            all_forum_num_info.append(forum_num_info.__dict__)
        res["forum_num_info"] = all_forum_num_info
        return res


if __name__ == '__main__':
    forum_num_fetcher = ForumNumFetcher()
    forum_num_fetcher.crawl("https://www.zjooc.cn/course/2c9180836faa4a69017013e6ad601ff3", "第6期次")
    # https://www.zjooc.cn/course/2c9180836faa4928017012dce0b95857 第4期 7655 6571
