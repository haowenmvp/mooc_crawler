import pickle

import selenium
from requests_html import HTMLSession
import time

from retrying import retry
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.remote.webdriver import WebDriver
from crawler.fetcher import BaseFetcher
from persistence.model.basic_info import CourseListInfo
from datetime import datetime


class ListFetcher(BaseFetcher):
    def __init__(self):
        super().__init__()
        self.listHeaders = {
            "Accept": "text/html, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Connection": "keep-alive",
            "Content-Length": "55",
            "Content-Type": "application/x-www-form-urlencoded",
            "Cookie": "JSESSIONID=6B493D4844611AE7E9704D468FD75A17.whaty148084_zkyjy_yz",
            "Host": "www.coursegate.cn",
            "Origin": "http://www.coursegate.cn",
            "Referer": "http://www.coursegate.cn/spoc/open/course_list.action",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0",
            "X-Requested-With": "XMLHttpRequest"
        }
        self.courseHeaders = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Cookie": "JSESSIONID=6B493D4844611AE7E9704D468FD75A17.whaty148084_zkyjy_yz",
            "Host": "www.coursegate.cn",
            "Referer": "http://www.coursegate.cn/spoc/open/course_list.action",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0"
        }
        self.error_list = []

    @retry(stop_max_attempt_number=5)
    def deep_page(self, course_info_list, href):
        session3 = HTMLSession()
        r3 = session3.get("http://www.coursegate.cn" + href, headers=self.courseHeaders)
        time.sleep(0.5)
        course_list_info = CourseListInfo()
        course_list_info.url = "http://www.coursegate.cn" + href
        course_list_info.course_name = r3.html.find("div.m_mykcboxright>h1", first=True).text
        course_list_info.platform = '中科云教育'
        course_list_info.crowd = r3.html.find("span.m_red", first=True).text
        course_list_info.save_time = datetime.now()
        teachers = r3.html.find("span.m_blue")
        for teacher in teachers:
            course_list_info.team = course_list_info.team + teacher.text + ','
            course_list_info.teacher = course_list_info.teacher + teacher.text + ','
        course_list_info.team = course_list_info.team[0:-1]
        course_list_info.teacher = course_list_info.team.split(",")[0]
        if len(teachers) == 0:
            course_list_info.team = '暂无'
            course_list_info.teacher = '暂无'
        introduction = r3.html.find("div.m_kcxxleft>div.m_kcxxbox",first=True)
        course_list_info.introduction = introduction.find("div.m_kcxxtext", first=True).text.replace("\n", '').replace('\xa0', '').replace("\u3000", '')
        course_list_info.status = 1
        course_list_info.term_id = 1
        course_list_info.term_number = -1
        course_list_info.crowd_num = int(course_list_info.crowd)
        course_info_list.append(course_list_info.__dict__)
        print(course_list_info.__dict__)
        r3.close()
        session3.close()
        pass

    @retry(stop_max_attempt_number=5)
    def fetch_one_page(self, i):
        course_info_list = []
        session2 = HTMLSession()
        data = {
            "params.status": 0,
            "params.courseTypeId": "",
            "params.courseName": ""
        }
        r2 = session2.post("http://www.coursegate.cn/spoc/open//include/course_datas.action?params.pageSize=6&params"
                           ".curPage=" + str(i), data=data, headers=self.listHeaders)
        courseList = r2.html.find("ul.courseList_view")
        for ulItem in courseList:
            liList = ulItem.find("li.mr10")
            for li in liList:
                link = li.find("div.cosView_img", first=True)
                href = link.find("a", first=True).attrs["href"]
                self.deep_page(course_info_list, href)
                time.sleep(0.5)
        r2.close()
        session2.close()
        return course_info_list
        pass

    def get_all_pages(self):
        session1 = HTMLSession()
        data = {
            "params.status": 0,
            "params.courseTypeId": "",
            "params.courseName": ""
        }
        r1 = session1.post("http://www.coursegate.cn/spoc/open//include/course_datas.action?params.pageSize=6&params"
                           ".curPage=1", data=data, headers=self.listHeaders)
        totalCount = r1.html.find("input#page_totalCount", first=True).attrs
        print(totalCount["value"])
        r1.close()
        session1.close()
        return totalCount["value"]
        pass

    def fetch_all_page(self):
        total_list = []
        totalCount = int(self.get_all_pages())
        page = 1
        if totalCount % 6 == 0:
            totalPage = int(totalCount/6)
        else:
            totalPage = int(totalCount/6) + 1
        for i in range(1, totalPage + 1):
            print("-----将爬取第：" + str(page) + "/" + str(totalPage) + "页-----")
            total_list = total_list + self.fetch_one_page(i)
            page = page + 1
        for item3 in total_list:
            item3['extra'] = '共' + str(totalCount) + '门课'
        print(len(total_list))
        return total_list
        pass

    def run(self):
        total_list = self.fetch_all_page()
        set_list = self.distinct(total_list)
        after_deal_list = self.deal_data(set_list)
        # with open("zhongkeyun.pkl", "wb") as f:
        #     pickle.dump(after_deal_list, f)
        return {
            "course_list_info": after_deal_list,
            "error_list": self.error_list
        }

    def deal_data(self, set_list):
        after_deal = []
        for item1 in set_list:
            for item2 in after_deal:
                if item2["course_name"] == item1["course_name"] and \
                    item2["school"] == item1["school"] and \
                    item2["teacher"] == item1["teacher"]:
                    if item1["crowd_num"] > item2["crowd_num"]:
                        item2["valid"] = 0
                    else:
                        item1["valid"] = 0
            after_deal.append(item1)
        return after_deal
        pass


if __name__ == '__main__':
    url = 'http://moocs.unipus.cn/'
    options = Options()
    options.add_argument('-headless')
    browser = selenium.webdriver.Firefox(options=options)
    browser.get(url)
    fetcher = ListFetcher()
    fetcher.run()
