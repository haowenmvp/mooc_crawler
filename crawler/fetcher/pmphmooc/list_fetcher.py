from persistence.model.basic_info import CourseListInfo
from ..base_fetcher import BaseFetcher
from bs4 import BeautifulSoup
from datetime import datetime
import time
import requests
import json
import uuid
# 人卫社mooc
class ListFetcher(BaseFetcher):

    def get_course_list_ids(self):
        url = 'http://www.pmphmooc.com/api/student/course/list'
        payloadData = {
            "pageNo": 1,
            "pageSize": 1680,
            "t": {
                "type": 0,
                "courseStatus": "",
                "sort": "1",
                "classid": ""
            }
        }
        payloadHeader = {
            "User-Agent": 'User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
            'Content-Type': 'application/json'
        }
        r = requests.post(url, data=json.dumps(payloadData), headers=payloadHeader)
        #print('列表请求码:',r.status_code)
        d = r.json()
        records = d['result']['records']
        ids = []
        for i in range(len(records)):
            id = records[i]['id']
            ids.append(id)
        return ids
    def get_detail_ids(self, ids):
        detail_ids = []
        url = ' http://www.pmphmooc.com/api/student/course/detail'
        payloadHeader = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0",
            'Content-Type': 'application/json'
        }
        for id in ids:
            payloadData = {"id": id}
            r = requests.post(url, data=json.dumps(payloadData), headers=payloadHeader)
            d = r.json()
            history = d['result']['hisCourseList']
            term_number = len(history)
            for dic in history:
                id = dic['id']
                courseTime = dic['courseTime']
                detail_ids.append([id, courseTime, term_number])
        print('共获得',len(detail_ids),'个学期')
        return detail_ids

    def get_all_courseinfo(self,detail_ids)->list:
        courses_list = []
        error_list = []

        for item in detail_ids:
            try:
                course = CourseListInfo()
                url = 'http://www.pmphmooc.com/api/student/course/detail'
                payloadHeader = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0",
                    'Content-Type': 'application/json'
                }
                payloadData = {"id": item[0]}
                time.sleep(0.1)
                r = requests.post(url, data=json.dumps(payloadData), headers=payloadHeader)
                d = r.json()
                result = d['result']
                url = 'http://www.pmphmooc.com/#/moocDetails?courseID=' + str(item[0])
                course_name = str(result['name'])
                term_id = int(item[1])
                term_number = int(item[2])
                teacher_list = result['teacherList']
                team = ''
                for item in teacher_list:
                    t_name = item['name']
                    team = team + t_name + ','
                team = team[0:-1]

                platform = '人卫社MOOC'
                school = str(result['agencylist'][0]['name'])

                courseBegin = result['courseBegin'].split(' ')[0]
                courseEnd = result['courseEnd'].split(' ')[0]
                start_date = datetime(int(courseBegin.split('-')[0]), int(courseBegin.split('-')[1]), int(courseBegin.split('-')[2]))
                end_date = datetime(int(courseEnd.split('-')[0]), int(courseEnd.split('-')[1]), int(courseEnd.split('-')[2]))
                save_time = datetime.now()

                status = 3
                if result['courseWeekStatusMsg'] == '已结束':
                    status = 0
                if '周' in result['courseWeekStatusMsg']:
                    status = 1
                if result['courseWeekStatusMsg'] == '未开课':
                    status = 2

                crowd = str(result['studentCount'])
                introduction = str(result['catalog'])
                subject = str(result['classifyName'])

                course.url = url
                course.extra = len(detail_ids)
                course.course_name = course_name
                course.term_id = term_id
                course.term_number = term_number
                course.term = '第' + str(term_id) + '次开课'
                course.team = team
                course.platform = platform
                course.school = school
                course.start_date = start_date
                course.end_date = end_date
                course.save_time = save_time
                course.status = status
                course.crowd = crowd
                course.crowd_num = int(crowd)
                course.introduction = introduction
                course.subject = subject
                print(course.__dict__)
                courses_list.append(course.__dict__)
            except Exception as e:
                temp = dict()
                temp['error_url'] = url
                temp['platform'] = platform
                error_list.append(temp)
        return courses_list,error_list

    def run(self):
        ids = self.get_course_list_ids()
        details_ids = self.get_detail_ids(ids)
        courses_list, error = self.get_all_courseinfo(details_ids)
        result = {
            "course_list_info": courses_list,
            "error_list": error
        }
        return result