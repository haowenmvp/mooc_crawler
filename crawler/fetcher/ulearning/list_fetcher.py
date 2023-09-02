from persistence.model.basic_info import CourseListInfo
from ..base_fetcher import BaseFetcher
from datetime import datetime
import requests

# 好大学
class ListFetcher(BaseFetcher):

    def get_courses_ids(self):
        url = "https://courseapi.ulearning.cn/homepage/textbooks?pn=1&ps=1129&type=1&seclassifyId=&subjectId=&order=3&keyword=&orgId=&aspId=1"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'}

        r = requests.get(url, headers=headers)
        d = r.json()
        textbooks = d['textbooks']
        print('获取课程数：', len(textbooks))
        ids = []
        for item in textbooks:
            course = dict()
            course['id'] = item['textbookId']
            course['course_name'] = item['name']
            course['school'] = item['openCourseSchool']
            course['teacherlist'] = item['textbookTeacherList']
            course['crowd'] = item['totalStudent']
            course['introduction'] = item['description']
            course['subject'] = item['classify']
            course['extra'] = str(len(textbooks))
            ids.append(course)
        return ids

    def get_courses_info(self, ids):
        courses_list = []
        for item in ids:
            course = CourseListInfo()
            url = 'https://www.ulearning.cn/ulearning_web/web!courseDetail.do?courseID=' + str(item['id'])
            course_name = item['course_name']
            platform = '优学院（人民网公开课）'
            school = item['school']
            crowd = item['crowd']
            extra = item['extra']
            if crowd is None:
                crowd = 0
            introduction = item['introduction']
            subject = item['subject']
            if subject is not None:
                subject = ''
            teacherlist = item['teacherlist']
            if len(teacherlist) == 0:
                teacher = ''
                team=''
            else:
                teacher = teacherlist[0]['name']
                team = ''
                for i in range(len(teacherlist)):
                    t = teacherlist[i]['name']
                    team = team + t + ','
                team = team[0:-1]

            course.url = str(url)
            course.course_name = str(course_name)
            course.platform = str(platform)
            course.school = str(school)
            course.teacher = str(teacher)
            course.team = str(team)
            course.save_time = datetime.now()
            course.crowd = str(crowd)
            course.term_number = 1
            course.term_id = 1
            if crowd is not None:
                course.crowd_num = int(crowd)
            course.introduction = str(introduction)
            course.subject = str(subject)
            course.extra = str(extra)
            course.status = course.kStatusOn

            print(course.__dict__)
            courses_list.append(course.__dict__)
        return courses_list

    def run(self):
        # url_error记录错误爬取的网址 暂不反回
        ids = self.get_courses_ids()
        course_list = self.get_courses_info(ids)
        error_list = []
        all_list = {
            'course_list_info': course_list,
            'error_list': error_list
        }
        return all_list
