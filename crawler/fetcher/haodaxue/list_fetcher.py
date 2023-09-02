from persistence.model.basic_info import CourseListInfo
from ..base_fetcher import BaseFetcher
from datetime import datetime
import requests
from lxml import etree

# 好大学
class ListFetcher(BaseFetcher):

    def get_courses_ids(self):
        url = "https://www.cnmooc.org/portal/ajaxCourseIndex.mooc"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
        }
        payload = {'keyWord': '',
                   'openFlag': '0',
                   'fromType': '0',
                   'learningMode': '0',
                   'certType': '',
                   'languageId': '',
                   'categoryId': ['01', '02', '03', '04', '05', '06', '07'
                       , '08', '09', '10', '11', '12', '13', '14', '15'],
                   'menuType': 'course',
                   'schoolId': '',
                   'pageIndex': 1}
        r = requests.post(url, headers=headers, data=payload, timeout=0.5)
        r.encoding = r.apparent_encoding
        dom = etree.HTML(r.text)
        loops = dom.xpath('//*[@id="pageId"]/ul/li[9]/span/text()')[0]
        loops = int(loops.split(' ')[0].split('/')[-1])
        ids = []
        i = 1
        print('开始获取所有课程id......')
        while (i <= loops):
            try:
                payload = {'keyWord': '',
                           'openFlag': '0',
                           'fromType': '0',
                           'learningMode': '0',
                           'certType': '',
                           'languageId': '',
                           'categoryId': ['01', '02', '03', '04', '05', '06', '07'
                               , '08', '09', '10', '11', '12', '13', '14', '15'],
                           'menuType': 'course',
                           'schoolId': '',
                           'pageIndex': i}
                r = requests.post(url, headers=headers, data=payload, timeout=0.5)
                r.encoding = r.apparent_encoding
                dom = etree.HTML(r.text)
                xpath_courseid = '/html/body/ul/li/div/div[2]/h3/a/@courseid'
                xpath_courseopenid = '/html/body/ul/li/div/div[2]/h3/a/@courseopenid'
                #print('第',i,'页')
                courseids = dom.xpath(xpath_courseid)
                courseopenids = dom.xpath(xpath_courseopenid)
                for j, courseid in enumerate(courseids):
                    ids.append([courseid, courseopenids[j]])
                i += 1
            except Exception:
                continue
        print('获取完毕，共获取',len(ids),'门课')
        return ids

    def get_courses_info(self, ids):
        courses_list = []
        url_error = []
        for item in ids:
            try:
                course = CourseListInfo()
                url = 'https://www.cnmooc.org/portal/course/'+str(item[0])+'/'+str(item[1])+'.mooc'
                headers = {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
                }
                flag = 0
                while flag == 0:
                    try:
                        r = requests.get(url, headers=headers, timeout=0.5)
                        flag = 1
                    except Exception:
                        pass
                dom = etree.HTML(r.text)
                start_flag = 0
                end_flag = 0
                try:
                    date = dom.xpath('/html/body/div[2]/div[1]/div/div[2]/div[1]/div[2]/text()')[0].split('：')[-1].strip()
                    start_time = date.split('—')[0].strip()
                    course.start_date = datetime(int(start_time.split('-')[0]), int(start_time.split('-')[1]),
                                                 int(start_time.split('-')[2]))
                    start_flag = 1
                except Exception:
                    pass
                try:
                    date = dom.xpath('/html/body/div[2]/div[1]/div/div[2]/div[1]/div[2]/text()')[0].split('：')[-1].strip()
                    end_time = date.split('—')[1].strip()
                    course.end_date = datetime(int(end_time.split('-')[0]), int(end_time.split('-')[1]),
                                               int(end_time.split('-')[2]))
                    end_flag = 1
                except Exception:
                    pass
                save_time = datetime.now()
                if start_flag == 1 and end_flag == 1:
                    if course.start_date < save_time and course.end_date > save_time:
                        status = 1
                    if course.start_date > save_time:
                        status = 2
                    if course.end_date < save_time:
                        status = 0
                else:
                    status = 1
                xpath = "/html/body/div[2]/div[1]/div/div[2]/div[1]/div[1]/select/option[@value='" + str(item[1]) + "']/text()"
                term = dom.xpath(xpath)
                if len(term):
                    term = term[0].strip()
                else:
                    term = '随到随学'
                teachers = dom.xpath('/html/body/div[2]/div[2]/div[3]/div[2]/ul/li/div/div/h3/a/text()')
                if len(teachers) != 0:
                    teacher = teachers[0]
                    team = ''
                    for t in teachers:
                        team = team + t + ','
                else:
                    teacher = ''
                    team = ''
                team = team[0:-1]
                school = dom.xpath('/html/body/div[2]/div[2]/div[3]/div[2]/ul/li/div/div/p[1]/text()')
                if len(school)!= 0:
                    school = school[0]
                else:
                    school =''
                course_name = dom.xpath('/html/body/div[2]/div[1]/div/div[2]/h3/text()')
                if len(course_name)!=0:
                    course_name = course_name[0].strip()
                else:
                    course_name = dom.xpath('//*[@id="main"]/div[1]/div/div/div/h3/text()')[0]
                platform = '好大学在线'
                introduction = dom.xpath('/html/body/div[2]/div[3]/div[1]/p/text()')
                if len(introduction)!=0:
                    introduction = introduction[0].strip()
                else:
                    introduction = ''
                scoring_standard = dom.xpath('/html/body/div[2]/div[3]/div[3]/p/text()')
                if len(scoring_standard)!=0:
                    scoring_standard = scoring_standard[0]
                else:
                    scoring_standard = ''
                subject = dom.xpath('/html/body/div[2]/div[2]/div[2]/div[2]/p/text()')
                if len(subject)!=0:
                    subject = subject[0].strip()
                else:
                    subject = ''

                try:
                    terms = dom.xpath('/html/body/div[2]/div[1]/div/div[2]/div[1]/div[1]/select/option/@value')
                    term_number = len(terms)
                    term_id = terms.index(item[1])+1
                    course.term_id = term_id
                    course.term_number = term_number
                except Exception:
                    course.term_id = 1
                    course.term_number = 1

                course.extra = len(ids)
                course.term = str(term)
                course.teacher = str(teacher)
                course.school = str(school)
                course.course_name = str(course_name)
                course.platform = str(platform)
                course.url = str(url)
                course.save_time = save_time
                course.introduction = str(introduction)
                course.scoring_standard = str(scoring_standard)
                course.subject = subject
                course.team = team
                course.status = status
                print(course.__dict__)
                courses_list.append(course.__dict__)
            except Exception as e:
                temp = dict()
                temp['error_url'] = url
                temp['platform'] = platform
                url_error.append(temp)

        return courses_list,url_error

    def run(self):
        #url_error记录错误爬取的网址 暂不反回
        ids = self.get_courses_ids()
        course_list, url_error = self.get_courses_info(ids)
        result = {
            "course_list_info": course_list,
            "error_list": url_error
        }
        return result
