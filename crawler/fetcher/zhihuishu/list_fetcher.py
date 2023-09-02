from persistence.model.basic_info import CourseListInfo
from lxml import etree
import hashlib
from ..base_fetcher import BaseFetcher
from datetime import datetime
import requests
class ListFetcher(BaseFetcher):

    def run(self):
        ids1 = self.get_course_type1_ids()
        data1 = self.get_courses_type1_info(ids1)

        ids2 = self.get_course_type2_ids()
        data2= self.get_courses_type2_info(ids2)

        ids3 = self.get_course_type3_ids()
        data3 = self.get_courses_type3_info(ids3)

        data = data1+data2+data3
        result = {
            "course_list_info": data,
            "error_list": []
        }
        return result

    def get_course_type1_ids(self):
        url = "https://bd-search.zhihuishu.com/bg_search/indexPageSearch/shareCourse?family=-1&coursePackageId=-1&zhsmCourseCategory=-1&subjectFirstCode=-1&schoolLevel=-1&orderRuler=-1&pageNo=0&pageSize=60&uuid=&dateFormate=1592871622000"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
        }
        r = requests.get(url, headers=headers)
        d = r.json()
        total = d["total"]
        url = "https://bd-search.zhihuishu.com/bg_search/indexPageSearch/shareCourse?family=-1&coursePackageId=-1&zhsmCourseCategory=-1&subjectFirstCode=-1&schoolLevel=-1&orderRuler=-1&pageNo=0&pageSize="+str(total)+"&uuid=&dateFormate=1592871622000"
        r = requests.get(url, headers=headers)
        d = r.json()
        rt = d["rt"]
        ids = []
        for item in rt:
            ids.append(item["courseId"])
        # print(len(ids))
        # print(ids[0])
        return ids

    def get_courses_type1_info(self, ids):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
        }
        course_list = []
        # 对每一个课程组id
        for group_id in ids:
            url = "https://coursehome.zhihuishu.com/courseHome/{}#teachTeam"
            terms = []
            url = url.format(group_id)
            r = requests.get(url, headers=headers)
            dom = etree.HTML(r.text)
            term_ids = dom.xpath('//*[@id="firstLevel"]/div/ul/li/@termid')
            term_ids.reverse()
            coustom_term_id = 1
            for i, termid in enumerate(term_ids):
                path = '//*[@id="firstLevel"]/div/ul/li[{}]/a/@courseid'
                path = path.format(len(term_ids)-i)
                courseids = dom.xpath(path)[0]
                courseids = courseids.split("[")[-1].split("]")[0]
                for courseid in courseids.split(','):
                    temp = []
                    temp.append(termid)
                    temp.append(courseid.strip())
                    temp.append(coustom_term_id)
                    coustom_term_id += 1
                    terms.append(temp)

            for item in terms:
                try:
                    course = CourseListInfo()
                    data = {"courseId":item[1],"termId":item[0]}
                    r = requests.post("https://coursehome.zhihuishu.com/home/pageCount", headers=headers, data=data)
                    d = r.json()
                    crowd = d["personCount"]
                    total_crowd = d["personTotalCount"]
                    url = 'https://coursehome.zhihuishu.com/courseHome/{}/{}#teachTeam'
                    url = url.format(item[1], item[0])
                    r = requests.get(url, headers=headers)
                    dom = etree.HTML(r.text)

                    course_name = dom.xpath('//*[@id="courseName"]/text()')[0]
                    subject = dom.xpath('/html/body/div[3]/div[1]/div/div[2]/text()')[0]
                    term_id = item[2]
                    term = dom.xpath('//*[@id="Semesterselect-btn"]/a/text()')[0]
                    team = dom.xpath('/html/body/div[3]/div[2]/div[1]/div[2]/div[3]/ul/li[1]/span/span/text()')[0]
                    teacher = team.split("、")[0]
                    platform = '智慧树'
                    school = dom.xpath('/html/body/div[3]/div[2]/div[1]/div[2]/div[3]/ul/li[2]/div/span/text()')[0]
                    crowd_num = int(crowd)
                    total_crowd_num = int(total_crowd)
                    term_number = len(terms)
                    introduction = dom.xpath('/html/body/div[3]/div[2]/div[1]/div[2]/div[1]/p/text()')[0].strip()
                    platform_course_id = group_id
                    platform_term_id = str(item[1]) + str(item[0])
                    hash_str = str(group_id) + platform
                    course_group_id = hashlib.md5(hash_str.encode('utf-8')).hexdigest()

                    course.url=url
                    course.course_name = course_name
                    course.subject = subject
                    course.term_id = term_id
                    course.term = term
                    course.team=team
                    course.platform=platform
                    course.school=school
                    course.teacher=teacher
                    course.save_time=datetime.now()
                    course.status=1
                    course.crowd = str(crowd)
                    course.total_crowd = str(total_crowd)
                    course.crowd_num = crowd_num
                    course.total_crowd_num = total_crowd_num
                    course.term_number = term_number
                    course.introduction = introduction
                    course.platform_course_id = platform_course_id
                    course.platform_term_id = platform_term_id
                    course.course_group_id = course_group_id
                    course.block = "大学共享课"
                    course_list.append(course.__dict__)
                    print(course.__dict__)
                except Exception:
                    pass
        return course_list

    def get_course_type2_ids(self):
        url = "https://bd-search.zhihuishu.com/bg_search/indexPageSearch/virtualCourse?pageNo=0&pageSize=60&uuid=&dateFormate=1592881414000"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
        }
        r = requests.get(url, headers=headers)
        d = r.json()
        total = d["total"]
        url = "https://bd-search.zhihuishu.com/bg_search/indexPageSearch/virtualCourse?pageNo=0&pageSize="+str(total)+"&uuid=&dateFormate=1592881414000"
        r = requests.get(url, headers=headers)
        d = r.json()
        rt = d["rt"]
        ids = []
        for item in rt:
            temp = dict()
            if item["isLock"] == 0:
                temp['course_name'] = item['courseName']
                temp['school'] = item['schoolName']
                temp['teacher'] = item['speakerName']
                temp['courseUrl'] = item['courseUrl']
                ids.append(temp)
        return ids

    def get_courses_type2_info(self, ids):
        crowd_url = "https://virtualcourse.zhihuishu.com/course/static/info"
        team_url = "https://virtualcourse.zhihuishu.com/course/info/queryCourseTeachers"
        info_url ="https://virtualcourse.zhihuishu.com/course/info/queryCourseInfo"
        introduction_url = "https://virtualcourse.zhihuishu.com/course/info/queryCourseSource"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
        }
        courses_list=[]
        for item in ids:
            courseUrl = item["courseUrl"]
            courseId = courseUrl.split("=")[-1]

            course = CourseListInfo()
            params = {'courseId': courseId}
            r_crowd = requests.get(crowd_url, headers=headers, params=params)
            d_crowd = r_crowd.json()
            crowd = d_crowd["data"]["studentCount"]

            r_team = requests.get(team_url, headers=headers, params=params)
            team = ''
            d_team = r_team.json()
            rt = d_team['data']
            for i in rt:
                teacher = i['teacherName']
                if teacher is not None:
                    team = team + teacher + ','
            if team != '':
                team = team[0:-1]

            r_info = requests.get(info_url, headers=headers, params=params)
            d_info = r_info.json()
            subject = d_info["data"]["labelsName"]

            params = {'courseId': courseId,'extType':"1"}
            r_introduction = requests.get(introduction_url, headers=headers, params=params)
            d_introduction = r_introduction.json()
            introduction = d_introduction['data'][0]['content']
            platform = '智慧树'

            course.save_time = datetime.now()
            course.course_name = str(item['course_name'])
            course.platform = str(platform)
            course.school = str(item['school'])
            course.teacher = str(item['teacher'])
            course.url = courseUrl
            course.subject = str(subject)
            course.introduction = str(introduction)
            course.crowd = str(crowd)
            course.crowd_num = int(crowd)
            course.total_crowd = str(crowd)
            course.total_crowd_num = int(crowd)
            course.term_id = 1
            course.term_number = 1
            course.team = str(team)
            course.status = 1
            course.platform_course_id = courseId
            course.platform_term_id =courseId
            hash_str = courseId + "智慧树虚拟实验室"
            course.course_group_id = hashlib.md5(hash_str.encode('utf-8')).hexdigest()
            course.block = "虚拟实验室"
            print(course.__dict__)
            courses_list.append(course.__dict__)
        return courses_list

    def get_course_type3_ids(self):
        url = "https://bd-search.zhihuishu.com/bg_search/indexPageSearch/interestCourse?interestLabelId=-1&courseType=-1&orderRuler=1&pageNo=0&pageSize=60&uuid=&dateFormate=1592880138000"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
        }
        r = requests.get(url, headers=headers)
        d = r.json()
        total = d["total"]
        url = "https://bd-search.zhihuishu.com/bg_search/indexPageSearch/interestCourse?interestLabelId=-1&courseType=-1&orderRuler=1&pageNo=0&pageSize="+str(total)+"&uuid=&dateFormate=1592880138000"
        r = requests.get(url, headers=headers)
        d = r.json()
        rt = d["rt"]
        ids = []
        for item in rt:
            temp = dict()
            temp['course_name'] = item['courseName']
            temp['school'] = item['schoolName']
            temp['teacher'] = item['speakerName']
            temp['courseId'] = item['courseId']
            ids.append(temp)
        return ids

    def get_courses_type3_info(self, ids):
        courses_list = []
        info_url = 'https://b2cpush.zhihuishu.com/b2cpush/courseDetail/query2CCourseInfo'
        team_url = 'https://b2cpush.zhihuishu.com/b2cpush/courseDetail/query2CCourseTeacherTeam'
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
        }
        for item in ids:
            course = CourseListInfo()
            params = {'courseId': item['courseId'], 'studyMode': "1"}
            data = {'courseId': item['courseId'], 'studyMode': "1"}

            r_info = requests.get(info_url, headers=headers, params=params)
            d_info = r_info.json()
            crowd = d_info['rt']['totalStudyCount']
            introduction = d_info['rt']['introduction']

            r_team = requests.post(team_url, headers=headers, data=data)
            team = ''
            d_team = r_team.json()
            rt = d_team['rt']
            for i in rt:
                teacher = i['name']
                if teacher is not None:
                    team = team + teacher + ','
            if team != '':
                team = team[0:-1]
            url = 'https://www.zhihuishu.com/portals_h5/2clearning.html#/courseInfo/' + str(
                item['courseId']) + '?labelId=0&studyMode=1'
            platform = '智慧树'

            course.save_time = datetime.now()
            course.course_name = str(item['course_name'])
            course.platform = str(platform)
            course.school = str(item['school'])
            course.teacher = str(item['teacher'])
            course.url = url
            course.introduction = str(introduction)
            course.crowd = str(crowd)
            course.crowd_num = int(crowd)
            course.total_crowd = str(crowd)
            course.total_crowd_num = int(crowd)
            course.term_id = 1
            course.term_number = 1
            course.team = str(team)
            course.status = 1
            course.platform_course_id = item['courseId']
            course.platform_term_id = item['courseId']
            hash_str = str(item['courseId']) + "智慧树兴趣课"
            course.course_group_id = hashlib.md5(hash_str.encode('utf-8')).hexdigest()
            course.block = "兴趣课"
            print(course.__dict__)
            courses_list.append(course.__dict__)
        return courses_list


if __name__ == '__main__':
    list_fetcher = ListFetcher()
    ids1 = list_fetcher.get_course_type1_ids()
    print(len(set(ids1)))
    data1 = list_fetcher.get_courses_type1_info(set(ids1))

    ids2 = list_fetcher.get_course_type2_ids()
    data2 = list_fetcher.get_courses_type2_info(ids2)

    ids3 = list_fetcher.get_course_type3_ids()
    data3 = list_fetcher.get_courses_type3_info(ids3)

    data = data1+ data2 + data3
    print(len(data))

    import pickle
    with open('newzhihuishu.pkl', 'wb') as f:
        pickle.dump(data, f)

    urlsets = dict()
    for item in data:
        if item["url"] not in urlsets.keys():
            urlsets[item["url"]] = 1
        else:
            urlsets[item["url"]] += 1
    for key in urlsets.keys():
        if urlsets[key]>1:
            print(key,urlsets[key])
