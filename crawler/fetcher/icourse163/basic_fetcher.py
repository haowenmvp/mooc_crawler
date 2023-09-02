import datetime
import json
from time import sleep
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait

from crawler.fetcher import BaseFetcher

class BasicFetcher(BaseFetcher):
    def run(self) -> dict:
        announce_url = self.driver.current_url
        self.driver.get(announce_url)
        return self.get_basic_info()

    def get_basic_info(self) -> dict:
        res = {}
        res['semester_start_date'] = self.get_homepage_info()[0]
        res['semester_end_date'] = self.get_homepage_info()[1]
        res['semester_teacherteam_info'] = self.get_homepage_info()[2]
        res['semester_studentnum'] = self.get_homepage_info()[3]
        res['semester_resource_info'] = self.get_semester_resource_info()
        res['semester_homework_info'] = self.get_semester_homework_info()
        res['semester_interact_info'] = self.get_semester_interact_info()
        res['semester_exam_info'] = self.get_semester_exam_info()
        res['semester_test_info'] = self.get_semester_test_info()
        res['semester_extension_info'] = self.get_semester_extension_info()
        res['update_time'] = datetime.datetime.now()
        return res

    def get_homepage_info(self):
        homepage_info = []
        #进入首页界面
        self.driver.find_element_by_xpath("//div[@class='f-cb']/a[@class='f-fl']").click()
        sleep(5)
        #获取老师个数
        teachers_count_block = self.driver.find_elements_by_class_name('t-title')
        for teachers_count in teachers_count_block:
            teachers_count = teachers_count.text
        teachers_final_count = teachers_count[teachers_count.index('位')-1]
        teachers_final_count = int(teachers_final_count)
        #获取老师名称
        m = int(teachers_final_count / 3)
        n = int(teachers_final_count % 3)
        if n == 0 :
            k = m
        else:
            k = m + 1
        teachers_final_name = []
        for i in range(k):
            teachers_name_block = self.driver.find_elements_by_class_name('f-fc3')
            for teachers_name in teachers_name_block:
                if '位' not in teachers_name.text:
                     teachers_final_name.append(teachers_name.text)
            if self.driver.find_elements_by_class_name('um-list-slider_next') != []:
                self.driver.find_elements_by_class_name('um-list-slider_next')[0].click()
        course_director_name = teachers_final_name[0]
        teacher_team = []
        for i in range(1,len(teachers_final_name)):
            teacher_team.append(teachers_final_name[i])
        #获取学期信息
        current_term = self.driver.find_elements_by_class_name('ux-dropdown_cnt')[0].text
        terms = self.driver.find_element_by_css_selector('[class="ux-dropdown_listview th-bd2"]')
        term_list = terms.get_attribute('outerHTML')
        soap = BeautifulSoup(term_list,features='html5lib')
        term_times = len(soap.find_all('li'))
        #获取时间信息
        course_time_info = self.driver.find_elements_by_class_name('course-enroll-info_course-info_term-info_term-time')[0].text
        tmp1 = course_time_info.index('：')
        tmp2 = course_time_info.index('~')
        begin_time = course_time_info[tmp1+1:tmp2].strip()
        end_time = course_time_info[tmp2+1:].strip()
        #获取注册人数
        stu_num = self.driver.find_elements_by_class_name('course-enroll-info_course-enroll_price-enroll_enroll-count')[0].text
        tmp3 = stu_num.index('有')
        tmp4 = stu_num.index('人')
        stu_enroll_num = int(stu_num[tmp3+1:tmp4])

        semester_start_date = datetime.datetime.strptime(begin_time, '%Y-%m-%d')
        semester_end_date = datetime.datetime.strptime(end_time, '%Y-%m-%d')
        semester_teacherteam_info = {}
        semester_teacherteam_info['course_director_name'] = course_director_name
        semester_teacherteam_info['teacher_team'] = teacher_team
        semester_teacherteam_info_json = json.dumps(semester_teacherteam_info, ensure_ascii=False)
        semester_studentnum = stu_enroll_num
        homepage_info.append(semester_start_date)
        homepage_info.append(semester_end_date)
        homepage_info.append(semester_teacherteam_info_json)
        homepage_info.append(semester_studentnum)

        return homepage_info

    def get_semester_resource_info(self): 
        resource_info = {}
        self.driver.find_element_by_xpath("//ul[@class='tab u-tabul']/li[@data-name='课件']").click()
        sleep(5)
        tmp = self.driver.find_elements_by_css_selector('[class="j-titleName name f-fl f-thide"]')
        tmp_count = len(tmp)
        for i in range(1,tmp_count-1):
            self.driver.execute_script("arguments[0].scrollIntoView();", tmp[i])
            tmp[i].click()
            sleep(5)
        tmp1 = self.driver.find_elements_by_css_selector('[class="u-icon-video2"]')
        video_num = len(tmp1)
        tmp2 = self.driver.find_elements_by_css_selector('[class="u-icon-doc"]')
        ppt_num = len(tmp2)
        resource_info['video_num'] = video_num
        resource_info['ppt_num'] = ppt_num
        resource_info['video_time'] = ''
        resource_info['announce_num'] = ''
        return json.dumps(resource_info, ensure_ascii=False)

    def get_semester_homework_info(self):
        homework_info = {}
        #进入测验与验收界面
        self.driver.find_element_by_xpath("//ul[@class='tab u-tabul']/li[@data-name='测验与作业']").click()
        sleep(5)
        homework_blocks = self.driver.find_elements_by_class_name('m-chapterQuizHwItem')
        homework_times = len(homework_blocks)
        def get_num(i):
            self.driver.find_elements_by_class_name('j-quizBtn')[i].click()
            sleep(5)
            self.driver.find_elements_by_class_name('u-btn')[0].click()
            sleep(5)
            question_num_cell = self.driver.find_elements_by_class_name('m-choiceQuestion')
            return question_num_cell
        question_num = 0
        for i in range(homework_times):
            question_num += get_num(i)
            announce_url = self.driver.current_url
            self.driver.get(announce_url)
            self.driver.find_element_by_xpath("//ul[@class='tab u-tabul']/li[@data-name='测验与作业']").click()
            sleep(5)
        question_blocks = self.driver.find_elements_by_class_name('m-choiceQuestion')
        question_num = len(question_blocks)
        homework_info['homework_times'] = homework_times
        homework_info['question_num'] = question_num


        homework_info['homework_stu_num'] = ''

        return json.dumps(homework_info, ensure_ascii=False)

    def get_semester_interact_info(self): # todo
        return ''

    def get_semester_exam_info(self):
        exam_info = {}
        question_num = 0
        self.driver.find_element_by_xpath("//ul[@class='tab u-tabul']/li[@data-name='考试']").click()
        sleep(5)
        exam_blocks = self.driver.find_elements_by_class_name('titleBox')
        btn = self.driver.find_elements_by_class_name('u-btn')
        def get_num(i):
            self.driver.find_elements_by_class_name('u-btn')[i].click()
            sleep(5)
            try:
                self.driver.find_element_by_xpath("//div[@class='m-beforeTest']/a").click()
            except Exception as e:
                pass
            sleep(5)
            question_num_cell = self.driver.find_elements_by_class_name('examMode')
            return question_num_cell

        for i in range(len(btn)):
            question_num_cell = get_num(i)
            question_num += question_num_cell
            self.driver.find_elements_by_class_name('u-icon-return')[0].click()
            self.driver.find_elements_by_class_name('j-left')[0].click()
            sleep(5)

        exam_times = len(exam_blocks)
        exam_info['exam_times'] = exam_times
        exam_info['question_num'] = question_num
        exam_info['exam_stu_num'] = ''
        exam_info['exam_pass_num'] = ''

        return json.dumps(exam_info, ensure_ascii=False)

    def get_semester_test_info(self):
        return ''

    def get_semester_extension_info(self):
        return ''







