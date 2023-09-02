import time
import json
import logging
import selenium.webdriver
import selenium.webdriver.remote.webdriver
from selenium.webdriver.support.ui import WebDriverWait
import datetime
from crawler.fetcher import BaseFetcher


class BasicFetcher(BaseFetcher):
    def run(self) -> dict:
        return self.get_course_info()

    def get_course_info(self) -> dict:
        res = dict()
        res['update_time'] = datetime.datetime.now()
        res['semester_resource_info'] = ''
        res['semester_homework_info'] = ''
        res['semester_interact_info'] = ''
        res['semester_exam_info'] = ''
        res['semester_extension_info'] = ''
        ret = dict()
        ret['semester_result_info'] = res
        try:
            teacher_list = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_elements_by_xpath("//div[@class='sidebar']/div[3]//li"))
        except selenium.common.exceptions.TimeoutException:
            return ret
        course_director_name = ''
        teacher_team = []
        school_name = ''
        for i in range(len(teacher_list)):
            person_name = teacher_list[i].find_element_by_class_name('person-name').text
            person_attach = teacher_list[i].find_elements_by_class_name('person-attach')[0].text
            teacher_team.append(person_name)
            if i == 0:
                course_director_name = person_name
                school_name = person_attach
        semester_teacherteam_info = dict()
        semester_teacherteam_info['course_director_name'] = course_director_name
        semester_teacherteam_info['teacher_team'] = teacher_team
        semester_teacherteam_info_json = json.dumps(semester_teacherteam_info, ensure_ascii=False)
        #res['school_name'] = school_name
        res['semester_teacherteam_info'] = semester_teacherteam_info_json

        try:
            view_intro = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_class_name('view-intro'))
        except selenium.common.exceptions.TimeoutException:
            return ret
        #课程名
        #view_title_substr = view_intro.find_element_by_css_selector('h3.view-title.substr')
        #res['course_name'] = view_title_substr.text
        #开结课时间
        view_time = view_intro.find_elements_by_class_name('view-time')[0]
        semester_start_date = view_time.text.split('\n')[0][6:16]
        semester_end_date = view_time.text.split('\n')[0][19:]
        res['semester_start_date'] = datetime.datetime.strptime(semester_start_date, '%Y-%m-%d')
        res['semester_end_date'] = datetime.datetime.strptime(semester_end_date, '%Y-%m-%d')
        #选课人数
        semester_studentnum = view_intro.find_element_by_id('favoriteNum').text[0:-1]
        if semester_studentnum == '':
            res['semester_studentnum'] = 0
        else:
            res['semester_studentnum'] = int(semester_studentnum)

        #1.进入学习  直接进入 <a class="btn-public studySession" href="javascript:void(0)">进入学习</a>
        #2. 加入课程  <a class="btn-public" href="javascript:void(0)" id="joinCourse" iscanstudy="true" openobjecttype="10">加入课程</a>

        #3. 兴趣学习   直接进入 <a class="btn-public btn-interest" href="javascript:void(0)">兴趣学习</a>
        #4.回顾课程内容  直接进入 <a class="btn-public" href="/portal/session/index/11175.mooc">回顾课程内容</a>
        #判断是否需要先加入课程
        try:
            joincourse_a = view_intro.find_element_by_id('joinCourse')
            logging.info('加入课程')
            joincourse_a.click()
            time.sleep(2)
            try:
                userselectstudy_div = WebDriverWait(self.driver, self.kWaitSecond).until(
                    lambda x: x.find_element_by_id('userSelectStudy'))
            except selenium.common.exceptions.TimeoutException:
                return ret
            input_cr_span = userselectstudy_div.find_elements_by_css_selector('span.input-cr')[1]
            input_r_a = input_cr_span.find_element_by_class_name('input-r')
            input_r_a.click()
            time.sleep(2)
            confirminfo_a = userselectstudy_div.find_element_by_id('confirmInfo')
            confirminfo_a.click()
            time.sleep(2)
            try:
                btn_public_studysession = WebDriverWait(self.driver, self.kWaitSecond).until(
                    lambda x: x.find_element_by_class_name('view-intro').find_element_by_partial_link_text('进入学习'))
            except selenium.common.exceptions.TimeoutException:
                return ret
            btn_public_studysession.click()
            time.sleep(2)
        except selenium.common.exceptions.NoSuchElementException:
            try:
                join_study = view_intro.find_element_by_partial_link_text('进入学习')
                join_study.click()
                time.sleep(2)
                logging.info('进入学习')
            except selenium.common.exceptions.NoSuchElementException:
                try:
                    interest_study = view_intro.find_element_by_partial_link_text('兴趣学习')
                    interest_study.click()
                    time.sleep(2)
                    logging.info('兴趣学习')
                except selenium.common.exceptions.NoSuchElementException:
                    review_course_content = view_intro.find_element_by_partial_link_text('回顾课程内容')
                    review_course_content.click()
                    time.sleep(2)
                    logging.info('回顾课程内容')
        try:
            test_and_homework_a = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_xpath("//a[@title='测验与作业']"))
        except selenium.common.exceptions.TimeoutException:
            return ret
        test_and_homework_a.click()
        time.sleep(2)
        try:
            main_div = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_css_selector('div.content.person-center').find_element_by_class_name('main'))
        except selenium.common.exceptions.TimeoutException:
            return ret
        view_empty_box = main_div.find_elements_by_class_name('view-empty-box')
        if len(view_empty_box) == 1:
            return ret
        else:
            homework_toggle_list = main_div.find_elements_by_css_selector('tr.homework-toggle')
        semester_test_info = dict()
        semester_test_info['test_total_times'] = len(homework_toggle_list)
        test_total_questions = 0
        for i in range(len(homework_toggle_list)):
            test_total_questions += len(homework_toggle_list[i].find_elements_by_css_selector('tr'))
        semester_test_info['test_total_questions'] = test_total_questions
        semester_test_info_json = json.dumps(semester_test_info, ensure_ascii=False)
        res['semester_test_info'] = semester_test_info_json
        return ret
