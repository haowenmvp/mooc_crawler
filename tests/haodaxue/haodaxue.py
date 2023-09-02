#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import json
import datetime
import uuid
import logging
import settings
import selenium.webdriver
import selenium.webdriver.remote.webdriver
from selenium.webdriver.support.ui import WebDriverWait

class ForumFetcher:
    def __init__(self, driver: selenium.webdriver.remote.webdriver.WebDriver, kWaitSecond=10):
        self.__driver = driver
        self.__kWaitSecond = kWaitSecond

    def run(self) -> dict:
        return self.get_video_resource_info()

    def get_forum_info(self) -> dict:
        res = dict()
        res_list = list()
        res['forum_post_info'] = res_list
        try:
            view_intro = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                lambda x: x.find_element_by_class_name('view-intro'))
        except selenium.common.exceptions.TimeoutException:
            return res
        # 1.进入学习  直接进入 <a class="btn-public studySession" href="javascript:void(0)">进入学习</a>
        # 2. 加入课程  <a class="btn-public" href="javascript:void(0)" id="joinCourse" iscanstudy="true" openobjecttype="10">加入课程</a>

        # 3. 兴趣学习   直接进入 <a class="btn-public btn-interest" href="javascript:void(0)">兴趣学习</a>
        # 4.回顾课程内容  直接进入 <a class="btn-public" href="/portal/session/index/11175.mooc">回顾课程内容</a>
        # 判断是否需要先加入课程
        try:
            joincourse_a = view_intro.find_element_by_id('joinCourse')
            joincourse_a.click()
            time.sleep(2)
            try:
                userselectstudy_div = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                    lambda x: x.find_element_by_id('userSelectStudy'))
            except selenium.common.exceptions.TimeoutException:
                return res
            input_cr_span = userselectstudy_div.find_elements_by_css_selector('span.input-cr')[1]
            print(input_cr_span.text)
            input_r_a = input_cr_span.find_element_by_class_name('input-r')
            print(input_r_a.tag_name)
            input_r_a.click()
            time.sleep(2)
            confirminfo_a = userselectstudy_div.find_element_by_id('confirmInfo')
            confirminfo_a.click()
            time.sleep(2)
            try:
                btn_public_studysession = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                    lambda x: x.find_element_by_class_name('view-intro').find_element_by_partial_link_text('进入学习'))
            except selenium.common.exceptions.TimeoutException:
                return res
            btn_public_studysession.click()
            time.sleep(2)
            print(self.__driver.current_url)
            time.sleep(5)
        except selenium.common.exceptions.NoSuchElementException:
            try:
                join_study = view_intro.find_element_by_partial_link_text('进入学习')
                join_study.click()
                time.sleep(2)
                print(self.__driver.current_url)
                print('进入学习')
            except selenium.common.exceptions.NoSuchElementException:
                try:
                    interest_study = view_intro.find_element_by_partial_link_text('兴趣学习')
                    interest_study.click()
                    time.sleep(2)
                    print(self.__driver.current_url)
                    print('兴趣学习')
                except selenium.common.exceptions.NoSuchElementException:
                    review_course_content = view_intro.find_element_by_partial_link_text('回顾课程内容')
                    review_course_content.click()
                    time.sleep(2)
                    print(self.__driver.current_url)
                    print('回顾课程内容')
        try:
            discussion_a = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                lambda x: x.find_element_by_xpath("//a[@title='讨论区']"))
        except selenium.common.exceptions.TimeoutException:
            return res
        discussion_a.click()
        time.sleep(2)
        try:
            discussions_a = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                lambda x: x.find_elements_by_xpath("//a[@class='link-default']"))
        except selenium.common.exceptions.TimeoutException:
            return res
        print(len(discussions_a))
        for i in range(len(discussions_a)):
            print(i)
            try:
                discussion_len = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                    lambda x: x.find_elements_by_css_selector('td.td03'))
                print(len(discussion_len))
            except selenium.common.exceptions.TimeoutException:
                print("//td["+str(i)+"][@class='td03']")
                return res
            if discussion_len[i].text == '0':
                continue
            else:
                try:
                    discussion_a = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                        lambda x: x.find_elements_by_xpath("//a[@class='link-default']"))
                    print(len(discussion_a))
                except selenium.common.exceptions.TimeoutException:
                    print("//a[" + str(i) + "][@class='link-default']")
                    return res
                discussion_a[i].click()
                time.sleep(2)
                try:
                    thread_view = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                        lambda x: x.find_element_by_id('thread_view'))
                except selenium.common.exceptions.TimeoutException:
                    print("thread_view")
                    self.__driver.back()
                    time.sleep(2)
                trs = thread_view.find_elements_by_tag_name('tr')
                for j in range(len(trs)):
                    try:
                        thread_view = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                            lambda x: x.find_element_by_id('thread_view'))
                    except selenium.common.exceptions.TimeoutException:
                        print("thread_view")
                        self.__driver.back()
                        time.sleep(2)
                    trs = thread_view.find_elements_by_tag_name('tr')
                    forum_dict = dict()
                    forum_dict['forum_reply_id'] = '0'
                    forum_dict['forum_post_id'] = str(uuid.uuid1())
                    forum_dict['forum_name'] = '讨论区'
                    forum_subject = trs[j].find_element_by_css_selector('h3.post-title.cursor_poi.link_action')
                    forum_dict['forum_subject'] = forum_subject.text.strip()
                    forum_dict['forum_reply_userid'] = ''
                    forum_dict['forum_post_userrole'] = 0
                    forum_dict['forum_post_username'] = trs[j].find_element_by_css_selector('div.poster-area.substr').text.strip()
                    #forum_dict['forum_post_time'] = trs[j].
                    forum_post_time = trs[j].find_elements_by_tag_name('td')[3].text
                    forum_dict['forum_post_time'] = datetime.datetime.strptime(forum_post_time.split('\n')[1] + " " + forum_post_time.split('\n')[0], '%Y-%m-%d %H:%M')
                    forum_dict['update_time'] = datetime.datetime.now()
                    forum_dict['forum_post_type'] = 1
                    forum_subject.click()
                    time.sleep(2)
                    try:
                        postMain = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                            lambda x: x.find_element_by_id('postMain'))
                    except selenium.common.exceptions.TimeoutException:
                        print("postMain")
                        self.__driver.back()
                        time.sleep(2)
                    reply_items = postMain.find_elements_by_css_selector('li.reply-item')
                    print(len(reply_items))
                    for m in range(len(reply_items)):
                        forum_dict1 = dict()
                        if m == 0:
                            reply_items[m].find_element_by_class_name('reply-content')
                            forum_dict['forum_post_content'] = reply_items[m].find_element_by_class_name('reply-content').text
                        else:
                            try:
                                reply_intro = reply_items[m].find_element_by_class_name('reply-intro')
                            except selenium.common.exceptions.NoSuchElementException:
                                break
                            forum_dict1['forum_reply_userid'] = ''
                            forum_dict1['forum_post_userrole'] = 0
                            forum_dict1['forum_post_id'] = str(uuid.uuid1())
                            forum_dict1['forum_reply_id'] = forum_dict.get('forum_post_id')
                            forum_dict1['forum_name'] = '讨论区'
                            forum_dict1['forum_subject'] = forum_dict.get('forum_subject')
                            forum_dict1['forum_post_username'] = reply_intro.find_element_by_css_selector('span.reply-name.substr').text.strip()
                            forum_dict1['forum_post_time'] = datetime.datetime.strptime(reply_intro.find_element_by_css_selector('span.reply-time').text.strip(), '%Y-%m-%d %H:%M')
                            forum_dict1['forum_post_content'] = reply_intro.find_element_by_css_selector('div.reply-content').text.strip()
                            forum_dict1['update_time'] = datetime.datetime.now()
                            forum_dict1['forum_post_type'] = 2
                            reply_intro2_a = reply_intro.find_element_by_css_selector('div.reply-action.replyAction.clearfix').find_element_by_class_name('link-group').find_elements_by_tag_name('a')[1]
                            if int(reply_intro2_a.find_element_by_tag_name('em').text) > 0:
                                print("回复" + reply_intro2_a.find_element_by_tag_name('em').text)
                                #reply_intro2_a.find_element_by_class_name('b').click()
                                self.__driver.execute_script("$(arguments[0]).click()", reply_intro2_a)
                                time.sleep(1)
                                sub_ritems = reply_items[m].find_elements_by_css_selector('li.sub-ritem')
                                for n in range(len(sub_ritems)):
                                    forum_dict2 = dict()
                                    forum_dict2['forum_post_id'] = str(uuid.uuid1())
                                    forum_dict2['forum_subject'] = forum_dict.get('forum_subject')
                                    forum_dict2['forum_reply_id'] = forum_dict1.get('forum_post_id')
                                    forum_dict2['forum_name'] = '讨论区'
                                    forum_dict2['forum_post_username'] = sub_ritems[n].find_element_by_css_selector('a.reply-name.substr.cursor_def').text.strip()
                                    forum_dict2['forum_post_time'] = sub_ritems[n].find_element_by_css_selector(
                                        'span.reply-time').text.strip()
                                    forum_dict2['forum_post_content'] = sub_ritems[n].find_element_by_css_selector(
                                        'div.reply-content').text.strip()
                                    forum_dict2['update_time'] = datetime.datetime.now()
                                    forum_dict2['forum_post_type'] = 2
                                    forum_dict2['forum_reply_userid'] = ''
                                    forum_dict2['forum_post_userrole'] = 0
                                    res_list.append(forum_dict2)
                            res_list.append(forum_dict1)
                    res_list.append(forum_dict)
                    self.__driver.back()
                    time.sleep(2)
                self.__driver.back()
                time.sleep(2)
        res['forum_post_info'] = res_list
        return res

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
            teacher_list = WebDriverWait(self.__driver, self.__kWaitSecond).until(
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
            view_intro = WebDriverWait(self.__driver, self.__kWaitSecond).until(
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
                userselectstudy_div = WebDriverWait(self.__driver, self.__kWaitSecond).until(
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
                btn_public_studysession = WebDriverWait(self.__driver, self.__kWaitSecond).until(
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
            test_and_homework_a = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                lambda x: x.find_element_by_xpath("//a[@title='测验与作业']"))
        except selenium.common.exceptions.TimeoutException:
            return ret
        test_and_homework_a.click()
        time.sleep(2)
        try:
            main_div = WebDriverWait(self.__driver, self.__kWaitSecond).until(
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

    # {"resource_structure_info" : {}, "resource_info" : []}
    def get_video_resource_info(self) -> dict:
        video_type_list = ['.avi', '.rmvb', '.rm', '.asf',  '.divx' , '.mpg', '.mpeg', '.mpe', '.wmv', '.mp4', '.mkv', '.vob', '.flv']
        res = dict()
        resource_structure_info = list()
        resource_info = list()
        res['resource_structure_info'] = resource_structure_info
        res['resource_info'] = resource_info
        try:
            view_intro = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                lambda x: x.find_element_by_class_name('view-intro'))
        except selenium.common.exceptions.TimeoutException:
            return res
        # 1.进入学习  直接进入 <a class="btn-public studySession" href="javascript:void(0)">进入学习</a>
        # 2. 加入课程  <a class="btn-public" href="javascript:void(0)" id="joinCourse" iscanstudy="true" openobjecttype="10">加入课程</a>

        # 3. 兴趣学习   直接进入 <a class="btn-public btn-interest" href="javascript:void(0)">兴趣学习</a>
        # 4.回顾课程内容  直接进入 <a class="btn-public" href="/portal/session/index/11175.mooc">回顾课程内容</a>
        # 判断是否需要先加入课程
        try:
            joincourse_a = view_intro.find_element_by_id('joinCourse')
            joincourse_a.click()
            time.sleep(2)
            try:
                userselectstudy_div = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                    lambda x: x.find_element_by_id('userSelectStudy'))
            except selenium.common.exceptions.TimeoutException:
                return res
            input_cr_span = userselectstudy_div.find_elements_by_css_selector('span.input-cr')[1]
            print(input_cr_span.text)
            input_r_a = input_cr_span.find_element_by_class_name('input-r')
            print(input_r_a.tag_name)
            input_r_a.click()
            time.sleep(2)
            confirminfo_a = userselectstudy_div.find_element_by_id('confirmInfo')
            confirminfo_a.click()
            time.sleep(2)
            try:
                btn_public_studysession = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                    lambda x: x.find_element_by_class_name('view-intro').find_element_by_partial_link_text('进入学习'))
            except selenium.common.exceptions.TimeoutException:
                return res
            btn_public_studysession.click()
            time.sleep(2)
            print(self.__driver.current_url)
            time.sleep(5)
        except selenium.common.exceptions.NoSuchElementException:
            try:
                join_study = view_intro.find_element_by_partial_link_text('进入学习')
                join_study.click()
                time.sleep(2)
                print(self.__driver.current_url)
                print('进入学习')
            except selenium.common.exceptions.NoSuchElementException:
                try:
                    interest_study = view_intro.find_element_by_partial_link_text('兴趣学习')
                    interest_study.click()
                    time.sleep(2)
                    print(self.__driver.current_url)
                    print('兴趣学习')
                except selenium.common.exceptions.NoSuchElementException:
                    review_course_content = view_intro.find_element_by_partial_link_text('回顾课程内容')
                    review_course_content.click()
                    time.sleep(2)
                    print(self.__driver.current_url)
                    print('回顾课程内容')
        try:
            discussion_a = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                lambda x: x.find_element_by_xpath("//a[@title='章节导航']"))
        except selenium.common.exceptions.TimeoutException:
            return res
        discussion_a.click()
        time.sleep(2)
        try:
            main_chapter_nav = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                lambda x : x.find_element_by_css_selector('div.main.chapter-nav')
            )
        except selenium.common.exceptions.TimeoutException:
            return res
        unitNavigation = main_chapter_nav.find_element_by_id('unitNavigation')
        view_chapters = unitNavigation.find_elements_by_class_name('view-chapter')
        for i in range(len(view_chapters)):
            try:
                view_chapter = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                    lambda x: x.find_element_by_css_selector('div.main.chapter-nav').find_element_by_id('unitNavigation').find_elements_by_class_name('view-chapter')[i]
                )
            except selenium.common.exceptions.TimeoutException:
                return res
            chapter_title = view_chapter.find_element_by_css_selector('span.chapter-text.substr').text
            chapter_index = i + 1
            view_lectures = view_chapter.find_elements_by_class_name('view-lecture')
            for j in range(len(view_lectures)):
                try:
                    view_chapter = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                        lambda x: x.find_element_by_css_selector('div.main.chapter-nav').find_element_by_id(
                            'unitNavigation').find_elements_by_class_name('view-chapter')[i]
                    )
                except selenium.common.exceptions.TimeoutException:
                    return res
                view_lecture = view_chapter.find_elements_by_class_name('view-lecture')[j]
                view_lecture_a = view_lecture.find_element_by_css_selector('a.lecture-text.substr.unitItem')
                view_lecture_dict = dict()
                view_lecture_dict['resource_structure_id'] = str(uuid.uuid1())
                view_lecture_dict['resource_chapter_index'] = chapter_index
                view_lecture_dict['resource_chapter_name'] = chapter_title
                view_lecture_dict['resource_knobble_index'] = j + 1
                view_lecture_dict['resource_knobble_name'] = view_lecture_a.text
                view_lecture_dict['update_time'] = datetime.datetime.now()
                resource_structure_info.append(view_lecture_dict)
                res['resource_structure_info'] = resource_structure_info
                view_lecture_a.click()
                time.sleep(2)
                try:
                    main_note_scroll = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                        lambda x : x.find_element_by_css_selector('div.main.main-note-scroll')
                    )
                except selenium.common.exceptions.TimeoutException:
                    return res
                if len(main_note_scroll.find_elements_by_class_name('main-scroll-item')) == 0:
                    continue
                view_tab_learn_tab = main_note_scroll.find_elements_by_class_name('main-scroll-item')[1].find_element_by_css_selector('div.view-tab.learn-tab')
                tab_inners_a = view_tab_learn_tab.find_elements_by_css_selector('a.tab-inner')
                logging.info("tab_inners_a" + str(len(tab_inners_a)))
                for m in range(len(tab_inners_a)):
                    flag = False
                    logging.info(m)
                    logging.info(tab_inners_a[m].text.strip())
                    for n in range(len(video_type_list)):
                        if tab_inners_a[m].text.strip().endswith(video_type_list[n]):
                            flag = True
                            break
                    if flag:
                        tab_inners_a[m].click()
                        time.sleep(10)
                        video = view_tab_learn_tab.find_element_by_tag_name('video')
                        resource_dict = dict()
                        resource_dict['resource_id'] = str(uuid.uuid1())
                        resource_dict['resource_name'] = tab_inners_a[m].text.strip()
                        resource_dict['resource_type'] = 1
                        resource_dict['resource_structure_id'] = view_lecture_dict.get('resource_structure_id')
                        resource_dict['resource_teacherid'] = ""
                        resource_dict['resource_time'] = 0
                        resource_dict['resource_storage_location'] = view_lecture_dict.get("resource_structure_id") + "/" + resource_dict.get("resource_name")
                        resource_dict['resource_network_location'] = video.get_attribute('src')
                        resource_dict['update_time'] = datetime.datetime.now()
                        resource_info.append(resource_dict)
                self.__driver.back()
        return res



def main():
    options = selenium.webdriver.ChromeOptions()
    browser = selenium.webdriver.Chrome(options=options)
    browser.get('https://www.cnmooc.org/home/login.mooc')
    input_name = browser.find_element_by_id('loginName')
    input_name.send_keys('ghliang88@163.com')
    input_pass = browser.find_element_by_id('password')
    input_pass.send_keys('123ghl456')
    login = browser.find_element_by_id('userLogin')
    login.click()
    print(browser.current_url)
    time.sleep(2)
    print(browser.current_url)
    introjs_button = browser.find_element_by_class_name('introjs-skipbutton')
    introjs_button.click()
    time.sleep(2)
    course = browser.find_element_by_partial_link_text('课程')
    course.click()
    time.sleep(2)
    try:
        course_list = WebDriverWait(browser, 10).until(
            lambda x: x.find_element_by_xpath("//div[@class='main']//ul/li[3]//h3/a"))
    except selenium.common.exceptions.TimeoutException:
        print("结束")
    course_list.click()
    time.sleep(2)
    forumFetcher = ForumFetcher(browser)
    print(forumFetcher.run())



if __name__ == '__main__':
    main()