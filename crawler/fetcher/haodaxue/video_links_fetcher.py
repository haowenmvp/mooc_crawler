#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from crawler.fetcher import BaseFetcher
import selenium.webdriver
import selenium.webdriver.remote.webdriver
from selenium.webdriver.support.ui import WebDriverWait
import time
import json
import datetime
import uuid
import logging


class VideoLinksFetcher(BaseFetcher):
    def run(self) -> dict:
        return self.get_video_resource_info()

    # {"resource_structure_info" : {}, "resource_info" : []}
    def get_video_resource_info(self) -> dict:
        video_type_list = ['.avi', '.rmvb', '.rm', '.asf',  '.divx' , '.mpg', '.mpeg', '.mpe', '.wmv', '.mp4', '.mkv', '.vob', '.flv']
        res = dict()
        resource_structure_info = list()
        resource_info = list()
        res['resource_structure_info'] = resource_structure_info
        res['resource_info'] = resource_info
        try:
            view_intro = WebDriverWait(self.driver, self.kWaitSecond).until(
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
                userselectstudy_div = WebDriverWait(self.driver, self.kWaitSecond).until(
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
                btn_public_studysession = WebDriverWait(self.driver, self.kWaitSecond).until(
                    lambda x: x.find_element_by_class_name('view-intro').find_element_by_partial_link_text('进入学习'))
            except selenium.common.exceptions.TimeoutException:
                return res
            btn_public_studysession.click()
            time.sleep(2)
        except selenium.common.exceptions.NoSuchElementException:
            try:
                join_study = view_intro.find_element_by_partial_link_text('进入学习')
                join_study.click()
                time.sleep(2)
                logging.info("进入学习")
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
            discussion_a = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_xpath("//a[@title='章节导航']"))
        except selenium.common.exceptions.TimeoutException:
            return res
        discussion_a.click()
        time.sleep(2)
        try:
            main_chapter_nav = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x : x.find_element_by_css_selector('div.main.chapter-nav')
            )
        except selenium.common.exceptions.TimeoutException:
            return res
        unitNavigation = main_chapter_nav.find_element_by_id('unitNavigation')
        view_chapters = unitNavigation.find_elements_by_class_name('view-chapter')
        for i in range(len(view_chapters)):
            try:
                view_chapter = WebDriverWait(self.driver, self.kWaitSecond).until(
                    lambda x: x.find_element_by_css_selector('div.main.chapter-nav').find_element_by_id('unitNavigation').find_elements_by_class_name('view-chapter')[i]
                )
            except selenium.common.exceptions.TimeoutException:
                return res
            chapter_title = view_chapter.find_element_by_css_selector('span.chapter-text.substr').text
            chapter_index = i + 1
            view_lectures = view_chapter.find_elements_by_class_name('view-lecture')
            for j in range(len(view_lectures)):
                try:
                    view_chapter = WebDriverWait(self.driver, self.kWaitSecond).until(
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
                    main_note_scroll = WebDriverWait(self.driver, self.kWaitSecond).until(
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
                self.driver.back()
        return res