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

from crawler.fetcher import BaseFetcher


class ForumFetcher(BaseFetcher):
    def run(self) -> dict:
        return self.get_forum_info()

    def get_forum_info(self) -> dict:
        res = dict()
        res_list = list()
        res['forum_post_info'] = res_list
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
                return res
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
            discussion_a = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                lambda x: x.find_element_by_xpath("//a[@title='讨论区']"))
        except selenium.common.exceptions.TimeoutException:
            return res
        discussion_a.click()
        time.sleep(2)
        try:
            discussions_a = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_elements_by_xpath("//a[@class='link-default']"))
        except selenium.common.exceptions.TimeoutException:
            return res
        print(len(discussions_a))
        for i in range(len(discussions_a)):
            print(i)
            try:
                discussion_len = WebDriverWait(self.driver, self.kWaitSecond).until(
                    lambda x: x.find_elements_by_css_selector('td.td03'))
                print(len(discussion_len))
            except selenium.common.exceptions.TimeoutException:
                print("//td["+str(i)+"][@class='td03']")
                return res
            if discussion_len[i].text == '0':
                continue
            else:
                try:
                    discussion_a = WebDriverWait(self.driver, self.kWaitSecond).until(
                        lambda x: x.find_elements_by_xpath("//a[@class='link-default']"))
                except selenium.common.exceptions.TimeoutException:
                    return res
                discussion_a[i].click()
                time.sleep(2)
                try:
                    thread_view = WebDriverWait(self.driver, self.kWaitSecond).until(
                        lambda x: x.find_element_by_id('thread_view'))
                except selenium.common.exceptions.TimeoutException:
                    self.driver.back()
                    time.sleep(2)
                trs = thread_view.find_elements_by_tag_name('tr')
                for j in range(len(trs)):
                    try:
                        thread_view = WebDriverWait(self.driver, self.kWaitSecond).until(
                            lambda x: x.find_element_by_id('thread_view'))
                    except selenium.common.exceptions.TimeoutException:
                        self.driver.back()
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
                        postMain = WebDriverWait(self.driver, self.kWaitSecond).until(
                            lambda x: x.find_element_by_id('postMain'))
                    except selenium.common.exceptions.TimeoutException:
                        self.driver.back()
                        time.sleep(2)
                    reply_items = postMain.find_elements_by_css_selector('li.reply-item')
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
                                self.driver.execute_script("$(arguments[0]).click()", reply_intro2_a)
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
                    self.driver.back()
                    time.sleep(2)
                self.driver.back()
                time.sleep(2)
        res['forum_post_info'] = res_list
        return res