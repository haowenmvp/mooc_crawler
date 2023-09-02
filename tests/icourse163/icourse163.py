#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import datetime
import uuid
import logging
import settings
import selenium.webdriver
import selenium.webdriver.remote.webdriver
from selenium.webdriver.support.ui import WebDriverWait

from utils import parse_time

kTargetUrl = 'https://www.icourse163.org/learn/HIT-1001515007'
kRootUrl = 'https://www.icourse163.org/'
kLoginUrl = 'https://www.icourse163.org/passport/sns/doOAuth.htm?snsType=6&oauthType=login&returnUrl=aHR0cHM6Ly93d3cuaWNvdXJzZTE2My5vcmcvaW5kZXguaHRtP2Zyb209c3R1ZHk='


class ForumFetcher:
    def run(self) -> dict:
        res = dict()
        res['forum_post_info'] = self.get_forum_info()
        return res

    def time_utils(self, type: int, time_str: str):
        time_str1 = ''
        if type == 1:
            time_str1 = time_str[1:5] + '-' + time_str[6:8] + '-' + time_str[9:11]
            return datetime.datetime.strptime(time_str1, '%Y-%m-%d')
        elif type == 2:
            if len(time_str.split("-")) > 1:
                return datetime.datetime.strptime(time_str, '%Y-%m-%d')
            if len(time_str.split(":")) > 1:
                hour = int(time_str.split(":")[0])
                minute = int(time_str.split(":")[1])
                return datetime.datetime.now().replace(hour=hour, minute=minute)
            if time_str.find("分钟") >= 0:
                minute = datetime.datetime.now().minute
                minute_diff = minute - int(time_str[0:time_str.find("分钟")])
                if minute_diff > 0:
                    return datetime.datetime.now().replace(minute=minute_diff)
                else:
                    return datetime.datetime.now().replace(minute=0)
            month_index = time_str.find("月")
            month = int(time_str[0:month_index])
            day = int(time_str[month_index+1:-1])
            return datetime.datetime.now().replace(month=month,day=day)

    def get_forum_info(self) -> list:
        res = list()
        self.__driver.get("http://www.icourse163.org/course/LZU-152001")
        '''
        try:
            WebDriverWait(self.__driver, self.__kWaitSecond).until(
                lambda x: x.find_element_by_css_selector("div.course-enroll-info_course-info_term-info_term-progress.unstarted")
            )
        except selenium.common.exceptions.TimeoutException:
            print("未开课")
            return res
        '''
        try:
            # TODO 貌似Headless模式被反爬了，这个地方headless加载不上
            main_page = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                lambda x: x.find_element_by_css_selector("span.ux-btn.ux-btn-.ux-btn-w220"))
        except selenium.common.exceptions.TimeoutException:
            return res
        if '老师已关闭该学期，无法查看' == main_page.text.strip():
            return res
        main_page.click()
        time.sleep(10)
        try:
             notice = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                lambda x: x.find_element_by_partial_link_text("公告"))
        except selenium.common.exceptions.TimeoutException:
            return res
        notice.click()
        time.sleep(1)

        try:
            discuss = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                lambda x: x.find_element_by_partial_link_text("讨论区"))
        except selenium.common.exceptions.TimeoutException:
            return res
        discuss.click()
        time.sleep(2)
        #self.__driver.get("https://www.icourse163.org/learn/SWJTU-1001908004?tid=1207120209#/learn/forumindex")
        while True:
            try:
                discuss_div = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                    lambda x: x.find_element_by_css_selector("div.u-forumlistwrap.j-alltopiclist").find_element_by_css_selector("div.m-data-lists.f-cb.f-pr.j-data-list"))
            except selenium.common.exceptions.TimeoutException:
                return res
            lis = discuss_div.find_elements_by_tag_name("li")
            for i in range(len(lis)):
                main_post = dict()
                try:
                    discuss_div = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                        lambda x: x.find_element_by_css_selector("div.u-forumlistwrap.j-alltopiclist").find_element_by_css_selector("div.m-data-lists.f-cb.f-pr.j-data-list"))
                except selenium.common.exceptions.TimeoutException:
                    return res
                li = discuss_div.find_elements_by_tag_name("li")[i]
                userinfo = li.find_elements_by_css_selector("span.userInfo.j-userInfo")[0]
                try:
                    userinfo.find_element_by_css_selector("span.type.lector")
                    main_post['forum_post_userrole'] = 1
                except selenium.common.exceptions.NoSuchElementException:
                    main_post['forum_post_userrole'] = 2
                try:
                    name_a = userinfo.find_element_by_tag_name("a")
                    main_post['forum_post_username'] = name_a.get_attribute('title').strip()
                except selenium.common.exceptions.NoSuchElementException:
                    main_post['forum_post_username'] = '匿名发表'
                    main_post['forum_post_userrole'] = 0
                reply_time = li.find_elements_by_css_selector("span.lb10.f-fc9")[0]
                main_post['forum_post_time'] = self.time_utils(1, reply_time.text.strip())
                discuss_a = li.find_element_by_css_selector("a.f-fc3.f-f0.lb10.j-link")
                discuss_a.click()
                time.sleep(2)
                try:
                    WebDriverWait(self.__driver, self.__kWaitSecond).until(
                        lambda x: x.find_element_by_class_name('j-post')
                    )
                except selenium.common.exceptions.TimeoutException:
                    logging.error('[ForumFetcher.get_forum_detail] Cannot load post div')

                main_post['forum_post_id'] = str(uuid.uuid1())
                main_post['forum_id'] = ''
                main_post['forum_name'] = self.__driver.find_element_by_xpath(
                    '//*[@id="courseLearn-inner-box"]/div/div[1]/div/a[2]').text
                div_main = self.__driver.find_element_by_class_name('j-post')
                main_post['forum_subject'] = div_main.find_element_by_class_name('j-title').text
                main_post['forum_post_content'] = div_main.find_element_by_class_name('j-content').text
                main_post['forum_post_type'] = 1
                main_post['forum_reply_id'] = '0'
                main_post['forum_reply_userid'] = ''
                main_post['update_time'] = datetime.datetime.now()
                print(main_post)
                res.append(main_post)
                try:
                    div_reply_list = self.__driver.find_elements_by_xpath("//*[@id='courseLearn-inner-box']/div/div[2]/div/div[4]/div/div[1]/div[1]/div")
                except selenium.common.exceptions.NoSuchElementException:
                    continue
                # TODO 二级回复/多页回复未处理
                for div_reply_all in div_reply_list:
                    div_reply = div_reply_all.find_element_by_class_name("m-detailInfoItem")
                    reply_content = div_reply.find_element_by_class_name('j-content').text
                    post_reply['forum_post_userrole'] = 4
                    try:
                        reply_user = div_reply.find_element_by_class_name('userInfo').find_element_by_tag_name('a').text
                    except selenium.common.exceptions.NoSuchElementException:
                        reply_user = div_reply.find_element_by_class_name('anonyInfo').text
                        post_reply['forum_post_userrole'] = 0
                    try:
                        div_reply.find_element_by_class_name('userInfo').find_element_by_css_selector("span.type.lector")
                        reply_is_teacher = True
                    except selenium.common.exceptions.NoSuchElementException:
                        reply_is_teacher = False
                    reply_time = div_reply.find_element_by_class_name('j-time').text

                    post_reply = dict()
                    post_reply['forum_post_id'] = str(uuid.uuid1())
                    post_reply['forum_id'] = ''
                    post_reply['forum_name'] = main_post['forum_name']
                    post_reply['forum_subject'] = main_post.get("forum_subject")
                    post_reply['forum_post_type'] = 2
                    post_reply['forum_reply_id'] = main_post['forum_post_id']
                    post_reply['forum_reply_userid'] = ''
                    post_reply['forum_post_username'] = reply_user
                    # TODO User role type should be a enum val
                    if post_reply.get("forum_post_userrole") != 0:
                        post_reply['forum_post_userrole'] = 1 if reply_is_teacher else 2
                    post_reply['forum_post_content'] = reply_content
                    post_reply['forum_post_time'] = self.time_utils(2, reply_time)
                    post_reply['update_time'] = datetime.datetime.now()
                    res.append(post_reply)
                    print(post_reply)
                    comment_a = div_reply.find_element_by_css_selector("a.f-fr.f-fc9.opt.cmtBtn.j-cmtBtn")
                    if comment_a.text.strip()[3:-1] != '0':

                        #self.__driver.execute_script("arguments[0].scrollIntoView();", comment_a)
                        comment_a.click()

                        #self.__driver.execute_script("$(arguments[0]).click()", comment_a)
                        time.sleep(2)
                        try:
                            div_reply2 = div_reply_all.find_element_by_class_name("m-commentWrapper")
                        except selenium.common.exceptions.NoSuchElementException:
                            continue
                        m_data_lists = div_reply2.find_element_by_class_name("m-data-lists")
                        m_divs = m_data_lists.find_elements_by_class_name("m-detailInfoItem")
                        for j in range(len(m_divs)):
                            post_reply1 = dict()
                            post_reply1['forum_post_id'] = str(uuid.uuid1())
                            post_reply1['forum_id'] = ''
                            post_reply1['forum_name'] = main_post['forum_name']
                            post_reply1['forum_subject'] = main_post.get("forum_subject")
                            post_reply1['forum_post_type'] = 2
                            post_reply1['forum_reply_id'] = post_reply1['forum_post_id']
                            post_reply1['forum_reply_userid'] = ''
                            # TODO User role type should be a enum val
                            post_reply1['forum_post_userrole'] = 4
                            m_div = m_data_lists.find_elements_by_class_name("m-detailInfoItem")[j]
                            try:
                                m_div.find_element_by_css_selector("span.type.lector")
                                post_reply1['forum_post_userrole'] = 1
                            except selenium.common.exceptions.NoSuchElementException:
                                post_reply1['forum_post_userrole'] = 2
                            try:
                                m_div.find_element_by_css_selector("a.f-fcgreen")
                            except selenium.common.exceptions.NoSuchElementException:
                                post_reply1['forum_post_userrole'] = 0

                            reply_content = m_div.find_element_by_class_name("f-richEditorText").text.strip()
                            if post_reply1.get("forum_post_userrole") == 0:
                                reply_user = '匿名发表'
                            else:
                                reply_user = m_div.find_element_by_css_selector("a.f-fcgreen").get_attribute("title")
                            reply_time = m_div.find_element_by_css_selector("div.f-fl.f-fc9.time.j-time").text.strip()
                            post_reply1['forum_post_username'] = reply_user
                            post_reply1['forum_post_content'] = reply_content
                            post_reply1['forum_post_time'] = self.time_utils(2, reply_time)
                            post_reply1['update_time'] = datetime.datetime.now()
                            print(post_reply1)
                            res.append(post_reply1)
                self.__driver.back()
                time.sleep(2)
            try:
                next_page = WebDriverWait(self.__driver, self.__kWaitSecond).until(
                    lambda x: x.find_element_by_partial_link_text("下一页"))
            except selenium.common.exceptions.TimeoutException:
                return res
            if next_page.get_attribute('class') == 'zbtn znxt js-disabled':
                break
            else:
                next_page.click()
                time.sleep(2)
        print(len(res))
        return res

    def __init__(self,
                 driver: selenium.webdriver.remote.webdriver.WebDriver, wait_time=10):
        self.__driver = driver
        self.__kWaitSecond = wait_time


def main():
    print(datetime.datetime.now())
    start_time = datetime.datetime.now().timestamp()
    options = selenium.webdriver.ChromeOptions()
    browser = selenium.webdriver.Chrome(options=options)
    browser.get('https://www.icourse163.org/')
    login = browser.find_element_by_partial_link_text("登录")
    login.click()
    ux_login = browser.find_element_by_class_name("ux-login-set-login-set-panel")
    email_login = ux_login.find_elements_by_tag_name("li")[1]
    email_login.click()
    time.sleep(2)
    j_urs_container = ux_login.find_element_by_id("j-ursContainer-0")
    iframe1 = WebDriverWait(j_urs_container, 5).until(
        lambda x: x.find_element_by_tag_name("iframe"))
    browser.switch_to.frame(iframe1)
    login_form = browser.find_element_by_id("login-form")
    username = login_form.find_elements_by_class_name("inputbox")[0].find_elements_by_tag_name("input")[0]
    username.send_keys("ghliang88@163.com")
    password = login_form.find_elements_by_class_name("inputbox")[1].find_elements_by_tag_name("input")[1]
    password.send_keys("G1&h2&l3")
    login1 = login_form.find_element_by_partial_link_text("登录")
    login1.click()
    time.sleep(2)
    course = browser.find_elements_by_class_name("m-slideTop-cateFunc-f")[0]
    course.click()
    time.sleep(5)
    windows = browser.window_handles
    browser.switch_to_window(windows[-1])

    courses = browser.find_element_by_xpath("//*[@id='app']/div/div/div[2]/div[1]/div[2]/div/div/div/ul/div[1]")
    time.sleep(2)
    courses.click()
    windows = browser.window_handles
    browser.switch_to_window(windows[-1])
    forum = ForumFetcher(browser)
    print(forum.run())
    print(datetime.datetime.now())
    end_time = datetime.datetime.now().timestamp()
    print((end_time - start_time) / 60.0)

if __name__ == '__main__':
    main()
