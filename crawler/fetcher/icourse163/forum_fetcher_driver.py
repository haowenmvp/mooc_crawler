#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import time
import datetime
import uuid
import logging
import selenium.webdriver
import selenium.webdriver.remote.webdriver
from selenium.webdriver.support.ui import WebDriverWait
import traceback
from exceptions.offlineError import OfflineError
from crawler.fetcher import BaseFetcher


class ForumFetcher(BaseFetcher):
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
            day = int(time_str[month_index + 1:-1])
            return datetime.datetime.now().replace(month=month, day=day)

    def get_forum_info(self) -> list:
        res = list()
        # try:
        #     WebDriverWait(self.driver, self.kWaitSecond).until(
        #         lambda x: x.find_element_by_css_selector(
        #             "div.course-enroll-info_course-info_term-info_term-progress.unstarted")
        #     )
        # except selenium.common.exceptions.TimeoutException:
        #     return res
        # try:
        if len(self.driver.find_elements_by_css_selector(
                "div.course-enroll-info_course-info_term-info_term-progress.unstarted")) != 0:
            return res
        try:
            # TODO 貌似Headless模式被反爬了，这个地方headless加载不上
            main_page = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_css_selector("span.ux-btn.ux-btn-.ux-btn-w220"))
        except selenium.common.exceptions.TimeoutException:
            return res
        if '老师已关闭该学期，无法查看' == main_page.text.strip():
            return res
        main_page.click()
        time.sleep(5)
        try:
            notice = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_partial_link_text("公告"))
        except selenium.common.exceptions.TimeoutException:
            if len(self.driver.find_elements_by_css_selector(
                    "[class='mooc-login-set']")) != 0:
                logging.error("[ForumFetcher.get_forum_info] Find announcement time out, perhaps offline.")
                raise OfflineError('[Icourse163 crawler is offline.]')
            # return res
        notice.click()
        time.sleep(1)
        try:
            discuss = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_partial_link_text("讨论区"))
        except selenium.common.exceptions.TimeoutException:
            logging.error("[ForumFetcher.get_forum_info] Time Out.")
            return res
        discuss.click()
        time.sleep(2)
        page = 0
        while True:
            page += 1
            logging.info("[ForumFetcher.get_forum_info] Processing Page No.%d ", page)
            try:
                discuss_div = WebDriverWait(self.driver, self.kWaitSecond).until(
                    lambda x: x.find_element_by_css_selector(
                        "div.u-forumlistwrap.j-alltopiclist").find_element_by_css_selector(
                        "div.m-data-lists.f-cb.f-pr.j-data-list"))
            except selenium.common.exceptions.TimeoutException:
                logging.error("[ForumFetcher.get_forum_info] Time Out.")
                return res
            lis = discuss_div.find_elements_by_tag_name("li")
            for i in range(len(lis)):
                logging.info(
                    "[ForumFetcher.get_forum_info] Processing Page No.%d main_post No.%d , %d mainposts in total",
                    page, i + 1, len(lis))
                main_post = dict()
                loop = 0
                try:
                    while loop < 3:
                        try:
                            discuss_div = WebDriverWait(self.driver, self.kWaitSecond).until(
                                lambda x: x.find_element_by_css_selector(
                                    "div.u-forumlistwrap.j-alltopiclist").find_element_by_css_selector(
                                    "div.m-data-lists.f-cb.f-pr.j-data-list"))
                        except selenium.common.exceptions.TimeoutException:
                            logging.error("[ForumFetcher.get_forum_info] Time Out.")
                        try:
                            loop += 1
                            li = discuss_div.find_elements_by_tag_name("li")[i]
                            logging.info(
                                "[ForumFetcher.get_forum_info] Find [li] successfully after try %d times .", loop)
                            break
                        except Exception as e:
                            logging.error(e)
                            logging.warning(
                                "[ForumFetcher.get_forum_info] Find [li] failed at %d times, URL is %s, len is %d , No.%d",
                                loop,
                                self.driver.current_url,
                                len(discuss_div.find_elements_by_tag_name("li")), i)
                            self.driver.refresh()
                            continue
                except Exception as e:
                    logging.error("[ForumFetcher.get_forum_info] Failed three times. Ignore  No.%d mainpost. ", i)
                    continue
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
                    WebDriverWait(self.driver, self.kWaitSecond).until(
                        lambda x: x.find_element_by_class_name('j-post')
                    )
                except selenium.common.exceptions.TimeoutException:
                    logging.error('[ForumFetcher.get_forum_detail] Cannot load post div')

                main_post['forum_post_id'] = str(uuid.uuid1())
                main_post['forum_id'] = 0
                try:
                    main_post['forum_name'] = self.driver.find_element_by_xpath(
                        '//*[@id="courseLearn-inner-box"]/div/div[1]/div/a[2]').text
                    div_main = self.driver.find_element_by_class_name('j-post')
                    main_post['forum_subject'] = div_main.find_element_by_class_name('j-title').text
                    main_post['forum_post_content'] = div_main.find_element_by_class_name('j-content').text
                    main_post['forum_post_type'] = 1
                    main_post['forum_reply_id'] = '0'
                    main_post['forum_reply_userid'] = ''
                    main_post['update_time'] = datetime.datetime.now()
                    print("[ForumFetcher.get_forum_info] MAIN POST :%s", main_post)
                    res.append(main_post)
                except selenium.common.exceptions.NoSuchElementException:
                    self.driver.back()
                    time.sleep(2)
                    continue

                # TODO 二级回复/多页回复未处理
                while True:
                    try:
                        div_reply_list = self.driver.find_elements_by_xpath(
                            "//*[@id='courseLearn-inner-box']/div/div[2]/div/div[4]/div/div[1]/div[1]/div")
                    except selenium.common.exceptions.NoSuchElementException:
                        break
                    for div_reply_all in div_reply_list:
                        post_reply = dict()
                        div_reply = div_reply_all.find_element_by_class_name("m-detailInfoItem")
                        reply_content = div_reply.find_element_by_class_name('j-content').text
                        post_reply['forum_post_userrole'] = 4
                        try:
                            reply_user = div_reply.find_element_by_class_name('userInfo').find_element_by_tag_name(
                                'a').text
                        except selenium.common.exceptions.NoSuchElementException:
                            reply_user = div_reply.find_element_by_class_name('anonyInfo').text
                            post_reply['forum_post_userrole'] = 0
                        try:
                            div_reply.find_element_by_class_name('userInfo').find_element_by_css_selector(
                                "span.type.lector")
                            reply_is_teacher = True
                        except selenium.common.exceptions.NoSuchElementException:
                            reply_is_teacher = False
                        reply_time = div_reply.find_element_by_class_name('j-time').text

                        post_reply['forum_post_id'] = str(uuid.uuid1())
                        post_reply['forum_id'] = 0
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
                        # logging.info("POST RELPY :%s",post_reply)
                        comment_a = div_reply.find_element_by_css_selector("a.f-fr.f-fc9.opt.cmtBtn.j-cmtBtn")
                        if comment_a.text.strip()[3:-1] != '0':

                            # self.driver.execute_script("arguments[0].scrollIntoView();", comment_a)
                            comment_a.click()

                            # self.driver.execute_script("$(arguments[0]).click()", comment_a)
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
                                post_reply1['forum_id'] = 0
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
                                    reply_user = m_div.find_element_by_css_selector("a.f-fcgreen").get_attribute(
                                        "title")
                                reply_time = m_div.find_element_by_css_selector(
                                    "div.f-fl.f-fc9.time.j-time").text.strip()
                                post_reply1['forum_post_username'] = reply_user
                                post_reply1['forum_post_content'] = reply_content
                                post_reply1['forum_post_time'] = self.time_utils(2, reply_time)
                                post_reply1['update_time'] = datetime.datetime.now()
                                # print(post_reply1)
                                res.append(post_reply1)
                    try:
                        next_page1 = WebDriverWait(self.driver, self.kWaitSecond).until(
                            lambda x: x.find_element_by_partial_link_text("下一页"))
                    except selenium.common.exceptions.TimeoutException:
                        break
                    if next_page1.get_attribute('class') == 'zbtn znxt js-disabled':
                        break
                    else:
                        next_page1.click()
                        time.sleep(5)
                self.driver.back()
                time.sleep(2)
            try:
                next_page = WebDriverWait(self.driver, self.kWaitSecond).until(
                    lambda x: x.find_element_by_partial_link_text("下一页"))
            except selenium.common.exceptions.TimeoutException:
                # logging.error("[ForumFetcher.get_forum_info] Time Out.")
                return res
            if next_page.get_attribute('class') == 'zbtn znxt js-disabled':
                break
            else:
                next_page.click()
                time.sleep(2)

        # except BaseException as e:
        #     logging.error("[ForumFetcher.get_forum_info] %s", traceback.format_exc())
        #     return {}
        # # print(len(res))
        return res
