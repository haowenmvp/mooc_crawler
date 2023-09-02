import uuid
import logging
import datetime

import selenium.common.exceptions

from typing import List
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.remote.webdriver import WebDriver, WebElement

from persistence.model.forum import ForumPostInfo
from .. import BaseFetcher


class ForumFetcher(BaseFetcher):
    kTimePatterns = ['%Y-%m-%d %H:%M', '%Y-%m-%d', '%m-%d %H:%M']

    def __init__(self, driver: WebDriver):
        super().__init__(driver)
        self.forum_info_list = list()

    def run(self) -> dict:
        self.jump_to_forum_page()
        self.get_forum_info()

        res = dict()
        forum_post_info_list = [info.__dict__ for info in self.forum_info_list]
        res['forum_post_info'] = forum_post_info_list
        return res

    def jump_to_forum_page(self):
        try:
            btn = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_xpath('//*[@id="li_coursebbs"]'))
            btn.click()
            WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_id('topicAll'))
        except selenium.common.exceptions.TimeoutException:
            logging.error("[jump_to_forum_page] btn element not found.")

    def get_forum_info(self):
        pos = -1
        forum_elem, pos = self.get_next_forum_elem(pos)

        while forum_elem:
            if len(forum_elem.find_elements_by_class_name('kc_answer_bnt')) == 0:
                res = self.get_simple_forum_info(forum_elem)
                self.forum_info_list.extend(res)
            else:
                try:
                    btn = forum_elem.find_element_by_class_name('kc_answer_bnt')
                    window_num = len(self.driver.window_handles)
                    btn.click()
                    WebDriverWait(self.driver, self.kWaitSecond).until(
                        lambda x: len(x.window_handles) > window_num
                    )

                    self.driver.switch_to.window(self.driver.window_handles[-1])
                    res = self.get_detail_forum_info()
                    self.forum_info_list.extend(res)
                except Exception as e:
                    logging.error('[get_forum_info] get forum detail failed. err: [%s]', e)
                finally:
                    self.driver.close()
                    self.driver.switch_to.window(self.driver.window_handles[-1])

            forum_elem, pos = self.get_next_forum_elem(pos)
            logging.info('[get_forum_info] get forum num: [%s]', len(self.forum_info_list))
            # TODO forum limit
            if len(self.forum_info_list) > 3000:
                logging.info('[get_forum_info] forum num: [%s] reached limit 3000, break', len(self.forum_info_list))
                break

    def get_simple_forum_info(self, forum_elem: WebElement) -> List[ForumPostInfo]:
        res = list()
        forum = ForumPostInfo()
        forum.forum_post_id = str(uuid.uuid1())
        forum.forum_id = ''
        forum.forum_name = ''
        forum.forum_post_type = ForumPostInfo.TopicTypeEnum.kTypeMain
        forum.forum_reply_id = 0
        forum.forum_reply_userid = 0
        forum.forum_post_username = forum_elem.find_element_by_class_name('kc_mutual_student').\
            find_elements_by_tag_name('span')[0].text.strip()
        forum.forum_post_userrole = ForumPostInfo.OwnerRoleTypeEnum.kTypeUnknown

        forum_title = forum_elem.find_element_by_class_name('kc_mutual_ask').\
            find_element_by_class_name('kc_mutual_ask_name').text.strip()
        forum_content = forum_elem.find_element_by_class_name('kc_mutual_ask').\
            find_element_by_class_name('kc_mutual_ask_text').text.strip()
        forum.forum_post_content = forum_title + '\n' + forum_content

        time_text = forum_elem.find_element_by_class_name('kc_mutual_right'). \
            find_element_by_tag_name('span').text.strip()
        forum.forum_post_time = self.parse_time(time_text)
        forum.update_time = datetime.datetime.now()

        res.append(forum)

        try:
            answer_elem = forum_elem.find_element_by_class_name('kc_mutual_answer')
        except selenium.common.exceptions.NoSuchElementException:
            return res

        answer_box_list = answer_elem.find_elements_by_class_name('kc_answer_box')
        for answer_box in answer_box_list:
            try:
                person = answer_box.find_element_by_tag_name('span').text.strip()
                content = answer_box.find_element_by_tag_name('dt').text.strip()[len(person):]
            except selenium.common.exceptions.NoSuchElementException:
                continue

            answer_forum = ForumPostInfo()
            answer_forum.forum_post_id = str(uuid.uuid1())
            answer_forum.forum_id = ''
            answer_forum.forum_name = ''
            answer_forum.forum_post_type = ForumPostInfo.TopicTypeEnum.kTypeReply
            answer_forum.forum_reply_id = forum.forum_post_id
            answer_forum.forum_reply_userid = ''
            answer_forum.forum_post_username = person[: -1]
            answer_forum.forum_post_userrole = ForumPostInfo.OwnerRoleTypeEnum.kTypeUnknown
            answer_forum.forum_post_content = content
            answer_forum.update_time = datetime.datetime.now()

            res.append(answer_forum)

        return res

    def get_detail_forum_info(self) -> List[ForumPostInfo]:
        res = list()
        main_topic_elem = self.driver.find_element_by_xpath('/html/body/div[2]/div[2]/div[1]')
        main_forum = ForumPostInfo()
        main_forum.forum_post_id = str(uuid.uuid1())
        main_forum.forum_id = ''
        main_forum.forum_name = ''
        main_forum.forum_post_type = ForumPostInfo.TopicTypeEnum.kTypeMain
        main_forum.forum_reply_id = ''
        main_forum.forum_reply_userid = ''

        main_topic_detail_elem = main_topic_elem.find_element_by_class_name('oneRight')
        main_forum.forum_post_username = main_topic_detail_elem.find_element_by_class_name('oneTop').\
            find_element_by_class_name('name').text.strip()

        if '/images/group/t.png' in main_topic_elem.find_elements_by_tag_name('img')[1].get_attribute('src'):
            main_forum.forum_post_userrole = ForumPostInfo.OwnerRoleTypeEnum.kTeacher
        elif '/images/group/s.png' in main_topic_elem.find_elements_by_tag_name('img')[1].get_attribute('src'):
            main_forum.forum_post_userrole = ForumPostInfo.OwnerRoleTypeEnum.kStudent
        else:
            main_forum.forum_post_userrole = ForumPostInfo.OwnerRoleTypeEnum.kTypeUnknown

        main_forum_title = main_topic_detail_elem.find_element_by_tag_name('h3').\
            find_element_by_tag_name('span').text.strip()
        main_forum_content = main_topic_detail_elem.find_element_by_id('topicContent').text.strip()
        main_forum.forum_post_content = main_forum_title + '\n' + main_forum_content

        time_text = main_topic_detail_elem.find_element_by_class_name('oneTop').\
            find_element_by_class_name('gray').text.strip()
        main_forum.post_time = self.parse_time(time_text)
        main_forum.update_time = datetime.datetime.now()
        res.append(main_forum)

        reply_box = self.driver.find_element_by_xpath('/html/body/div[2]/div[2]/div[2]')
        reply_elem_list = reply_box.find_elements_by_class_name('SecondDiv')
        pos = 0
        while True:
            reply_elem = reply_elem_list[pos]
            first_reply_forum = ForumPostInfo()
            first_reply_forum.forum_post_id = str(uuid.uuid1())
            first_reply_forum.forum_id = ''
            first_reply_forum.forum_name = ''
            first_reply_forum.forum_post_type = ForumPostInfo.TopicTypeEnum.kTypeReply
            first_reply_forum.forum_reply_id = main_forum.forum_post_id
            first_reply_forum.forum_reply_userid = ''

            reply_detail_elem = reply_elem.find_element_by_class_name('secondRight')
            first_reply_forum.forum_post_username = reply_detail_elem.find_element_by_class_name('oneTop').\
                find_element_by_class_name('name').text.strip()

            if '/images/group/t.png' in reply_elem.find_elements_by_tag_name('img')[1].get_attribute('src'):
                first_reply_forum.owner_role = ForumPostInfo.OwnerRoleTypeEnum.kTeacher
            elif '/images/group/s.png' in reply_elem.find_elements_by_tag_name('img')[1].get_attribute('src'):
                first_reply_forum.owner_role = ForumPostInfo.OwnerRoleTypeEnum.kStudent
            else:
                first_reply_forum.owner_role = ForumPostInfo.OwnerRoleTypeEnum.kTypeUnknown

            first_reply_forum.forum_post_content = reply_detail_elem.find_element_by_tag_name('h3').text.strip()

            time_text = reply_detail_elem.find_element_by_class_name('oneTop').\
                find_element_by_class_name('gray').text.strip()
            first_reply_forum.post_time = self.parse_time(time_text)
            first_reply_forum.update_time = datetime.datetime.now()

            res.append(first_reply_forum)

            second_reply_elem_list = reply_detail_elem.find_elements_by_class_name('btCon')
            for second_reply_elem in second_reply_elem_list:
                detail_elem = second_reply_elem.find_element_by_class_name('tiOne')
                second_reply_forum = ForumPostInfo()
                second_reply_forum.forum_post_id = str(uuid.uuid1())
                second_reply_forum.forum_id = ''
                second_reply_forum.forum_name = ''
                second_reply_forum.forum_post_type = ForumPostInfo.TopicTypeEnum.kTypeReply
                second_reply_forum.forum_reply_id = first_reply_forum.forum_post_id
                second_reply_forum.forum_reply_userid = ''
                second_reply_forum.forum_post_username = detail_elem.find_element_by_class_name('name').text.strip()
                second_reply_forum.forum_post_userrole = ForumPostInfo.OwnerRoleTypeEnum.kTypeUnknown
                second_reply_forum.forum_post_content = detail_elem.find_element_by_name('replyfirstname').text.strip()
                time_text = detail_elem.find_elements_by_tag_name('span')[2].text.strip()
                second_reply_forum.post_time = self.parse_time(time_text)
                second_reply_forum.update_time = datetime.datetime.now()
                res.append(second_reply_forum)

            pos += 1
            if pos >= len(reply_elem_list):
                more_btn_list = reply_box.find_elements_by_class_name('allMore')
                if not more_btn_list:
                    break
                more_btn = more_btn_list[0]
                if more_btn.get_attribute('style') == 'display: none;':
                    break
                else:
                    more_btn.click()
                    reply_elem_list = WebDriverWait(reply_box, self.kWaitSecond).until(
                        lambda x: x.find_elements_by_class_name('SecondDiv')
                        if len(x.find_elements_by_class_name('SecondDiv')) > pos else None)

        return res

    def get_next_forum_elem(self, now_index) -> (WebElement, int):
        index = now_index + 1
        try:
            topic_list = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_id('topicAll').find_elements_by_class_name('kc_mutual_row'))
        except selenium.common.exceptions.TimeoutException:
            logging.error('[get_next_forum_elem] Cannot load topic list')
            return None, -1

        if len(topic_list) > index:
            return topic_list[index], index

        try:
            btn = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_id('getMoreTopic'))
        except selenium.common.exceptions.TimeoutException:
            logging.warning('[get_next_forum_elem] Cannot load button, maybe has finished')
            return None, -1

        # self.driver.execute_script("document.getElementById('getMoreTopic').click()")
        btn.click()

        topic_div = self.driver.find_element_by_id('topicAll')

        try:
            topic_list = WebDriverWait(topic_div, self.kWaitSecond).until(
                lambda x: x.find_elements_by_class_name('kc_mutual_row')
                if len(x.find_elements_by_class_name('kc_mutual_row')) > len(topic_list) else None
            )
        except selenium.common.exceptions.TimeoutException:
            logging.warning("[get_next_forum_elem] can not load more topic.")
            return None, -1

        if len(topic_list) > index:
            return topic_list[index], index
        else:
            return None, -1

    def parse_time(self, time_text: str) -> datetime.datetime:
        res = datetime.datetime(1999, 1, 1)
        for time_pattern in self.kTimePatterns:
            try:
                res = datetime.datetime.strptime(time_text, time_pattern)
                break
            except ValueError:
                continue

        if res == datetime.datetime(1999, 1, 1):
            logging.warning('[parse_time] time parse failed. text: [%s]', time_text)

        if res.year == 1900:
            return datetime.datetime(datetime.datetime.now().year, res.month, res.day, res.hour, res.minute, res.second)
        else:
            return res

