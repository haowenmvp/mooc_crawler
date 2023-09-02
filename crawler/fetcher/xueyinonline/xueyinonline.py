import re
import datetime

import selenium.common.exceptions

from typing import List, Tuple
from logging import Logger
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.remote.webdriver import WebDriver, WebElement
from model import CourseInfo, ForumLog, VideoInfo

from crawlers.base import BaseCrawler

kWaitSecond = 5
kTimePatterns = ['%Y-%m-%d %H:%M', '%Y-%m-%d', '%m-%d %H:%M']


class XueYinOnlineCrawler(BaseCrawler):
    def __init__(self, driver: WebDriver, logger: Logger):
        """
        :param driver: A driver opened course page already.
        :param logger: Logger
        """
        super().__init__(driver, logger)
        self.course_url = driver.current_url
        self.course_info = CourseInfo()
        self.course_standard_info = CourseInfo()
        self.video_info_list = list()
        self.forum_info_list = list()

    def run(self) -> (CourseInfo, CourseInfo, List[VideoInfo], List[ForumLog]):
        """
        :return: (CourseInfo, [VideoInfo, ...], [ForumLog, ...])
        """
        # self.course_info.term_id = term_id
        self.course_info.platform = '北京学银在线教育科技有限公司'

        self.logger.info('[run] jumping to main page')
        self.jump_to_main_page()

        self.logger.info('[run] getting teacher and school')
        teacher, school = self.get_teacher_school()
        self.course_info.school = school

        self.logger.info('[run] jumping to course info page')
        self.jump_to_course_info_page()
        self.logger.info('[run] getting course detail')
        self.get_course_info_detail()

        self.logger.info('[run] jumping to main page')
        self.jump_to_main_page()

        # chapter_name, teacher_name, url = self.jump_to_video_page()

        # while len(url) != 0:
        #     self.logger.info("[get_course_info] requesting chapter[%s], url [%s]", chapter_name, url)
        #     self.driver.get(url)
        #     self.get_video_document_homework_info(teacher_name, chapter_name)
        #     chapter_name, teacher_name, url = self.get_next_video_document_work()

        # self.driver.close()
        # self.driver.switch_to.window(self.driver.window_handles[-1])

        self.logger.info('[run] jumping to main page')
        self.jump_to_main_page()
        self.logger.info('[run] switch to forum page')
        self.jump_to_forum_page()
        self.logger.info('[run] getting forum data')
        self.get_forum_info()

        return self.course_info, None, self.video_info_list, self.forum_info_list

    def jump_to_main_page(self):
        self.driver.get(self.course_url)

    def select_term(self):
        term_list = self.driver.find_element_by_xpath(
            '/html/body/div/div[2]/div[2]/dl/dd[2]/span[1]/span').find_elements_by_tag_name('a')
        for term in term_list:
            if str(self.course_info.term_id) in term.text:
                self.driver.get(term.get_attribute('href'))
                break

    def get_teacher_school(self) -> (str, str):
        try:
            info_div = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_xpath('/html/body/div/div[2]/div[2]/dl/dd[1]'))
        except selenium.common.exceptions.TimeoutException:
            self.logger.error("[get_teacher_school] element not found.")
            return '', ''
        cut_pos_1 = info_div.text.find('：')
        cut_pos_2 = info_div.text.rfind('/')

        return info_div.text[cut_pos_1 + 1: cut_pos_2], info_div.text[cut_pos_2 + 1:]

    def get_start_end_date(self) -> (datetime.date, datetime.date):
        try:
            span_elem = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_xpath('/html/body/div/div[2]/div[1]/span[1]'))
        except selenium.common.exceptions.TimeoutException:
            self.logger.error("[get_start_end_date] element not found.")
            return None

        text = span_elem.text
        cut_pos = text.find('：')
        text = text[cut_pos + 1:]
        cut_pos = text.find('至')
        start_date_text = text[: cut_pos].strip()
        end_date_text = text[cut_pos + 1:].strip()

        return datetime.datetime.strptime(start_date_text, "%Y-%m-%d").date(), \
               datetime.datetime.strptime(end_date_text, "%Y-%m-%d").date()

    def jump_to_course_info_page(self):
        try:
            btn = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_xpath('//*[@id="course_info"]/a[3]'))
            href = btn.get_attribute('href')
            self.driver.get(href)
        except selenium.common.exceptions.TimeoutException:
            self.logger.error('[jump_to_course_info_page] cannot find jump btn.')
            raise selenium.common.exceptions.NoSuchElementException

    def get_course_info_detail(self):
        start_date, end_date = self.get_start_end_date()
        self.course_standard_info.start_date = start_date
        self.course_standard_info.end_date = end_date

        try:
            resource_info_web_table = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_xpath('/html/body/div/div[2]/div[2]/table/tbody'))
            resource_info_table = list()
            rows = resource_info_web_table.find_elements_by_tag_name('tr')
            for row in rows:
                resource_info_table.append(row.find_elements_by_tag_name('td'))

            self.course_standard_info.video_num = int(resource_info_table[0][2].text.strip()[:-1])
            self.course_standard_info.during_second_total = int(resource_info_table[0][4].text.strip()[: -2]) * 60
            self.course_standard_info.homework_questions_total = int(resource_info_table[1][1].text.strip()[: -1])
            self.course_standard_info.exam_questions_total = int(resource_info_table[1][3].text.strip()[: -1])
            self.course_standard_info.documents_num = int(resource_info_table[2][2].text.strip()[:-1])
            self.course_standard_info.announces_num = int(resource_info_table[2][4].text.strip()[:-1])
        except selenium.common.exceptions.TimeoutException:
            self.logger.error("[get_course_info_detail] resource element not found.")

        try:
            student_num_elem = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_xpath('//*[@id="allChooseCount"]'))
            cut_pos = student_num_elem.text.rfind('：')
            self.course_standard_info.students_num = int(student_num_elem.text[cut_pos + 1:])
        except selenium.common.exceptions.TimeoutException:
            self.logger.error("[get_course_info_detail] student element not found.")

        try:
            study_info_elem = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_xpath('//*[@id="stuStatisticsCount"]/div[2]/p[3]'))
            pattern = re.compile(r'统计：任务点视频共计(\d+)个,非任务点视频(\d+)个; 测验和作业共计(\d+)次,共有(\d+)人参与', re.I)
            data = pattern.findall(study_info_elem.text)
            if len(data) == 0:
                self.logger.error("[get_course_info_detail] cannot find study info in [%s]", study_info_elem.text)
            else:
                data = data[0]
                self.course_standard_info.homework_total = int(data[2])
                self.course_standard_info.homework_students_total = int(data[3])
        except selenium.common.exceptions.TimeoutException:
            self.logger.error("[get_course_info_detail] study element not found.")

        try:
            forum_info_elem = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_xpath('//*[@id="bbsData"]/div[2]/p[3]'))
            pattern = re.compile(r'统计：共计发帖总数(\d+)帖，其中教师发帖数：(\d+) ,参与互动人数：(\d+)', re.I)
            data = pattern.findall(forum_info_elem.text)
            if len(data) == 0:
                self.logger.error("[get_course_info_detail] cannot find forum info in [%s]", forum_info_elem.text)
            else:
                data = data[0]
                self.course_standard_info.forum_topic_num = int(data[0])
                self.course_standard_info.teacher_topic_num = int(data[1])
                self.course_standard_info.interaction_people_total = int(data[2])

            exam_info_elem = self.driver.find_element_by_xpath('//*[@id="clean_paperLibrary"]/p[2]')
            pattern = re.compile(r'统计：当前期次共计(\d+)次考试，累计参与考试(\d+)人次，共计(\d+)人参与', re.I)
            data = pattern.findall(exam_info_elem.text.strip())
            if len(data) == 0:
                self.logger.error("[get_course_info_detail] cannot find exam info in [%s]", exam_info_elem.text)
            else:
                data = data[0]
                self.course_standard_info.exam_total = int(data[0])
                self.course_standard_info.exam_students_total = int(data[2])
        except selenium.common.exceptions.TimeoutException:
            self.logger.error("[get_course_info_detail] forum element not found.")

        try:
            pass_info_elem = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_xpath('/html/body/div/div[8]/p'))
            pattern = re.compile(r'总人数：(\d+) 通过人数:(\d+) 通过率：([\d\\.]+)%', re.I)
            data = pattern.findall(pass_info_elem.text.strip())
            if len(data) == 0:
                self.logger.error("[get_course_info_detail] cannot find pass info in [%s]", pass_info_elem.text)
            else:
                data = data[0]
                self.course_standard_info.exam_students_passed = int(data[1])
        except selenium.common.exceptions.TimeoutException:
            self.logger.error("[get_course_info_detail] pass element not found.")
        except Exception as e:
            self.logger.error(e)

    def get_students_num(self) -> int:
        result = 0
        try:
            span_elem = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_xpath('/html/body/div/div[3]/div[2]/dl/dt'))
        except selenium.common.exceptions.TimeoutException:
            self.logger.error("[get_students_num] element not found.")
            return result

        text = span_elem.text
        try:
            result = int(text)
        except ValueError:
            self.logger.error("[get_students_num] elem text is not a number: [%s].", text)

        return result

    def jump_to_video_page(self) -> Tuple[str, str, str]:
        try:
            btn = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_xpath('/html/body/div/div[4]/ul/li[2]'))
            btn.click()
        except selenium.common.exceptions.TimeoutException:
            self.logger.error("[jump_to_video_page] dir element not found.")
            return '', '', ''

        try:
            btn = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_xpath('/html/body/div/div[5]/div[2]/div[2]/div[1]/div[3]/div/ul/li[1]/a'))
            btn.click()
        except selenium.common.exceptions.TimeoutException:
            self.logger.error("[jump_to_video_page] element not found.")
            return '', '', ''

        self.driver.implicitly_wait(kWaitSecond)
        self.driver.switch_to.window(self.driver.window_handles[-1])

        chapter_name = self.driver.find_element_by_xpath('//*[@id="caNavContent"]/div/div[1]/div[1]/div[2]/a').text
        teacher_name = self.driver.find_element_by_xpath('//*[@id="navBox"]/div[1]/div/div/div/div[2]/span').text

        return chapter_name, teacher_name, self.driver.current_url

    def get_next_video_document_work(self) -> Tuple[str, str, str]:
        """
        :return: (chapter_name, teacher_name, video_page_url). And return ('', '', '') if no more.
        """
        teacher_name = self.driver.find_element_by_xpath('//*[@id="navBox"]/div[1]/div/div/div/div[2]/span').text

        # get next link
        js_script = """
        var now_node = document.getElementById('courseChapterSelected').parentElement;
        var next_node = now_node.nextElementSibling;
        
        while (next_node != null && next_node.className != "rel") {
            next_node = next_node.nextElementSibling;
        }

        if (next_node != null) {
            return next_node;
        }
        
        next_parent = now_node.parentElement.nextElementSibling;
        
        while (next_parent != null && next_parent.tagName != "DIV") {
            next_parent = next_parent.nextElementSibling;
        }
        
        if (next_parent == null) {
            return null;
        }
        
        next_node = next_parent.children[0];
        while (next_node != null && next_node.className != "rel") {
            next_node = next_node.nextElementSibling;
        }
        next_node = next_node.nextElementSibling;

        return next_node;
        """

        next_rel = self.driver.execute_script(js_script)
        if next_rel is None:
            return '', '', ''

        href = ''
        now_url = self.driver.current_url
        node_item_list = next_rel.find_elements_by_class_name('nodeItem')
        for node_item in node_item_list:
            href = node_item.get_attribute('href')
            if href == now_url:
                continue
            else:
                break

        # get chapter name
        parent_div = next_rel.find_element_by_xpath('..')
        item_list = parent_div.find_elements_by_class_name('nodeItem')
        now_chapter_name = item_list[0].text.strip()

        return now_chapter_name, teacher_name, href

    def get_video_document_homework_info(self, teacher_name: str, chapter_name: str):
        try:
            now_chapter_elem = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_id('courseChapterSelected'))
        except selenium.common.exceptions.TimeoutException:
            self.logger.error("[get_video_document_homework_info] chapter selector not found")
            return None

        chapter_info_elem_list = now_chapter_elem.find_elements_by_class_name('wh')
        title = chapter_info_elem_list[1].text.strip()

        try:
            elem_list = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_elements_by_class_name('ans-attach-ct'))
        except selenium.common.exceptions.NoSuchElementException:
            self.logger.error("[get_video_document_homework_info] data elements not found")
            return None
        except selenium.common.exceptions.TimeoutException:
            self.logger.warning("[get_video_document_homework_info] section [%s] data elements not found", title)
            return None

        self.logger.debug("[get_video_document_homework_info] num of ans-attach-ct: %s", len(elem_list))
        for elem in elem_list:
            iframe = elem.find_element_by_tag_name('iframe')
            iframe_src = iframe.get_attribute('src')
            if 'video' in iframe_src:
                self.driver.switch_to.frame(iframe)
                video_during_second = self.get_video_during_second()
                self.driver.switch_to.parent_frame()

                video_info = VideoInfo()
                video_info.during_second = video_during_second
                video_info.teacher_name = teacher_name
                video_info.title = title
                video_info.chapter_id = int(chapter_info_elem_list[0].text.split('.')[0])
                video_info.chapter_name = chapter_name
                video_info.section_id = int(chapter_info_elem_list[0].text.split('.')[1])
                video_info.section_name = title
                video_info.resource_name = str(len(self.video_info_list))
                self.video_info_list.append(video_info)

                self.course_info.during_second_total += video_during_second
                self.course_info.video_num += 1
                self.logger.debug("[get_video_document_homework_info] video during second: %s", video_during_second)
                continue
            if 'pdf' in iframe_src or 'ppt' in iframe_src:
                self.course_info.documents_num += 1
                self.logger.debug("[get_video_document_homework_info] num of course documents: %s",
                                  self.course_info.documents_num)
                continue
            if 'work' in iframe_src:
                self.driver.switch_to.frame(iframe)
                question_num = self.get_work_question_num()
                self.driver.switch_to.parent_frame()
                self.logger.debug("[get_video_document_homework_info] num of questions: %s", question_num)

                if '测试' in title or '测验' in title or '考试' in title:
                    self.course_info.exam_total += 1
                    self.course_info.exam_questions_total += question_num
                else:
                    self.course_info.homework_total += 1
                    self.course_info.homework_questions_total += question_num
                continue
        self.logger.info("[get_video_document_homework_info] section [%s] collected", title)

    def get_video_during_second(self) -> int:
        try:
            play_btn = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_class_name('vjs-big-play-button'))
            play_btn.click()
        except selenium.common.exceptions.TimeoutException:
            self.logger.error("[get_video_during_second] play button not found")
            return 0

        try:
            duration = WebDriverWait(self.driver, 60).until(
                lambda x: x.execute_script('return document.getElementById("video_html5_api").duration'))
        except selenium.common.exceptions.TimeoutException:
            self.logger.warning("[get_video_during_second] video element not load in 60s. skip")
            return 0

        self.logger.debug("[get_video_during_second] time: %s", duration)

        return int(duration)

    def get_work_question_num(self):
        try:
            iframe = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_id('frame_content'))
            self.driver.switch_to.frame(iframe)

            question_elem_list = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_elements_by_class_name('TiMu'))
        except selenium.common.exceptions.TimeoutException:
            self.logger.error("[get_work_question_num] question elements not found")
            return 0
        finally:
            self.driver.switch_to.parent_frame()

        return len(question_elem_list)

    def jump_to_forum_page(self):
        try:
            btn = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_xpath('//*[@id="li_coursebbs"]'))
            btn.click()
            WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_id('topicAll'))
        except selenium.common.exceptions.TimeoutException:
            self.logger.error("[jump_to_forum_page] btn element not found.")

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
                    WebDriverWait(self.driver, kWaitSecond).until(
                        lambda x: len(x.window_handles) > window_num
                    )

                    self.driver.switch_to.window(self.driver.window_handles[-1])
                    res = self.get_detail_forum_info()
                    self.forum_info_list.extend(res)
                except Exception as e:
                    self.logger.error('[get_forum_info] get forum detail failed. err: [%s]', e)
                finally:
                    self.driver.close()
                    self.driver.switch_to.window(self.driver.window_handles[-1])

            forum_elem, pos = self.get_next_forum_elem(pos)
            self.logger.info('[get_forum_info] get forum num: [%s]', len(self.forum_info_list))
            # TODO forum limit
            if len(self.forum_info_list) > 3000:
                self.logger.info('[get_forum_info] forum num: [%s] reached limit 3000, break', len(self.forum_info_list))
                break

    def get_simple_forum_info(self, forum_elem: WebElement) -> List[ForumLog]:
        res = list()
        forum = ForumLog()
        forum.id = len(self.forum_info_list)
        forum.owner = forum_elem.find_element_by_class_name('kc_mutual_student').\
            find_elements_by_tag_name('span')[0].text.strip()
        time_text = forum_elem.find_element_by_class_name('kc_mutual_right').\
            find_element_by_tag_name('span').text.strip()
        forum.time = self.parse_time(time_text)

        forum.title = forum_elem.find_element_by_class_name('kc_mutual_ask').\
            find_element_by_class_name('kc_mutual_ask_name').text.strip()
        forum.content = forum_elem.find_element_by_class_name('kc_mutual_ask').\
            find_element_by_class_name('kc_mutual_ask_text').text.strip()
        forum.TopicType = forum.TopicTypeEnum.kTypeMain

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

            answer_forum = ForumLog()
            answer_forum.id = len(self.forum_info_list) + len(res)
            answer_forum.owner = person[: -1]
            answer_forum.content = content
            answer_forum.TopicType = answer_forum.TopicTypeEnum.kTypeReply
            answer_forum.upper_topic_id = len(self.forum_info_list)
            answer_forum.main_topic_id = len(self.forum_info_list)

            res.append(answer_forum)

        return res

    def get_detail_forum_info(self) -> List[ForumLog]:
        res = list()
        main_topic_elem = self.driver.find_element_by_xpath('/html/body/div[2]/div[2]/div[1]')
        main_forum = ForumLog()
        main_forum.id = len(self.forum_info_list)

        if '/images/group/t.png' in main_topic_elem.find_elements_by_tag_name('img')[1].get_attribute('src'):
            main_forum.owner_role = ForumLog.OwnerRoleTypeEnum.kTeacher
        elif '/images/group/s.png' in main_topic_elem.find_elements_by_tag_name('img')[1].get_attribute('src'):
            main_forum.owner_role = ForumLog.OwnerRoleTypeEnum.kStudent

        main_topic_detail_elem = main_topic_elem.find_element_by_class_name('oneRight')
        main_forum.owner = main_topic_detail_elem.find_element_by_class_name('oneTop').\
            find_element_by_class_name('name').text.strip()
        time_text = main_topic_detail_elem.find_element_by_class_name('oneTop').\
            find_element_by_class_name('gray').text.strip()
        main_forum.time = self.parse_time(time_text)

        main_forum.title = main_topic_detail_elem.find_element_by_tag_name('h3').\
            find_element_by_tag_name('span').text.strip()
        main_forum.content = main_topic_detail_elem.find_element_by_id('topicContent').text.strip()
        main_forum.TopicType = ForumLog.TopicTypeEnum.kTypeMain
        res.append(main_forum)

        reply_box = self.driver.find_element_by_xpath('/html/body/div[2]/div[2]/div[2]')
        reply_elem_list = reply_box.find_elements_by_class_name('SecondDiv')
        pos = 0
        while True:
            reply_elem = reply_elem_list[pos]
            first_reply_forum = ForumLog()
            first_reply_forum.id = main_forum.id + len(res)
            first_reply_forum.main_topic_id = main_forum.id
            first_reply_forum.upper_topic_id = main_forum.id

            if '/images/group/t.png' in reply_elem.find_elements_by_tag_name('img')[1].get_attribute('src'):
                first_reply_forum.owner_role = ForumLog.OwnerRoleTypeEnum.kTeacher
            elif '/images/group/s.png' in reply_elem.find_elements_by_tag_name('img')[1].get_attribute('src'):
                first_reply_forum.owner_role = ForumLog.OwnerRoleTypeEnum.kStudent

            reply_detail_elem = reply_elem.find_element_by_class_name('secondRight')
            first_reply_forum.owner = reply_detail_elem.find_element_by_class_name('oneTop').\
                find_element_by_class_name('name').text.strip()
            time_text = reply_detail_elem.find_element_by_class_name('oneTop').\
                find_element_by_class_name('gray').text.strip()
            first_reply_forum.time = self.parse_time(time_text)

            first_reply_forum.content = reply_detail_elem.find_element_by_tag_name('h3').text.strip()
            first_reply_forum.TopicType = ForumLog.TopicTypeEnum.kTypeReply
            res.append(first_reply_forum)

            second_reply_elem_list = reply_detail_elem.find_elements_by_class_name('btCon')
            for second_reply_elem in second_reply_elem_list:
                detail_elem = second_reply_elem.find_element_by_class_name('tiOne')
                second_reply_forum = ForumLog()
                second_reply_forum.id = main_forum.id + len(res)
                second_reply_forum.main_topic_id = main_forum.id
                second_reply_forum.upper_topic_id = first_reply_forum.id

                second_reply_forum.owner = detail_elem.find_element_by_class_name('name').text.strip()
                second_reply_forum.content = detail_elem.find_element_by_name('replyfirstname').text.strip()
                time_text = detail_elem.find_elements_by_tag_name('span')[2].text.strip()
                second_reply_forum.time = self.parse_time(time_text)
                second_reply_forum.TopicType = ForumLog.TopicTypeEnum.kTypeReply
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
                    reply_elem_list = WebDriverWait(reply_box, kWaitSecond).until(
                        lambda x: x.find_elements_by_class_name('SecondDiv')
                        if len(x.find_elements_by_class_name('SecondDiv')) > pos else None)

        return res

    def get_next_forum_elem(self, now_index) -> (WebElement, int):
        index = now_index + 1
        try:
            topic_list = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_id('topicAll').find_elements_by_class_name('kc_mutual_row'))
        except selenium.common.exceptions.TimeoutException:
            self.logger.error('[get_next_forum_elem] Cannot load topic list')
            return None, -1

        if len(topic_list) > index:
            return topic_list[index], index

        try:
            btn = WebDriverWait(self.driver, kWaitSecond).until(
                lambda x: x.find_element_by_id('getMoreTopic'))
        except selenium.common.exceptions.TimeoutException:
            self.logger.warning('[get_next_forum_elem] Cannot load button, maybe has finished')
            return None, -1

        # self.driver.execute_script("document.getElementById('getMoreTopic').click()")
        btn.click()

        topic_div = self.driver.find_element_by_id('topicAll')

        try:
            topic_list = WebDriverWait(topic_div, kWaitSecond).until(
                lambda x: x.find_elements_by_class_name('kc_mutual_row')
                if len(x.find_elements_by_class_name('kc_mutual_row')) > len(topic_list) else None
            )
        except selenium.common.exceptions.TimeoutException:
            self.logger.warning("[get_next_forum_elem] can not load more topic.")
            return None, -1

        if len(topic_list) > index:
            return topic_list[index], index
        else:
            return None, -1

    def parse_time(self, time_text: str) -> datetime.datetime:
        res = datetime.datetime(1999, 1, 1)
        for time_pattern in kTimePatterns:
            try:
                res = datetime.datetime.strptime(time_text, time_pattern)
                break
            except ValueError:
                continue

        if res == datetime.datetime(1999, 1, 1):
            self.logger.warning('[parse_time] time parse failed. text: [%s]', time_text)

        if res.year == 1900:
            return datetime.datetime(datetime.datetime.now().year, res.month, res.day, res.hour, res.minute, res.second)
        else:
            return res


def main():
    import sys
    import time
    import logging

    def get_logger(log_file):
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        fmt = logging.Formatter('[%(asctime)s] [%(threadName)s: %(thread)d] [%(filename)s:%(lineno)d] [%(levelname)s]: '
                                '%(message)s')
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.INFO)
        sh.setFormatter(fmt)
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)
        fh.setFormatter(fmt)
        logger.addHandler(sh)
        logger.addHandler(fh)

        return logger

    options = selenium.webdriver.ChromeOptions()
    #   options.add_argument('headless')
    browser = selenium.webdriver.Chrome(options=options)

    browser.get('http://www.chinaooc.cn')
    jump_btn = browser.find_element_by_xpath('/html/body/div/div[2]/a[2]')
    jump_btn.click()

    browser.find_element_by_xpath('//*[@id="loginName"]').send_keys("courseviewer")
    browser.find_element_by_xpath('//*[@id="password"]').send_keys("111111")

    while browser.current_url != "http://www.chinaooc.cn/front/ruler/rule_course.htm":
        pass
    print("Login success.")

    selector = WebDriverWait(browser, kWaitSecond).until(
        lambda x: x.find_element_by_id('course4Search.courseOnlineWeb'))
    Select(selector).select_by_visible_text('北京学银在线教育科技有限公司')
    browser.find_element_by_xpath('//*[@id="caseContentDiv"]/div/h3/span/button').click()

    WebDriverWait(browser, kWaitSecond).until(
        lambda x: x.find_element_by_xpath('//*[@id="caseContentDiv"]/table/tbody/tr[8]/td[11]/a[2]')).click()
    time.sleep(1)
    browser.switch_to.window(browser.window_handles[-1])

    WebDriverWait(browser, kWaitSecond).until(
        lambda x: x.find_element_by_xpath('//*[@id="form1"]/div/table[1]/tbody/tr[11]/td[5]/button')).click()
    time.sleep(1)
    browser.switch_to.window(browser.window_handles[-1])

    crawler = XueYinOnlineCrawler(browser, get_logger('xueyin.log'))
    res = crawler.run()
    browser.close()
    browser.switch_to.window(browser.window_handles[-1])
    print(res)
    time.sleep(10)


if __name__ == '__main__':
    main()
