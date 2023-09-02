import os
import sys
import json
import time
import logging
import subprocess
import selenium.common
import selenium.webdriver
import selenium.webdriver.remote.webdriver
from selenium.webdriver.support.ui import WebDriverWait

kTargetUrl = 'https://www.icourse163.org/learn/HIT-1001515007'
kRootUrl = 'https://www.icourse163.org/'
kLoginUrl = 'https://www.icourse163.org/passport/sns/doOAuth.htm?snsType=6&oauthType=login&returnUrl=aHR0cHM6Ly93d3cuaWNvdXJzZTE2My5vcmcvaW5kZXguaHRtP2Zyb209c3R1ZHk='

kCourseInfoSuffix = {
    'announce': '#/learn/announce',
    'score': '#/learn/score',
    'content': '#/learn/content',
    'testlist': '#/learn/testlist',
    'examlist': '#/learn/examlist',
    'forumindex': '#/learn/forumindex',
}


class Bot:
    def __init__(self,
                 driver: selenium.webdriver.remote.webdriver.WebDriver,
                 qr_path: str,
                 cookie_path: str,
                 logger: logging.Logger):
        self.__driver = driver
        self.__qr_path = qr_path
        self.__cookie_path = cookie_path
        self.__logger = logger

    def login(self):
        return self.__cookie_login() or self.__qr_login()

    def get_course_info(self, course_url):
        self.__driver.get(course_url)
        self.__get_tab_list()

    def get_course_announce(self, course_url):
        self.__driver.get(course_url + kCourseInfoSuffix['announce'])

        while True:
            div_list = WebDriverWait(self.__driver, 10).until(
                lambda x:
                x.find_element_by_class_name('m-notice')
                    .find_element_by_class_name('j-list')
                    .find_elements_by_class_name('noitce'))

            need_load = False
            for div in div_list:
                if div.find_element_by_class_name('j-showmore').text.find('点击载入更多') != -1:
                    div.find_element_by_class_name('j-showmore').click()
                    need_load = True
                    break
            if not need_load:
                break

        for div in div_list:
            info = div.find_element_by_class_name('noticeitem')
            title = info.find_element_by_class_name('detailtitlpage').text
            detail = info.find_element_by_class_name('j-detailcntout').text
            date = info.find_element_by_class_name('lptimeout').text
            print('title: ' + title)
            print('date: ' + date)
            print('text:')
            print(detail)

    def get_course_video_links(self, course_url):
        self.__driver.get(course_url + kCourseInfoSuffix['content'])

        div_content_box = WebDriverWait(self.__driver, 5).until(
            lambda x: x.find_element_by_class_name('m-learnChapterList')
        )
        div_content_box = self.__driver.find_element_by_class_name('m-learnChapterList')

        div_content_list = div_content_box.find_elements_by_class_name('m-learnChapterNormal')

        self.__logger.info('Got Chapter List: %d', len(div_content_list))

        for div_content in div_content_list:
            if div_content.find_element_by_class_name('j-down').get_attribute('style') == 'display: none;':
                div_content.click()
                self.__logger.info('titleBox of [%s] clicked.', div_content.find_element_by_class_name('titleBox').text)

            div_lesson_list = div_content.find_element_by_class_name('lessonBox').find_elements_by_class_name('normal')

            self.__logger.info('Got Lesson List: %d', len(div_lesson_list))
            for div_lesson in div_lesson_list:
                div_lesson.click()
                time.sleep(3)
                break
            break

        div_selects = self.__driver.find_elements_by_class_name('u-select')
        div_chapter_select = div_selects[0]
        div_lesson_select = div_selects[1]

        num_chapter = len(div_chapter_select.find_elements_by_class_name('f-thide'))

        result = []
        for i in range(num_chapter):
            div_chapter_select.click()
            div_chapter_select.find_elements_by_class_name('f-thide')[i].click()

            num_lesson = len(div_lesson_select.find_elements_by_class_name('f-thide'))

            for j in range(num_lesson):
                div_lesson_select.click()
                div_lesson_select.find_elements_by_class_name('f-thide')[j].click()
                time.sleep(1)

                try:
                    div_video = WebDriverWait(self.__driver, 3).until(
                        lambda x: x.find_element_by_class_name('j-mainVideo')
                    )

                    url_video = div_video.find_element_by_tag_name('source').get_attribute('src').split('?')[0]
                    self.__logger.info('Chapter [%s] Lesson [%s] Url: %s', div_chapter_select.text, div_lesson_select.text, url_video)
                    result.append((div_chapter_select.text, div_lesson_select.text, url_video))
                except selenium.common.exceptions.TimeoutException:
                    self.__logger.warning("Timeout.")
        return result

    def download_videos(self, video_info, path):
        os.mkdir(path)
        for chapter, lesson, url in video_info:
            file_name = path + '/' + chapter + '_' + lesson
            self.__logger.info('Downloading %s', file_name + '.mp4')
            subprocess.run(['wget', '-O', file_name + '.mp4', url], capture_output=True)
            self.__logger.info('Separating voice %s', file_name + '.wav')
            subprocess.run(
                ['ffmpeg', '-i', file_name + '.mp4',
                 '-f', 'wav', '-ar', '8000', '-acodec', 'pcm_s16le',
                 file_name + '.wav',
                 '-y'],
                capture_output=True)

    def get_course_forum(self, course_url):
        self.__driver.get(course_url + kCourseInfoSuffix['forumindex'])

        div_pages = WebDriverWait(self.__driver, 5).until(
            lambda x: x.find_elements_by_class_name('zpgi'))

        div_pages = self.__driver.find_elements_by_class_name('zpgi')
        max_page_num = 0
        for page_num in div_pages:
            if len(page_num.text) != 0:
                max_page_num = max(max_page_num, int(page_num.text))
        self.__logger.info('Forum pages: %d', max_page_num)

        result = {}
        for i in range(max_page_num):
            self.__logger.info('Collecting forum page: %d', i + 1)
            forum_list = WebDriverWait(self.__driver, 5).until(
                lambda x: x.find_elements_by_class_name('u-forumli'))
            for forum in forum_list:
                result[forum.find_element_by_class_name('j-link').text] = \
                    forum.find_element_by_class_name('j-link').get_attribute('href')
            self.__driver.find_elements_by_class_name('znxt')[0].click()
        return result

    def get_forum_info(self, forum_url):
        self.__driver.get(forum_url)

        self.__driver.implicitly_wait(3)
        forum_place = self.__driver.find_element_by_class_name('u-forumbreadnav').find_elements_by_tag_name('a')[1].text
        div_main = self.__driver.find_element_by_class_name('j-post')
        title = div_main.find_element_by_class_name('j-title').text
        content = div_main.find_element_by_class_name('j-content').text
        div_info = self.__driver.find_element_by_class_name('infobar')
        forum_user = div_info.find_element_by_class_name('j-name').text
        forum_date = div_info.find_element_by_class_name('j-time').text

        print('place: ' + forum_place)
        print('title: ' + title)
        print('content: ' + content)
        print('user: ' + forum_user)
        print('date: ' + forum_date)

        div_base_reply = self.__driver.find_element_by_class_name('j-reply-all')

        div_reply_list = div_base_reply.find_elements_by_class_name('m-detailInfoItem')
        for div_reply in div_reply_list:
            reply_content = div_reply.find_element_by_class_name('j-content').text
            reply_user = div_reply.find_element_by_class_name('userInfo').find_element_by_tag_name('a').text
            try:
                reply_user_type = div_reply.find_element_by_class_name('userInfo').find_element_by_tag_name('span').text
                reply_is_teacher = True
            except selenium.common.exceptions.NoSuchElementException:
                reply_is_teacher = False
            print('reply content: ' + reply_content)
            print('reply user: ' + reply_user)
            print('reply is_teacher: ' + str(reply_is_teacher))

        print(len(div_reply_list))

        pass

    def __cookie_login(self):
        if not os.path.exists(self.__cookie_path):
            self.__logger.warning('Cookies file not found')
            return False
        self.__logger.info('Cookies found at %s', self.__cookie_path)

        with open(self.__cookie_path, 'r', encoding='utf-8') as fp:
            cookies_json = fp.read()
        self.__logger.debug(cookies_json)

        cookies = json.loads(cookies_json, encoding='utf-8')
        self.__driver.get(kRootUrl)
        for cookie in cookies:
            self.__driver.add_cookie(cookie)
        return True

    def __qr_login(self):
        self.__driver.get(kLoginUrl)

        while self.__driver.title.find('登录') == -1:
            self.__logger.info('Waiting for login page')
            self.__driver.implicitly_wait(1)
        self.__driver.save_screenshot(self.__qr_path)

        # Waiting for User. After scanned QR code, browser will redirect automatically
        while self.__driver.title.find('中国大学MOOC') == -1:
            self.__logger.info('Waiting for User scan QR code')
            time.sleep(1)

        cookie_json = json.dumps(self.__driver.get_cookies())
        with open(self.__cookie_path, 'w', encoding='utf-8') as fp:
            fp.write(cookie_json)

        return True

    def __get_tab_list(self):
        tab_list = self.__driver.find_element_by_id('j-courseTabList').find_elements_by_class_name('j-tabitem')
        for one_tab in tab_list:
            print(one_tab.text)


def main(argv: list):
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

    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'
    options = selenium.webdriver.ChromeOptions()
    #options.add_argument('headless')
    options.add_argument(f'user-agent={user_agent}')
    browser = selenium.webdriver.Chrome(options=options)
    bot = Bot(browser, 'test.png', 'test.json', get_logger('test.log'))
#    bot.login()
#   bot.get_course_announce(kTargetUrl)
#   bot.get_forum_info('https://www.icourse163.org/learn/HIT-1001515007#/learn/forumdetail?pid=1212261030')
    urls = bot.get_course_video_links(kTargetUrl)
    print(urls)
    # res = bot.get_course_forum(kTargetUrl)
    # print(res)
    browser.close()
    # bot.download_videos(urls, 'test')


if __name__ == '__main__':
    main(sys.argv)
