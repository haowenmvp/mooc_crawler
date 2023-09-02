import datetime
import json
import logging
import os
import urllib.parse
import uuid
import subprocess

import selenium.common.exceptions

from .basic_fetcher import BaseFetcher
from typing import List, Dict

from selenium.webdriver.support.ui import WebDriverWait

from persistence.model.resource import ResourceInfo, ResourceStructureInfo


class VideoFetcher(BaseFetcher):

    def run(self) -> dict:
        res = dict()
        res['resource_info'] = self.get_video_infos()
        return res

    def get_video_infos(self) -> List[dict]:
        self.enter_video_page()

        res = list()
        while True:
            link = self.get_mp4_video_link()
            if not link:
                link = self.get_m3u8_video_link()

            if not link:
                logging.warning('[get_video_infos] cannot get video link.')
            else:
                chapter_text = self.driver.find_element_by_class_name('j-chapter').find_element_by_class_name('up').text
                lesson_text = self.driver.find_element_by_class_name('j-lesson').find_element_by_class_name('up').text
                res_info = ResourceInfo()
                res_info.resource_id = uuid.uuid1()
                res_info.resource_name = '[%s] %s' % (chapter_text, lesson_text)
                res_info.resource_type = ResourceInfo.ResourceTypeEnum.kVideo
                res_info.resource_teacher = ''
                try:
                    duration = WebDriverWait(self.driver, 6 * self.kWaitSecond).until(
                        lambda x: x.execute_script('return document.getElementsByTagName("video")[0].duration'))
                except selenium.common.exceptions.TimeoutException:
                    logging.warning("[get_video_infos] video element not load in %s seconds. skip", 6 * self.kWaitSecond)
                    duration = 0
                res_info.resource_time = duration / 60
                res_info.resource_network_location = link
                res_info.update_time = datetime.datetime.now()
                res.append(res_info.__dict__)

            if not self.jump_next_video():
                break

        return res

    def enter_video_page(self):
        try:
            div_content_list = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x:
                x.find_element_by_class_name('m-learnChapterList').find_elements_by_class_name('m-learnChapterNormal')
            )
        except selenium.common.exceptions.TimeoutException:
            logging.error("[get_video_infos] cannot get chapter list")
            raise TimeoutError('[get_video_infos] cannot get chapter list')

        # find a entry to video page
        for div_content in div_content_list:
            if div_content.find_element_by_class_name('j-down').get_attribute('style') == 'display: none;':
                div_content.click()
                logging.info('[get_video_infos] titleBox of [%s] clicked.', div_content.find_element_by_class_name('titleBox').text)

            try:
                div_lesson_list = div_content.find_element_by_class_name('lessonBox').find_elements_by_class_name('normal')
            except selenium.common.exceptions.TimeoutException:
                logging.warning("[get_video_infos] cannot get lesson list at %s",
                                div_content.find_element_by_class_name('titleBox').text)
                continue

            logging.info('[get_video_infos] Got Lesson List: %d', len(div_lesson_list))
            for div_lesson in div_lesson_list:
                # found
                div_lesson.click()
                break
            break

    def jump_next_video(self) -> bool:
        return self.jump_next_unit() or self.jump_next_lesson() or self.jump_next_chapter()

    def jump_next_unit(self) -> bool:
        try:
            div_unit_list = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_class_name('j-unitslist').find_elements_by_tag_name('li'))
        except selenium.common.exceptions.TimeoutException:
            logging.error("[jump_next_video] cannot get unit list")
            raise TimeoutError('[jump_next_video] cannot get unit list')
        pos = 0
        for div_unit in div_unit_list:
            if 'current' in div_unit.get_attribute('class'):
                break
            else:
                pos += 1
        pos += 1
        for i in range(pos, len(div_unit_list)):
            div_span = div_unit_list[i].find_element_by_tag_name('span')
            if 'video' in div_span.get_attribute('class'):
                break
            else:
                pos += 1
        if pos < len(div_unit_list):
            div_unit_list[pos].click()
            return True
        return False

    def jump_next_lesson(self) -> bool:
        # find next lesson of this chapter
        try:
            div_lesson_list = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_class_name('j-lesson'))
        except selenium.common.exceptions.TimeoutException:
            logging.error("[jump_next_video] cannot get lesson list")
            raise TimeoutError('[jump_next_video] cannot get lesson list')

        now_lesson = div_lesson_list.find_element_by_class_name('up')
        now_lesson.click()
        lesson_list = div_lesson_list.find_element_by_class_name('down').find_elements_by_class_name('list')
        pos = 0
        for lesson in lesson_list:
            if now_lesson.text == lesson.text:
                break
            else:
                pos += 1
        pos += 1
        if pos < len(lesson_list):
            lesson_list[pos].click()
            return True
        return False

    def jump_next_chapter(self) -> bool:
        # find next chapter
        try:
            div_chapter_list = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_class_name('j-chapter'))
        except selenium.common.exceptions.TimeoutException:
            logging.error("[jump_next_video] cannot get chapter list")
            raise TimeoutError('[jump_next_video] cannot get chapter list')
        now_chapter = div_chapter_list.find_element_by_class_name('up')
        now_chapter.click()
        chapter_list = div_chapter_list.find_element_by_class_name('down').find_elements_by_class_name('list')
        pos = 0
        for chapter in chapter_list:
            now_chapter_text = ''.join(now_chapter.text.split(' '))
            chapter_text = ''.join(chapter.text.split(' '))
            if now_chapter_text == chapter_text:
                break
            else:
                pos += 1
        pos += 1
        if pos < len(chapter_list):
            chapter_list[pos].click()
            # choose first lesson
            try:
                WebDriverWait(self.driver, self.kWaitSecond).until(
                    lambda x: x.find_element_by_class_name('j-lesson').find_element_by_class_name('up').text == '请选择课时'
                )
            except selenium.common.exceptions.TimeoutException:
                logging.error("[jump_next_video] cannot load new lesson list")
                return False

            self.driver.find_element_by_class_name('j-lesson').find_element_by_class_name('up').click()
            self.driver.find_element_by_class_name('j-lesson').find_element_by_class_name('down'). \
                find_elements_by_class_name('list')[0].click()
            return True
        return False

    def get_mp4_video_link(self) -> str:
        try:
            div_video = WebDriverWait(self.driver, self.kWaitSecond).until(
                lambda x: x.find_element_by_tag_name('video')
            )

            url_video = div_video.find_element_by_tag_name('source').get_attribute('src')
        except selenium.common.exceptions.TimeoutException:
            logging.warning("[get_mp4_video_link] find video elem Timeout.")
            return ''
        except selenium.common.exceptions.NoSuchElementException:
            logging.info("[get_mp4_video_link] cannot found mp4 source.")
            return ''

        url_parts = urllib.parse.urlparse(url_video)
        if url_parts.path.endswith('.mp4'):
            logging.info('[get_mp4_video_link] get mp4 at [%s]', url_video)
            return url_video
        else:
            logging.info('[get_mp4_video_link] video is not mp4 format')
            return ''

    def get_m3u8_video_link(self) -> str:
        logs = [json.loads(log['message'])['message'] for log in self.driver.get_log('performance')]
        urls = self.get_request_url_from_logs(logs)
        for url in urls:
            url_parts = urllib.parse.urlparse(url)
            if url_parts.path.endswith('.m3u8'):
                logging.info('[get_m3u8_video_link] get m3u8 at [%s]', url)
                return url
        logging.info('[get_m3u8_video_link] video is not m3u8 format')
        return ''

    @classmethod
    def get_request_url_from_logs(cls, logs: List[Dict]) -> List[str]:
        res = list()
        for log in logs:
            method = log.get('method')
            if not method or method != 'Network.requestWillBeSent':
                continue
            params = log.get('params')
            if not params or not isinstance(params, dict):
                logging.warning("[get_request_url_from_logs] cannot get param. log: [%s]", log)
                continue
            request = params.get('request')
            if not request or not isinstance(request, dict):
                logging.warning("[get_request_url_from_logs] cannot get request. params: [%s]", params)
                continue
            url = request.get('url')
            if not url or not isinstance(url, str):
                logging.warning("[get_request_url_from_logs] cannot get url. request: [%s]", request)
                continue
            res.append(url)
        return res

    @classmethod
    def download_mp4_file(cls, url, path, filename):
        os.mkdir(path)
        filename = path + '/' + filename
        logging.info('[download_mp4_file] Downloading %s', filename)
        subprocess.run(['wget', '-O', filename, url], capture_output=True)

    @classmethod
    def download_m3u8_file(cls, url, path, filename):
        os.mkdir(path)
        filename = path + '/' + filename
        logging.info('[download_m3u8_file] Downloading %s', filename)
        subprocess.run(['ffmpeg', '-i', url, filename], capture_output=True)
