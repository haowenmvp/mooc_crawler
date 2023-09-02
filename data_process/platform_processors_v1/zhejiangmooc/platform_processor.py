import hashlib
import pickle
from datetime import datetime

import mysql.connector

from data_process.platform_processors.base_processor import BaseProcessor
from data_process.processor import Processor
from persistence.db.course_group_repo import CourseGroupRepository
from persistence.db.course_list_info_repo import CourseListInfoRepository
from persistence.db.platform_resource_repo import PlatformResourceRepository
from persistence.db.school_resource_repo import SchoolResourceRepository
from persistence.model.basic_info import CourseGroupInfo, CourseListInfo
from persistence.model.pipeline_config import PipelineConfig
from persistence.model.process_config import ProcessConfig
from crawler.fetcher.zhejiangmooc.list_fetcher import ListFetcher
from pipeline.pipelines import DataPipeline


class PlatformProcessor(BaseProcessor):
    def __init__(self, processor: Processor):
        super().__init__(processor)
        self.platform = "浙江省高等学校在线开放课程共享平台"
        self.__conn = mysql.connector.connect(host=self.processor.repo["host"], user=self.processor.repo["username"],
                                              password=self.processor.repo["password"],
                                              database=self.processor.repo["database"],
                                              port=self.processor.repo["port"])

    def init_table(self):
        sql = "select a.course_group_id,a.course_name,a.school,a.teacher,a.platform,a.status,a.term_id from course_list_info a \
        inner join (select course_group_id,max(term_id) as term_count from course_list_info where platform=\"浙江省高等学校在线开放课程共享平台\" and course_group_id != '' group by course_group_id)b \
        on a.course_group_id=b.course_group_id and a.term_id =b.term_count"
        res = self.processor.course_list_repo.fetch_all(sql)
        courses = []
        for course in res:
            course_group = CourseGroupInfo()
            course_group.course_group_id = course[0]
            course_group.course_name = course[1]
            course_group.school = course[2]
            course_group.teacher = course[3]
            course_group.platform = course[4]
            course_group.status = 0 if course[5] == 0 else 1
            course_group.term_count = course[6]
            course_group.update_time = datetime.now()
            courses.append(course_group)
        self.processor.course_group_repo.create_course_groups(courses)
        pass

    def platform_process(self):
        pass

    def school_process(self):
        pass

    def course_groupify(self):
        pass

    def process_table_old_course(self):
        sql = "select course_id, url from course_list_info where platform = '浙江省高等学校在线开放课程共享平台' and " \
              "course_group_id = '' and valid = 1"
        res = self.processor.course_list_repo.fetch_all(sql)
        urls = []
        for course in res:
            urls.append(course[1])
        # zhejiang = ListFetcher()
        # term_list = zhejiang.run_by_urls(urls)
        with open("zhejiangmooc_xiajia.pkl", "rb") as f:
            term_list = pickle.load(f)
        for course_list_info in term_list:
            if course_list_info != []:
                course_list_info["platform_term_id"] = course_list_info["url"].replace("https://www.zjooc.cn/course/", "")
                hash_str = course_list_info["course_group_id"] + course_list_info["platform"]
                hash_md5 = hashlib.md5(hash_str.encode("utf-8")).hexdigest()
                course_list_info["course_group_id"] = hash_md5
        for item in term_list:
            if item != []:
                for course in res:
                    if course[1] == item["url"]:
                        item["course_id"] = course[0]
                        break
        for item2 in term_list:
            if item2 != []:
                if item2["course_id"] != 0:
                    course_list_info = CourseListInfo()
                    course_list_info.course_id = item2["course_id"]
                    course_list_info.platform = item2["platform"]
                    course_list_info.course_group_id = item2["course_group_id"]
                    course_list_info.platform_term_id = item2["platform_term_id"]
                    print(course_list_info.course_id)
                    self.processor.course_list_repo.update_ids_by_course_id(course_list_info, course_list_info.course_id)
        pass

    def run(self):
        self.course_groupify()
        self.school_process()
        self.platform_process()


if __name__ == '__main__':
    config = ProcessConfig()
    processor = Processor(config)
    platform_processor = PlatformProcessor(processor)
    platform_processor.process_table_old_course()
