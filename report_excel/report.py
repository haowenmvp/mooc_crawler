import logging

import mysql.connector
from persistence.db.course_list_info_repo import CourseListInfoRepository
import datetime
from persistence.excel import CreatExcel
from persistence.model.report_config import ReportConfig
from report_excel.send_email import Email


class Report(object):
    def __init__(self, config: ReportConfig):
        self.course_list_info_repo = CourseListInfoRepository(**config.db_config)
        self.__host = config.db_config["host"]
        self.__port = config.db_config["port"]
        self.__user = config.db_config["username"]
        self.__password = config.db_config["password"]
        self.__database = config.db_config["database"]
        self.__conn = mysql.connector.connect(host=self.__host, user=self.__user, password=self.__password,
                                              database=self.__database, port=self.__port)
        self.report_weekly_path = config.report_weekly_path
        self.report_daily_path = config.report_daily_path
        self.to_excel = CreatExcel()
        self.email = Email(config.email_config)
        pass

    def get_new_courses_weekly(self):
        end_date = datetime.datetime.now()
        begin_date = end_date + datetime.timedelta(days=-7)
        course_info_list = self.course_list_info_repo.get_courses_info_by_platform_date(platform="all", begin_date=begin_date, end_date=end_date, field_type="save_time")
        dict_list = [course.__dict__ for course in course_info_list]
        excel_path = self.report_weekly_path
        full_path = self.to_excel.write(excel_path, "每周新增课程列表", dict_list)
        length = len(full_path.split('/'))
        excel_name = full_path.split('/')[length - 1]
        self.email.send_with_excel(subject="每周新增课程列表", main_text="附件为本周新增课程列表。若无法打开，请右键点击属性，点击解除锁定", excel_path=full_path, excel_name=excel_name)
        pass

    def get_run_log_daily(self):
        cursor = self.__conn.cursor()
        sql = """select course_name,crawl_num,course_num,crawled_next_time,start_time,crawl_finish_time,start_handle_time,finish_time ,TIMESTAMPDIFF(SECOND,start_time,crawl_finish_time)/course_num as crawl_per_course_time,TIMESTAMPDIFF(SECOND,create_time,start_time) as wait_start,TIMESTAMPDIFF(SECOND,start_time,crawl_finish_time) as crawl_time,TIMESTAMPDIFF(SECOND,crawl_finish_time,start_handle_time) as wait_time,TIMESTAMPDIFF(SECOND,start_handle_time,finish_time) as handle_time,crawled_failed_num,status
from (select a.course_name,b.status,b.create_time,b.start_time,b.crawl_finish_time,b.finish_time,b.crawled_failed_num,b.start_handle_time,b.crawled_next_time,b.crawl_num
from (SELECT * from schedule_task where crawled_next_time >= curdate()) b 
JOIN (select course_name,id from task_info) a on a.id = b.task_id) c 
left join (SELECT DISTINCT(platform),count(*)as course_num from course_crowd_weekly where update_time >= curdate() GROUP BY platform) d on c.course_name = d.platform 
ORDER BY course_num desc"""
        cursor.execute(sql)
        results = cursor.fetchall()
        self.__conn.commit()
        results_list = list()
        for item in results:
            info_dict = {}
            info_dict["course_name"] = item[0]
            info_dict["crawl_num"] = item[1]
            info_dict["course_num"] = item[2]
            info_dict["crawled_next_time"] = item[3]
            info_dict["start_time"] = item[4]
            info_dict["crawl_finish_time"] = item[5]
            info_dict["start_handle_time"] = item[6]
            info_dict["finish_time"] = item[7]
            info_dict["crawl_per_course_time"] = item[8]
            info_dict["wait_start"] = item[9]
            info_dict["crawl_time"] = item[10]
            info_dict["wait_time"] = item[11]
            info_dict["handle_time"] = item[12]
            info_dict["crawled_failed_num"] = item[13]
            info_dict["status"] = item[14]
            results_list.append(info_dict)
        excel_path = self.report_daily_path
        full_path = self.to_excel.write(excel_path, "每日爬虫日志", results_list)
        length = len(full_path.split('/'))
        excel_name = full_path.split('/')[length - 1]
        self.email.send_with_excel(subject="每日爬虫日志", main_text="附件为本日爬虫运行日志。若无法打开，请右键点击属性，点击解除锁定", excel_path=full_path, excel_name=excel_name)
        pass

    def run(self, type_report):
        if type_report == "new_course":
            self.get_new_courses_weekly()
        elif type_report == "run_log":
            self.get_run_log_daily()
        else:
            logging.info("[report]：Invalid report type")
        pass

# if __name__ == '__main__':
#     logging.basicConfig(format="[%(threadName)s: %(thread)d] [%(levelname)s]:"
#                                " %(message)s", level=logging.INFO, handlers=[logging.StreamHandler(), ])
#
#     config_path = 'config/manager.json'
#     config = JsonManagerConfig().read(config_path)
#     test = Report(config.db_config)
#     test.run("run_log")