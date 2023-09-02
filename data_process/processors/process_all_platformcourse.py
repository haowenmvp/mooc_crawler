import utils
from persistence.model.process_config import ProcessConfig
from persistence.db.course_list_info_repo import CourseListInfoRepository
from persistence.db.course_group_repo import CourseGroupRepository
from persistence.db.platform_resource_repo import PlatformResourceRepository
from persistence.db.school_resource_repo import SchoolResourceRepository
from persistence.db.course_exception_repo import CourseExceptionRepository
from persistence.model.basic_info import SchoolResource
from persistence.model.basic_info import CourseGroupInfo
from persistence.model.basic_info import PlatformResource
from persistence.model.course_exception import CourseException
import datetime


class ProcessorAllPlatform:
    def __init__(self, process_config):
        self.platforms_config = process_config.platform_dict
        self.repo_config = process_config.repo
        self.course_list_repo = CourseListInfoRepository(**self.repo_config)
        self.course_group_repo = CourseGroupRepository(**self.repo_config)
        self.platform_resource_repo = PlatformResourceRepository(**self.repo_config)
        self.school_resource_repo = SchoolResourceRepository(**self.repo_config)
        self.course_exception_repo = CourseExceptionRepository(**self.repo_config)
        self.platform_resource = list()
        self.school_resource = list()
        self.course_group = list()
        self.act_start_time = datetime.datetime.now()

    def run(self):
        self.run_exception()  # 寻找数据异常的课程
        for platform, platform_config in self.platforms_config.items():
            platform_processor_path = platform_config['platform_processor']
            if platform_processor_path == "":
                continue
            course_list_info_platform = self.course_list_repo.get_course_list_by_platform(platform)
            # print(course_list_info_platform)
            platform_processor = utils.load_class_type(platform_processor_path)
            processor = platform_processor()
            print(platform)
            print("source data length: ", len(course_list_info_platform))
            try:
                group_list, platform_list, school_list = processor.run(course_list_info_platform)
                print(platform_list.__dict__)
                self.platform_resource.append(platform_list)
                self.course_group = self.course_group + group_list
                self.school_resource = self.school_resource + school_list
            except:
                pass
            # course_num_huazhong = 0
            # course_num_huazhong_group = 0
            # for item in school_list:
            #     if item.school_name == '华中科技大学':
            #         course_num_huazhong = item.accumulate_course_num
            #         print("school resource: 华科课程数 ", course_num_huazhong)
            # for item in group_list:
            #     if item.school == '华中科技大学':
            #         course_num_huazhong_group = course_num_huazhong_group + 1
            # print("course group: 华科课程数 ", course_num_huazhong_group)
        self.process_course_group()
        self.process_school_resource()
        self.process_platform_resource()

    def run_exception(self):
        today = datetime.datetime.today()
        yesterday = today - datetime.timedelta(days=1)
        delete_weekly_data_sql = 'delete from course_crowd_weekly_oneday where 1=1'
        self.course_list_repo.batch_all(delete_weekly_data_sql)
        print("删除临时表数据crowd")
        copy_data_sql = "insert into course_crowd_weekly_oneday select * from course_crowd_weekly where date(update_time) = date(" + today.strftime(
            "%Y%m%d") + ")"
        self.course_list_repo.batch_all(copy_data_sql)
        print("复制临时表数据crowd")
        delete_crowd_data_sql = 'delete from forum_num_oneday where 1=1'
        self.course_list_repo.batch_all(delete_crowd_data_sql)
        print("删除临时表数据forum")
        copy_data_sql = "insert into forum_num_oneday select * from forum_num where date(save_time) >= date(" + yesterday.strftime(
            "%Y%m%d") + ")"
        self.course_list_repo.batch_all(copy_data_sql)
        print("复制临时表数据forum")
        course_list_valid = self.course_list_repo.get_valid_course_list()
        print(len(course_list_valid))
        exception_list = []
        i = 0
        for course in course_list_valid:
            i = i + 1
            print(i)
            # print("course.update_time: " + str(course.update_time))
            if today.date() != course.update_time.date():
                continue
            # print("do exception")
            course_id = course.course_id
            data_yesterday_sql = "select crowd_num, total_crowd_num from course_crowd_weekly_oneday where course_id = " + str(course_id) + " and " \
                "date(update_time) = date('" + yesterday.strftime("%Y-%m-%d") + "') "
            crowd_res = self.course_list_repo.fetch_all(data_yesterday_sql)
            post_num_now_sql = "select forum_num from forum_num_oneday where course_id = " + str(course_id) + " and date(save_time) =" \
                "date('" + today.strftime("%Y-%m-%d") + "')"
            post_num_yesterday_sql = "select forum_num from forum_num_oneday where course_id = "\
                + str(course_id) + " and date(save_time) = date('" + yesterday.strftime("%Y-%m-%d") + "')"
            post_res_now = self.course_list_repo.fetch_all(post_num_now_sql)
            post_res_yesterday = self.course_list_repo.fetch_all(post_num_yesterday_sql)
            # print(post_res_now)
            # print(post_res_yesterday)
            # print(crowd_res)
            post_num_now = None if len(post_res_now) == 0 else post_res_now[0][0]
            post_num_yesterday = None if len(post_res_yesterday) == 0 else post_res_yesterday[0][0]
            crowd_yesterday = None if len(crowd_res) == 0 or crowd_res[0][0] is None else crowd_res[0][0]
            total_crowd_yesterday = None if len(crowd_res) == 0 or crowd_res[0][1] is None else crowd_res[0][1]
            course_exception = CourseException(course, post_num_now, post_num_yesterday, crowd_yesterday, total_crowd_yesterday)
            course_exception.generate_exception_type()
            if course_exception.exception_type != "":
                exception_list.append(course_exception)
                print(course.course_name + " exception type : " + course_exception.exception_type)
        self.course_exception_repo.create_course_exception_list(exception_list)

    def process_school_resource(self):
        school_name_resource = dict()
        for item in self.school_resource:
            assert isinstance(item, SchoolResource)
            if item.school_name not in school_name_resource.keys():
                school_name_resource[item.school_name] = item
            else:
                resourse = school_name_resource[item.school_name]
                resourse.accumulate_course_num = resourse.accumulate_course_num + item.accumulate_course_num
                resourse.accumulate_crowd = resourse.accumulate_crowd + item.accumulate_crowd
                resourse.open_course_num = resourse.open_course_num + item.open_course_num
                resourse.open_course_crowd = resourse.open_course_crowd + item.open_course_crowd
        self.school_resource_repo.create_school_resources(list(school_name_resource.values()))

    def process_course_group(self):
        #  修改更新策略， 不再直接删除原表，而是从昨天的数据比较进行更新或者插入

        #  获取course_list_info需要插入的course_group_id集合（course_group表中不存在）
        obtain_sql = 'select course_group_id from course_list_info where course_group_id not in ' \
                     '(select course_group_id from course_group)'
        res = self.course_list_repo.fetch_all(obtain_sql)
        print(res)
        insert_course_group_id = set()
        for item in res:
            insert_course_group_id.add(item[0])
        insert_list = list()
        update_list = list()
        for group_info in self.course_group:
            assert  isinstance(group_info, CourseGroupInfo)
            if group_info.course_group_id in insert_course_group_id:
                insert_list.append(group_info)
            else:
                update_list.append(group_info)
        #  插入新纪录
        one_insert_num = 1000
        total_insert_times = int(len(insert_list) / one_insert_num) if len(insert_list) % one_insert_num == 0 else int(
            len(insert_list) / one_insert_num) + 1
        for i in range(total_insert_times):
            start = one_insert_num * i
            end = one_insert_num * (i + 1)
            if i < total_insert_times - 1:
                self.course_group_repo.create_course_groups(insert_list[start: end])
            else:
                self.course_group_repo.create_course_groups(insert_list[start:])
        #  更新已存在记录
        for item in update_list:
            self.course_group_repo.update_course_group_by_id(item)
        #  最后一步，删除course_group中不该存在的记录
        delete_no_exit_sql = "delete from course_group where update_time < '" + self.act_start_time.strftime("%Y-%m-%d %H:%M:%S") + "'"
        self.course_list_repo.batch_all(delete_no_exit_sql)

    def process_platform_resource(self):
        for item in self.platform_resource:
            assert isinstance(item, PlatformResource)
        self.platform_resource_repo.create_platform_resources(self.platform_resource)


if __name__ == '__main__':
    ProcessorAllPlatform(ProcessConfig()).run()
