import datetime
from persistence.model.basic_info import CourseGroupInfo
from persistence.model.basic_info import CourseListInfo
from persistence.model.process_config import ProcessConfig
import logging


class CourseGroupProcessor:
    def __init__(self, processor):
        self.processor = processor
        self.platform_list = ['爱课程(中国大学MOOC)', '安徽省网络课程学习中心平台', '微知库数字校园学习平台', '中科云教育', '北京高校优质课程研究会', '优学院（人民网公开课）',
                             '北京高校邦科技有限公司', '国家开放大学出版社有限公司（荟学习网）', '浙江省高等学校在线开放课程共享平台', '玩课网',
                             '优慕课', '人卫社MOOC', '好大学在线', '中国高校外语慕课平台', '智慧职教',
                             '学堂在线', '北京学银在线教育科技有限公司', '北京超星尔雅教育科技有限公司', '重庆高校在线开放课程平台（重庆市教育委员会）', '华文慕课', '优课联盟', '智慧树']
        # self.platform_list = [
        #                       '学堂在线', '北京学银在线教育科技有限公司', '北京超星尔雅教育科技有限公司', '重庆高校在线开放课程平台（重庆市教育委员会）', '华文慕课', '优课联盟']

    '''
    1. 处理时筛选该平台有效的课程 ：valid=1
    2. 在今天新增的学期中进行判断是否为新增课程组，并对 coursegroup 表进行更新：以新增学期的 term_id 来更新 coursegroup 中的term_count,同时 status 置为 1
    3. 对该平台的 coursegroup 记录中原本正在开课状态的课程进行遍历，判断之前在开课的课程是否结束：通过 term_count 和 coursegroupid 来查找最新学期的状态
        1）学期即为课程组的平台，term_id 为 1
        2) 平台分多学期的课程，term_id 从 1 递增
            a) 没有自主模式的平台：通过判断 max(term_id) 学期的开课状态 
            b) 存在自主模式的平台：包含自主模式的课程自主模式学期 term_id 设为 0，term_count 为所有学期数count(*)，
               判断课程开课状态：判断 max(term_id)学期的开课状态，若正常学期无开课则判断自主模式是否开课  
    4. 判断学期开课状态：
        0 ：结束
        1/2 ：开课
        update_time < yesterday ：结课
    '''

    def process_course_group(self):
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        last_update = datetime.date.today() - datetime.timedelta(days=2)
        last_update_zj = datetime.date.today() - datetime.timedelta(days=5)
        print(str(yesterday))
        sql_find_new_term = 'select course_group_id,course_name,school,teacher,platform,term_id from course_list_info where save_time > "' + str(
            yesterday) + '%"and valid = 1'
        res = self.processor.course_list_repo.fetch_all(sql_find_new_term)
        course_groups = []
        for line in res:
            course_group_id = line[0]
            if course_group_id != '':
                sql_find_term_count = 'select count(*) from course_list_info where course_group_id="' + course_group_id + '" and valid=1'
                term_count = self.processor.course_list_repo.fetch_all(sql_find_term_count)[0][0]
                group = self.processor.course_group_repo.get_group_by_groupid(course_group_id)
                if group:
                    self.processor.course_group_repo.update_new_term(course_group_id, term_count)
                    self.processor.course_group_repo.update_course_status(course_group_id, 1)
                else:
                    course_group = CourseGroupInfo()
                    course_group.course_group_id = line[0]
                    course_group.course_name = line[1]
                    course_group.school = line[2]
                    course_group.teacher = line[3]
                    course_group.platform = line[4]
                    course_group.status = 1
                    course_group.term_count = 1
                    course_group.update_time = datetime.datetime.now()
                    course_groups.append(course_group)
        self.processor.course_group_repo.create_course_group(course_group)
        sql_find_group_ing = 'select course_group_id,term_count,platform from course_group where status=1'
        res = self.processor.course_list_repo.fetch_all(sql_find_group_ing)
        for course_group_id, term_count, platform in res:
            sql_find_min_term_id = 'SELECT min(term_id) from course_list_info where course_group_id="' + course_group_id + '"'
            if term_count == 1:
                status, update_time = self.processor.course_list_repo.get_status_by_course_group_id(course_group_id)
            else:
                sql_find_max_term_id = 'SELECT max(term_id) from course_list_info where course_group_id="' + course_group_id + '"'
                term_id = self.processor.course_list_repo.fetch_all(sql_find_max_term_id)[0][0]
                status, update_time = self.processor.course_list_repo.get_maxterm_status(course_group_id, term_id)
            if status == 0 or (platform == "浙江省高等学校在线开放课程共享平台" and update_time.date() < last_update_zj) or (
                    platform != "浙江省高等学校在线开放课程共享平台" and update_time.date() < last_update):
                self.processor.course_group_repo.update_course_status(course_group_id, 0)
            elif self.processor.course_list_repo.fetch_all(sql_find_min_term_id)[0][0] == 0:
                status, update_time = self.processor.course_list_repo.get_maxterm_status(course_group_id, 0)
                if status == 0 or update_time.date() < last_update:
                    self.processor.course_group_repo.update_course_status(course_group_id, 0)

    def process_course_group_platform(self, platform):
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        sql_find_zj = 'select max(update_time) from course_list_info where platform= "' + platform + '"'
        res = self.processor.course_list_repo.fetch_all(sql_find_zj)[0][0]
        last_update = res.date()
        sql_find_new_term = 'select course_group_id,course_name,platform,school,teacher,term_id from course_list_info where save_time > "' + str(
            yesterday) + '%" and platform="' + platform + '"and valid = 1'
        res = self.processor.course_list_repo.fetch_all(sql_find_new_term)
        print(sql_find_new_term)
        for line in res:
            course_group_id = line[0]
            if course_group_id != '':
                sql_find_term_count = 'select count(*) from course_list_info where course_group_id="' + course_group_id + '" and valid=1'
                term_count = self.processor.course_list_repo.fetch_all(sql_find_term_count)[0][0]
                group = self.processor.course_group_repo.get_group_by_groupid(course_group_id)
                if group:
                    self.processor.course_group_repo.update_new_term(course_group_id, term_count)
                    self.processor.course_group_repo.update_course_status(course_group_id, 1)
                    # print("new_term")
                else:
                    course_group = CourseGroupInfo()
                    course_group.course_group_id = line[0]
                    course_group.course_name = line[1]
                    course_group.platform = line[2]
                    course_group.school = line[3]
                    course_group.teacher = line[4]
                    course_group.status = 1
                    course_group.term_count = 1
                    self.processor.course_group_repo.create_course_group(course_group)
                    # print("new_course")
        sql_find_group_ing = 'select course_group_id,term_count from course_group where platform= "' + platform + '"and status=1'
        res = self.processor.course_list_repo.fetch_all(sql_find_group_ing)
        print(sql_find_group_ing)
        print(len(res))
        for course_group_id, term_count in res:
            try:
                sql_find_min_term_id = 'SELECT min(term_id) from course_list_info where course_group_id="' + course_group_id + '"'
                if term_count == 1:
                    status, update_time = self.processor.course_list_repo.get_status_by_course_group_id(
                        course_group_id)
                else:
                    sql_find_max_term_id = 'SELECT max(term_id) from course_list_info where course_group_id="' + course_group_id + '"'
                    term_id = self.processor.course_list_repo.fetch_all(sql_find_max_term_id)[0][0]
                    status, update_time = self.processor.course_list_repo.get_maxterm_status(course_group_id,
                                                                                             term_id)
                if status == 0 or update_time.date() < last_update:
                    print(course_group_id)
                    self.processor.course_group_repo.update_course_status(course_group_id, 0)
                elif self.processor.course_list_repo.fetch_all(sql_find_min_term_id)[0][0] == 0:
                    status, update_time = self.processor.course_list_repo.get_maxterm_status(course_group_id, 0)
                    if status != 0 or update_time.date() > last_update:
                        print(course_group_id)
                        self.processor.course_group_repo.update_course_status(course_group_id, 1)
                else:
                    pass
            except:
                logging.error(
                    "[CourseGroupProcessor.process_course_group_platform] Can't find course_group_id in course_list_info:%s %s",
                    platform, course_group_id)
                with open('./process_course_group_log', 'a+') as f:
                    self.processor.course_group_repo.delete_info_by_groupid(course_group_id)
                    f.write("%s %s %s\n" % (datetime.date.today(), platform, course_group_id))

    def process_course_group_update(self):
        #  修改更新策略，删除原有的数据，直接根据course_list_info重新导入course_group表
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        three_days_ago = datetime.date.today() - datetime.timedelta(days=3)
        # delete_no_exit_sql = 'delete from course_group where course_group_id not in (select course_group_id from course_list_info)'
        # self.processor.course_list_repo.fetch_all(delete_no_exit_sql)
        obtain_not_in_group_sql = 'select course_group_id, course_name, platform, school, teacher, status, ' \
                                  'update_time from course_list_info where valid = 1'
        # res = self.processor.course_list_repo.fetch_all(obtain_not_in_group_sql)
        self.processor.course_group_repo.clear_table()
        res = self.processor.course_list_repo.get_all_list()
        print(len(res))
        group_id_info = dict()
        for course_list_info in res:
            if course_list_info.valid != 1:
                continue
            course_group_id = course_list_info.course_group_id
            course_name = course_list_info.course_name
            platform = course_list_info.platform
            school = course_list_info.school
            teacher = course_list_info.teacher
            status = course_list_info.status
            update_time = course_list_info.update_time
            if course_group_id not in group_id_info.keys():
                course_group = CourseGroupInfo()
                course_group.course_group_id = course_group_id
                course_group.platform = platform
                course_group.course_name = course_name
                course_group.school = school
                course_group.teacher = teacher
                course_group.update_time = datetime.datetime.now()
                course_group.term_count = 1
                course_group.status = CourseGroupInfo.CourseGroupStatusEnum.kStatusOn if status == 1 and (yesterday.__le__(update_time) or (platform == '浙江省高等学校在线开放课程共享平台' and three_days_ago.__le__(update_time)))else CourseGroupInfo.CourseGroupStatusEnum.kStatusEnd
                group_id_info[course_group_id] = course_group
            else:
                course_group = group_id_info[course_group_id]
                course_group.term_count = course_group.term_count + 1
                course_group.update_time = datetime.datetime.now()
                if course_group.status == CourseGroupInfo.CourseGroupStatusEnum.kStatusEnd and status == 1 and (yesterday.__le__(update_time) or (platform == '浙江省高等学校在线开放课程共享平台' and three_days_ago.__le__(update_time))):
                    course_group.status = CourseGroupInfo.CourseGroupStatusEnum.kStatusOn
        insert_list = list(group_id_info.values())
        one_insert_num = 1000
        total_insert_times = int(len(insert_list)/one_insert_num) if len(insert_list) % one_insert_num == 0 else int(len(insert_list)/one_insert_num) + 1
        for i in range(total_insert_times):
            start = one_insert_num*i
            end = one_insert_num*(i+1)
            if i < total_insert_times - 1:
                self.processor.course_group_repo.create_course_groups(insert_list[start: end])
            else:
                self.processor.course_group_repo.create_course_groups(insert_list[start:])

    def course_group_update_from_yesterday(self):
        #  修改更新策略， 不再直接删除原表，而是从昨天的数据比较进行更新或者插入
        #  第一步，删除course_group中在course_list_info不存在的记录
        delete_no_exit_sql = 'delete from course_group where course_group_id not in (select course_group_id ' \
                             ' from course_list_info)'
        self.processor.course_list_repo.batch_all(delete_no_exit_sql)
        #  获取course_list_info中数据来合并整理成course_group信息
        res = self.processor.course_list_repo.get_all_list()
        print(len(res))
        group_id_info = dict()
        for course_list_info in res:
            if course_list_info.valid != 1:
                continue
            course_group_id = course_list_info.course_group_id
            course_name = course_list_info.course_name
            platform = course_list_info.platform
            school = course_list_info.school
            teacher = course_list_info.teacher
            status = course_list_info.status
            update_time = course_list_info.update_time
            if course_group_id not in group_id_info.keys():
                course_group = CourseGroupInfo()
                course_group.course_group_id = course_group_id
                course_group.platform = platform
                course_group.course_name = course_name
                course_group.school = school
                course_group.teacher = teacher
                course_group.update_time = datetime.datetime.now()
                course_group.term_count = 1
                course_group.status = CourseGroupInfo.CourseGroupStatusEnum.kStatusOn if self.judge_course_status(
                    platform, status, update_time) else CourseGroupInfo.CourseGroupStatusEnum.kStatusEnd
                group_id_info[course_group_id] = course_group
            else:
                course_group = group_id_info[course_group_id]
                course_group.term_count = course_group.term_count + 1
                course_group.update_time = datetime.datetime.now()
                if course_group.status == CourseGroupInfo.CourseGroupStatusEnum.kStatusEnd and self\
                        .judge_course_status(platform, status, update_time):
                    course_group.status = CourseGroupInfo.CourseGroupStatusEnum.kStatusOn
        #  获取course_list_info需要插入的course_group_id集合（course_group表中不存在）
        obtain_sql = 'select course_group_id from course_list_info where course_group_id not in ' \
                     '(select course_group_id from course_group)'
        res = self.processor.course_list_repo.fetch_all(obtain_sql)
        print(res)
        insert_course_group_id = set()
        for item in res:
            insert_course_group_id.add(item[0])
        # print(insert_course_group_id)
        insert_list = []
        update_list = []
        for key, value in group_id_info.items():
            if key in insert_course_group_id:
                insert_list.append(value)
            else:
                update_list.append(value)
        #  插入新纪录
        one_insert_num = 1000
        total_insert_times = int(len(insert_list) / one_insert_num) if len(insert_list) % one_insert_num == 0 else int(
            len(insert_list) / one_insert_num) + 1
        for i in range(total_insert_times):
            start = one_insert_num * i
            end = one_insert_num * (i + 1)
            if i < total_insert_times - 1:
                self.processor.course_group_repo.create_course_groups(insert_list[start: end])
            else:
                self.processor.course_group_repo.create_course_groups(insert_list[start:])
        #  更新已存在记录
        for item in update_list:
            self.processor.course_group_repo.update_course_group_by_id(item)

    @staticmethod
    def judge_course_status(platform, status, update_time):
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        three_days_ago = datetime.date.today() - datetime.timedelta(days=3)
        if platform == '浙江省高等学校在线开放课程共享平台' and three_days_ago.__le__(update_time) and status == 1:
            return True
        if platform == '微知库数字校园学习平台' and yesterday.__le__(update_time) and (status == 1 or status == 3):
            return True
        if yesterday.__le__(update_time) and status == 1:
            return True
        return False

    def run(self):
        # for platform_name in self.platform_list:
        #     self.process_course_group_platform(platform_name)
        #     print(platform_name + " is done.")
        self.course_group_update_from_yesterday()
