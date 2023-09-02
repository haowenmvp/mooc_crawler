from datetime import datetime
from persistence.model.basic_info import SchoolResource
from persistence.model.process_config import ProcessConfig

PLATFORM_CURCROWD = "('爱课程(中国大学MOOC)', '安徽省网络课程学习中心平台', '微知库数字校园学习平台', '中科云教育', '北京高校优质课程研究会',\
                     '优学院（人民网公开课）', '重庆高校在线开放课程平台（重庆市教育委员会）', '国家开放大学出版社有限公司（荟学习网）', '浙江省高等学校在线开放课程共享平台',\
                     '玩课网', '优慕课', '人卫社MOOC', '好大学在线', '中国高校外语慕课平台', '智慧职教', '智慧树')"


class SchoolinfoProcessor:
    def __init__(self, processor):
        self.processor = processor

    def sclinfo_process(self):
        #  统计学期人数平台的数据
        sql = 'SELECT a.school ,open_course,all_course,open_num,all_num from ' \
              '((SELECT count(*) as open_course,school,sum(crowd_num) as open_num from course_list_info  where status!=0 and valid=1 and school!="" and school!="None" and DATE_SUB(CURDATE(), INTERVAL 4 DAY) <= date(update_time) and platform in ' + PLATFORM_CURCROWD + 'group by school)a join ' \
              '(SELECT count(*) as all_course,school, sum(crowd_num) as all_num from course_list_info  where school!="" and valid=1 and school!="None" and DATE_SUB(CURDATE(), INTERVAL 4 DAY) <= date(update_time) and platform in ' + PLATFORM_CURCROWD + 'group by school)b' \
              ' on a.school=b.school) order by open_course desc'
        # sql = 'SELECT a.school ,open_course,all_course,open_num,all_num from ' \
        #       '((SELECT count(*) as open_course,school, platform from course_list_info  where status!=0 and valid=1 and school!="" and school!="None" and date(update_time) = curdate() and platform in ' + PLATFORM_CURCROWD + 'group by school)a join ' \
        #       '(SELECT count(*) as all_course,school from course_list_info  where school!="" and valid=1 and school!="None" and date(update_time) = curdate() and platform in ' + PLATFORM_CURCROWD + 'group by school)b join' \
        #       '(SELECT sum(crowd_num) as open_num,school from course_list_info  where status!=0 and valid=1 and school!="" and school!="None" and date(update_time) = curdate() and platform in ' + PLATFORM_CURCROWD + 'group by school)c join' \
        #       '(SELECT sum(crowd_num) as all_num,school from course_list_info  where school!="" and valid=1 and school!="None" and date(update_time) = curdate() and platform in ' + PLATFORM_CURCROWD + ' group by school)d ' \
        #       'on a.school=b.school and b.school = c.school and c.school=d.school) order by open_course desc'
        res1 = self.processor.course_list_repo.fetch_all(sql)
        school_resources = []
        school_set = {}
        for line in res1:
            school_resource = SchoolResource()
            school_resource.school_name = line[0]
            school_resource.open_course_num = line[1]
            school_resource.accumulate_course_num = line[2]
            # school_resource.open_course_crowd = int(line[3]) if line[3] else 0
            school_resource.accumulate_crowd = int(line[4]) if line[4] else 0
            school_resource.update_time = datetime.now()
            school_set[line[0]] = school_resource
        #     print(line)
        # return
        # for item in school_set.keys():
        #     s = school_set.get(item)
        #     print(s.school_name, s.open_course_num, s.accumulate_course_num, s.open_course_crowd, s.accumulate_crowd)
        sql = 'SELECT a.school ,open_course,all_course,open_num, all_num from' \
              '((SELECT count(*) as open_course,school from course_list_info  where status!=0 and school!="" and valid=1 and school!="None" and DATE_SUB(CURDATE(), INTERVAL 4 DAY) <= date(update_time) and platform not in ' + PLATFORM_CURCROWD + ' group by school)a join ' \
              '(SELECT count(*) as all_course,school from course_list_info  where school!=""and school!="None" and valid=1 and DATE_SUB(CURDATE(), INTERVAL 4 DAY) <= date(update_time) and platform not in ' + PLATFORM_CURCROWD + ' group by school)b join' \
              '(SELECT sum(open_crowd) as open_num, school from (SELECT max(total_crowd_num) as open_crowd, school from course_list_info  where status!=0 and valid=1 and school!="" and school!="None" and DATE_SUB(CURDATE(), INTERVAL 4 DAY) <= date(update_time) and platform not in ' + PLATFORM_CURCROWD + ' group by course_group_id)x1 group by school) c join' \
              '(SELECT sum(open_crowd) as all_num, school from (SELECT max(total_crowd_num) as open_crowd, school from course_list_info  where school!="" and school!="None" and valid=1 and DATE_SUB(CURDATE(), INTERVAL 4 DAY) <= date(update_time) and platform not in ' + PLATFORM_CURCROWD + ' group by course_group_id)x2 group by school)d ' \
              'on a.school=b.school and b.school = c.school and c.school=d.school)'
        # for school in school_set.keys():
        #     print(school_set[school].__dict__)
        res2 = self.processor.course_list_repo.fetch_all(sql)
        for line in res2:
            if line[0] in school_set.keys():
                school_resource = school_set.get(line[0])
                school_resource.open_course_num += line[1]
                school_resource.accumulate_course_num += line[2]
                # school_resource.open_course_crowd = school_resource.open_course_crowd+int(line[3]) if line[3] else school_resource.open_course_crowd
                school_resource.accumulate_crowd = school_resource.accumulate_crowd + (int(line[4])) if line[
                    4] else school_resource.accumulate_crowd
            else:
                school_resource = SchoolResource()
                school_resource.school_name = line[0]
                school_resource.open_course_num = line[1]
                school_resource.accumulate_course_num = line[2]
                # school_resource.open_course_crowd = int(line[3]) if line[3] else 0
                school_resource.accumulate_crowd = int(line[4]) if line[4] else 0
                school_resource.update_time = datetime.now()
                school_set[line[0]] = school_resource
        # for item in school_set.keys():
        #     s = school_set.get(item)
        #     print(s.school_name, s.open_course_num, s.accumulate_course_num, s.open_course_crowd, s.accumulate_crowd)
        for item in school_set.keys():
            school_resources.append(school_set.get(item))
        self.processor.school_info_repo.create_school_resources(school_resources)

    def run(self):
        self.sclinfo_process_type2()

    def sclinfo_process_type2(self):
        #  上一个函数查询的结果有问题，得到的course_num其实是学期数量，并不是课程数量
        #  不直接利用数据库作复杂的查询，在课程数据完整获取后再执行处理
        sql = 'SELECT status, school ,platform, crowd_num, total_crowd_num, course_group_id from course_list_info ' \
                'where valid=1 and school!="" and school!="None" and DATE_SUB(CURDATE(), INTERVAL 4 DAY) <= '\
                'date(update_time)'
        res = self.processor.course_list_repo.fetch_all(sql)
        school_resources = []
        school_set = {}
        course_group_id_done = set()  # 在处理累计选课人数的平台，要防止课程选课人数同一门课程重复计算, 也防止课程数重复计算
        course_group_open = set()  # 防止开课数重复计算
        for line in res:
            status = line[0]
            school = line[1]
            platform = line[2]
            crowd_num = line[3]
            total_crowd_num = line[4]
            course_group_id = line[5]
            if school in school_set.keys():
                school_resource = school_set.get(school)
                school_resource.open_course_num = school_resource.open_course_num + 1 if status == 1 and course_group_id not in course_group_open else school_resource.open_course_num
                school_resource.accumulate_course_num = school_resource.accumulate_course_num + 1 if course_group_id not in course_group_id_done else school_resource.accumulate_course_num
                if platform in PLATFORM_CURCROWD:
                    school_resource.accumulate_crowd = school_resource.accumulate_crowd + int(crowd_num) if crowd_num else school_resource.accumulate_crowd
                    school_resource.open_course_crowd = school_resource.open_course_crowd + int(crowd_num) if crowd_num and status == 1 else school_resource.open_course_crowd
                else:
                    school_resource.accumulate_crowd = school_resource.accumulate_crowd + int(total_crowd_num) if course_group_id not in course_group_id_done and total_crowd_num else school_resource.accumulate_crowd
                    school_resource.open_course_crowd = school_resource.open_course_crowd + int(total_crowd_num) if status == 1 and course_group_id not in course_group_open and total_crowd_num else school_resource.open_course_crowd
            else:
                school_resource = SchoolResource()
                school_resource.school_name = school
                school_resource.open_course_num = 1 if status == 1 and course_group_id not in course_group_open else 0
                school_resource.accumulate_course_num = 1
                if platform in PLATFORM_CURCROWD:
                    school_resource.accumulate_crowd = int(crowd_num) if crowd_num else 0
                    school_resource.open_course_crowd = int(crowd_num) if crowd_num and status == 1 else 0
                else:
                    school_resource.accumulate_crowd = int(total_crowd_num) if total_crowd_num else 0
                    school_resource.open_course_crowd = int(total_crowd_num) if total_crowd_num and status == 1 else 0
                school_resource.update_time = datetime.now()
                school_set[school] = school_resource
            course_group_id_done.add(course_group_id)
            if status == 1:
                course_group_open.add(course_group_id)
        for item in school_set.keys():
            school_resources.append(school_set.get(item))
            resource = school_set.get(item)
            # print(resource.school_name, resource.accumulate_course_num, resource.open_course_num, resource.accumulate_crowd, resource.open_course_crowd)
        self.processor.school_info_repo.create_school_resources(school_resources)


if __name__ == '__main__':
    from data_process.processor import Processor
    config = ProcessConfig()
    prc = Processor(config)
    scl = SchoolinfoProcessor(prc)
    scl.sclinfo_process()