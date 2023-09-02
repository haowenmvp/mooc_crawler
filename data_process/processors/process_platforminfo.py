import datetime
from persistence.model.basic_info import PlatformResource
from persistence.model.process_config import ProcessConfig
import logging
# from data_process.processor import Processor

yesterday = datetime.date.today() - datetime.timedelta(days=1)


class PlatforminfoProcessor:
    def __init__(self, processor):
        self.processor = processor

    platform_list = ['爱课程(中国大学MOOC)', '安徽省网络课程学习中心平台', '微知库数字校园学习平台', '中科云教育', '北京高校优质课程研究会', '优学院（人民网公开课）',
                     '北京高校邦科技有限公司', '国家开放大学出版社有限公司（荟学习网）', '浙江省高等学校在线开放课程共享平台', '玩课网',
                     '优慕课', '人卫社MOOC', '好大学在线', '中国高校外语慕课平台', '智慧职教',
                     '学堂在线', '北京学银在线教育科技有限公司', '北京超星尔雅教育科技有限公司', '重庆高校在线开放课程平台（重庆市教育委员会）', '华文慕课', '优课联盟', '智慧树']

    def plfinfo_processor_type1(self, platform):
        sqls = (
            'SELECT count(*) from course_group  where status=1 and platform="' + platform + '\"',
            'SELECT count(*) from course_group where platform="' + platform + '\"',
            'SELECT SUM(a.crowd) FROM (SELECT * FROM course_list_info WHERE valid=1) a INNER JOIN (SELECT course_group_id, term_count FROM course_group WHERE platform ="' + platform + '\"and status="1") b ON a.course_group_id = b.course_group_id and a.term_id=b.term_count',
            'SELECT SUM(a.crowd) FROM (SELECT * FROM course_list_info WHERE valid=1) a INNER JOIN (SELECT course_group_id, term_count FROM course_group WHERE platform ="' + platform + '\") b ON a.course_group_id = b.course_group_id')

        res = []
        for sql in sqls:
            res.append(self.processor.course_list_repo.fetch_all(sql)[0][0])
        print(platform, res)
        platform_resource = PlatformResource()
        platform_resource.platform = platform
        platform_resource.open_course_num = res[0]
        platform_resource.accumulate_course_num = res[1]
        platform_resource.open_course_crowd = res[2] if res[2] else 0
        platform_resource.accumulate_crowd = res[3] if res[3] else 0
        platform_resource.update_time = datetime.datetime.now()
        self.processor.platform_info_repo.create_platform_resource(platform_resource)

    def plfinfo_processor_type2(self, platform):
        sqls = (
            'SELECT count(*) from course_group  where status=1 and platform="' + platform + '\"',
            'SELECT count(*) from course_group where platform="' + platform + '\"',
            'SELECT SUM(a.crowd) FROM (SELECT * FROM course_list_info WHERE update_time > "' + str(
                yesterday) + '" and valid=1) a INNER JOIN (SELECT course_group_id, term_count FROM course_group WHERE platform ="' + platform + '\"and status="1") b ON a.course_group_id = b.course_group_id',
            'SELECT SUM(a.crowd) FROM (SELECT * FROM course_list_info WHERE valid=1) a INNER JOIN (SELECT course_group_id, term_count FROM course_group WHERE platform ="' + platform + '\") b ON a.course_group_id = b.course_group_id')

        res = []
        for sql in sqls:
            res.append(self.processor.course_list_repo.fetch_all(sql)[0][0])
        print(platform, res)
        platform_resource = PlatformResource()
        platform_resource.platform = platform
        platform_resource.open_course_num = res[0]
        platform_resource.accumulate_course_num = res[1]
        platform_resource.open_course_crowd = res[2] if res[2] else 0
        platform_resource.accumulate_crowd = res[3] if res[3] else 0
        platform_resource.update_time = datetime.datetime.now()
        self.processor.platform_info_repo.create_platform_resource(platform_resource)

    def plfinfo_processor_type3(self, platform):
        sqls = [
            'SELECT count(*) from course_group  where status=1 and platform="' + platform + '\"',
            'SELECT count(*) from course_group where platform="' + platform + '\"',
            'SELECT SUM(a.total_num) FROM (SELECT course_group_id,max(total_crowd_num) AS total_num from course_list_info WHERE valid=1 GROUP BY course_group_id) a INNER JOIN (SELECT course_group_id, term_count FROM course_group WHERE platform ="{}") b ON a.course_group_id = b.course_group_id']

        res = []
        for sql in sqls:
            res.append(self.processor.course_list_repo.fetch_all(sql.format(platform))[0][0])
            # res.append(self.processor.fetch_all(sql.format(platform))[0][0])
        print(platform, res)
        # print(res)
        platform_resource = PlatformResource()
        platform_resource.platform = platform
        platform_resource.open_course_num = res[0]
        platform_resource.accumulate_course_num = res[1]
        platform_resource.accumulate_crowd = res[2] if res[2] else 0
        platform_resource.update_time = datetime.datetime.now()
        self.processor.platform_info_repo.create_platform_resource(platform_resource)

    def plfinfo_processor_type4(self, platform):
        sqls = [
            'SELECT count(*) from course_group  where status=1 and platform="' + platform + '\"',
            'SELECT count(*) from course_group where platform="' + platform + '\"',
            'SELECT SUM(a.total_num) FROM (SELECT course_group_id,max(crowd_num) AS total_num from course_list_info WHERE valid=1 GROUP BY course_group_id) a INNER JOIN (SELECT course_group_id, term_count FROM course_group WHERE platform ="{}") b ON a.course_group_id = b.course_group_id']

        res = []
        for sql in sqls:
            res.append(self.processor.course_list_repo.fetch_all(sql.format(platform))[0][0])
        print(platform, res)
        platform_resource = PlatformResource()
        platform_resource.platform = platform
        platform_resource.open_course_num = res[0]
        platform_resource.accumulate_course_num = res[1]
        platform_resource.accumulate_crowd = res[2] if res[2] else 0
        platform_resource.update_time = datetime.datetime.now()
        self.processor.platform_info_repo.create_platform_resource(platform_resource)

    def run(self):
        type1 = ['爱课程(中国大学MOOC)', '安徽省网络课程学习中心平台',
                 '浙江省高等学校在线开放课程共享平台',
                 '玩课网', '优慕课', '人卫社MOOC', '好大学在线', '中国高校外语慕课平台', '智慧职教',
                 '重庆高校在线开放课程平台（重庆市教育委员会）', '国家开放大学出版社有限公司（荟学习网）']
        type2 = ['微知库数字校园学习平台', '中科云教育', '北京高校优质课程研究会', '优学院（人民网公开课）', '北京超星尔雅教育科技有限公司', '智慧树']
        type3 = [
            '学堂在线', '北京学银在线教育科技有限公司', '华文慕课', '优课联盟']
        type4 = ['北京高校邦科技有限公司']
        for platform in type1:
            try:
                self.plfinfo_processor_type1(platform)
            except Exception as e:
                logging.error("[PlatforminfoProcessor.plfinfo_processor_type1]:[%s]", e)
                continue
        for platform in type2:
            try:
                self.plfinfo_processor_type2(platform)
            except Exception as e:
                logging.error("[PlatforminfoProcessor.plfinfo_processor_type2]:[%s]", e)
                continue
        for platform in type3:
            try:
                self.plfinfo_processor_type3(platform)
            except Exception as e:
                logging.error("[PlatforminfoProcessor.plfinfo_processor_type3]:[%s]", e)
                continue
        for platform in type4:
            try:
                self.plfinfo_processor_type4(platform)
            except Exception as e:
                logging.error("[PlatforminfoProcessor.plfinfo_processor_type4]:[%s]", e)
                continue


# if __name__ == '__main__':
#     config = ProcessConfig()
#     processor = Processor(config)
#     platforminfo_processor = PlatforminfoProcessor(processor)
#     platforminfo_processor.run()