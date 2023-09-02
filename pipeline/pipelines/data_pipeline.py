import logging
import uuid
import json
import hashlib
import re
import pickle
from typing import List

from pipeline.pipelines.base_pipeline import BasePipeline

from persistence.model.pipeline_config import PipelineConfig
from persistence.model.forum import ForumPostInfo, ForumBasicInfo
from persistence.model.task import Task
from persistence.model.course_info import CourseBasicInfo, SemesterBasicInfo, SemesterResultInfo
from persistence.model.basic_info import TeacherInfo
from persistence.model.basic_info import CourseListInfo
from persistence.model.basic_info import CourseCrowdWeekly

from persistence.db.course_list_info_repo import CourseListInfoRepository
from persistence.db.error_repo import ErrorRepository
from persistence.db.course_basic_info_repo import CourseBasicInfoRepository
from persistence.db.semester_basic_info_repo import SemesterBasicInfoRepository
from persistence.db.task_info_repo import TaskInfoRepository
from persistence.db.forum_post_info_repo import ForumPostInfoRepository
from persistence.db.forum_basic_info_repo import ForumBasicInfoRepository

from persistence.db.semester_result_info_repo import SemesterResultInfoRepository
from persistence.db.teacher_info_repo import TeacherInfoRepository
from persistence.db.course_crowd_weekly_repo import CourseCrowdWeeklyRepo
from persistence.db.forum_num_repo import ForumNumInfoRepository
from persistence.model.forum import ForumNumInfo


class DataPipeline(BasePipeline):
    def __init__(self, config: PipelineConfig):
        super().__init__(config)
        self.task_info_repo = TaskInfoRepository(**config.task_info_repo)
        self.forum_post_info_repo = ForumPostInfoRepository(**config.data_repo)
        self.forum_basic_info_repo = ForumBasicInfoRepository(**config.data_repo)
        self.teacher_info_repo = TeacherInfoRepository(**config.data_repo)
        self.course_list_info_repo = CourseListInfoRepository(**config.data_repo)
        self.course_crowd_repo = CourseCrowdWeeklyRepo(**config.data_repo)
        self.error_repo = ErrorRepository(**config.data_repo)
        self.forum_num_repo = ForumNumInfoRepository(**config.data_repo)

    def processing(self, task_id: str, data):
        assert isinstance(data, dict)
        task_info = self.task_info_repo.get_task_info_by_id(task_id)
        for key, val in data.items():
            if key == 'forum_post_info':
                if data['forum_post_info'] == []:
                    logging.warning("[processing] forum_post_info is NULL.")
                else:
                    self.process_forum_post_info(task_info, val)
            elif key == 'semester_result_info':
                self.process_semester_result_info(task_info, val)
            elif key == 'course_list_info':
                self.process_course_list_info(data["course_list_info"])
            elif key == "error_list":
                self.process_error_list(data["error_list"])
            elif key == "forum_num_info":
                self.process_forum_num(data["forum_num_info"])
            else:
                logging.warning("[processing] unknown data fields: %s", key)

    def process_forum_num(self, forum_num_dic_list):
        forum_num_list = []
        for forum_num in forum_num_dic_list:
            forum_num_info = ForumNumInfo()
            forum_num_info.__dict__.update(forum_num)
            forum_num_list.append(forum_num_info)
        self.forum_num_repo.create_forum_num_infos(forum_num_list)

    def update_basic_data(self, task: Task):
        # TODO Get or Set proc
        try:
            semester_info = self.semester_base_repo.get_semester_basic_info_by_id(task.course_id)
        except ValueError:
            semester_info = SemesterBasicInfo()
            semester_info.semester_id = task.course_id
            # TODO course_group_id
            semester_info.semester_seq = 0
            semester_info.semester_url = task.url
            semester_info.semester_platform = task.platform
            # TODO platform_id
            semester_info.semester_label = task.term
            self.semester_base_repo.create_semester_basic_info(semester_info)
        course_info = CourseBasicInfo()
        pass

    def process_forum_post_info(self, task: Task, data: List[dict]):
        save_list = list()
        for forum_info in data:
            forum_name = forum_info['forum_name']
            if forum_name:
                forum_id = self.forum_basic_info_repo.get_forum_id_by_semester_id_and_name(task.course_id, forum_name)
                if not forum_id:
                    basic_info = ForumBasicInfo()
                    basic_info.forum_id = 0
                    basic_info.semester_id = task.course_id
                    basic_info.forum_plate = forum_name
                    self.forum_basic_info_repo.create_forum_basic_info(basic_info)
                    forum_id = self.forum_basic_info_repo.get_forum_id_by_semester_id_and_name(task.course_id,
                                                                                               forum_name)
                forum_info['forum_id'] = forum_id
            if (forum_info['forum_post_userrole'] == ForumPostInfo.OwnerRoleTypeEnum.kTeacher) or (
                    forum_info['forum_post_userrole'] == ForumPostInfo.OwnerRoleTypeEnum.kAssistant):
                # TODO find teacher id
                forum_reply_userid = self.teacher_info_repo.get_teacher_id_by_school_teacher(task.school, forum_info[
                    'forum_post_username'])
                if not forum_reply_userid:
                    # course_director_id = str(uuid.uuid1())
                    teacher_info = TeacherInfo()
                    teacher_info.teacher_id = forum_info["forum_reply_userid"]
                    teacher_info.teacher_name = forum_info['forum_post_username']
                    teacher_info.teacher_school = task.school
                    teacher_info.teacher_forumid = forum_info['forum_id']
                    self.teacher_info_repo.create_teacher_info(teacher_info)
                    # forum_reply_userid = teacher_info.teacher_id
                # forum_info['forum_reply_userid'] = forum_reply_userid
            else:
                # forum_info['forum_reply_userid'] = str(uuid.uuid1())
                pass
            info = ForumPostInfo()
            info.__dict__ = forum_info
            save_list.append(info)
        logging.info("start create forum data")
        save_list2 = list()
        for i in range(0, len(save_list)):
            save_list2.append(save_list[i])
            if (i + 1) % 20000 == 0:
                self.forum_post_info_repo.create_forum_post_infos(save_list2)
                save_list2.clear()
            elif i == len(save_list) - 1:
                self.forum_post_info_repo.create_forum_post_infos(save_list2)
                save_list2.clear()

    def process_semester_result_info(self, task: Task, data: dict):
        semester_result_info = SemesterResultInfo()
        semester_result_data = data
        semester_result_info.semester_id = task.course_id
        semester_result_info.semester_start_date = semester_result_data['semester_start_date']
        semester_result_info.semester_end_date = semester_result_data['semester_end_date']
        semester_result_info.semester_exam_info = semester_result_data['semester_exam_info']
        semester_result_info.semester_homework_info = semester_result_data['semester_homework_info']
        semester_result_info.semester_extension_info = semester_result_data['semester_extension_info']
        semester_result_info.semester_resource_info = semester_result_data['semester_resource_info']
        semester_result_info.semester_studentnum = semester_result_data['semester_studentnum']
        semester_result_info.semester_test_info = semester_result_data['semester_test_info']
        semester_result_info.update_time = semester_result_data['update_time']
        logging.info("[process_semester_result_info] teacher team: %s",
                     semester_result_data['semester_teacherteam_info'])
        semester_teacherteam_info = json.loads(semester_result_data['semester_teacherteam_info'])
        course_director_name = semester_teacherteam_info['course_director_name']
        teacher_team = semester_teacherteam_info['teacher_team']
        school = task.school
        # TODO 替换查询API，查询为空则添加
        course_director_id = self.teacher_info_repo.get_teacher_id_by_school_teacher(school, course_director_name)
        if course_director_id == '':
            course_director_id = str(uuid.uuid1())
            teacher_info = TeacherInfo()
            teacher_info.teacher_id = course_director_id
            teacher_info.teacher_name = course_director_name
            teacher_info.teacher_school = school
            self.teacher_info_repo.create_teacher_info(teacher_info)
        teacher_id_list = []
        semester_teacherteam_info = {}
        for teacher in teacher_team:
            teacher_id = self.teacher_info_repo.get_teacher_id_by_school_teacher(school, teacher)
            if teacher_id == '':
                teacher_id = str(uuid.uuid1())
                teacher_info = TeacherInfo()
                teacher_info.teacher_id = teacher_id
                teacher_info.teacher_name = teacher
                teacher_info.teacher_school = school
                self.teacher_info_repo.create_teacher_info(teacher_info)
            teacher_id_list.append(teacher_id)
        semester_teacherteam_info['course_director_id'] = course_director_id
        semester_teacherteam_info['teacher_id_list'] = teacher_id_list
        semester_result_info.semester_teacherteam_info = json.dumps(semester_teacherteam_info)
        self.semester_result_info_repo.create_semester_result_info(semester_result_info)

    def process_course_list_info(self, data: List[dict]):
        course_list_info_0 = CourseListInfo()
        course_list_info_0.__dict__.update(data[0])
        if course_list_info_0.platform == '北京学银在线教育科技有限公司':
            data = self.process_xueyinonline(data)
        for course_info in data:
            course_list_info = CourseListInfo()
            course_list_info.__dict__.update(course_info)
            course_list_info.course_id = 0
            save_time = course_list_info.save_time
            if course_list_info.platform == '国家开放大学出版社有限公司（荟学习网）':
                course_list_info = self.huixuexi_storage(course_list_info)
            elif course_list_info.platform == '优课联盟':
                course_list_info = self.youkelianmeng_storage(course_list_info)
            elif course_list_info.platform == "中国高校外语慕课平台":
                course_list_info = self.gaoxiaowaiyumuke_storage(course_list_info)
            elif course_list_info.platform == "中科云教育":
                course_list_info = self.zhongkeyun_storage(course_list_info)
            elif course_list_info.platform == "智慧职教":
                course_list_info = self.zhihuizhijiao_storage(course_list_info)
            elif course_list_info.platform == "学堂在线":
                course_list_info = self.xuetangzaixian_storage(course_list_info)
            elif course_list_info.platform == "浙江省高等学校在线开放课程共享平台":
                course_list_info = self.zhejiangmooc_storage(course_list_info)
            elif course_list_info.platform == '爱课程(中国大学MOOC)':
                course_list_info = self.icourse163_storage(course_list_info)
            elif course_list_info.platform == '安徽省网络课程学习中心平台':
                course_list_info = self.ehuixue_storage(course_list_info)
            elif course_list_info.platform == '北京超星尔雅教育科技有限公司':
                course_list_info = self.erya_storage(course_list_info)
            elif course_list_info.platform == '北京高校邦科技有限公司':
                course_list_info = self.gaoxiaobang_storage(course_list_info)
            elif course_list_info.platform == '北京高校优质课程研究会':
                course_list_info = self.livedu_storage(course_list_info)
            elif course_list_info.platform == '优学院（人民网公开课）':
                course_list_info = self.ulearning_storage(course_list_info)
            elif course_list_info.platform == '北京学银在线教育科技有限公司':
                course_list_info = self.xueyinonline_storage(course_list_info)
            elif course_list_info.platform == '重庆高校在线开放课程平台（重庆市教育委员会）':
                course_list_info = self.cqooc_storage(course_list_info)
            elif course_list_info.platform == '玩课网':
                course_list_info = self.wanke_storage(course_list_info)
            elif course_list_info.platform == '微知库数字校园学习平台':
                course_list_info = self.weizhiku_storage(course_list_info)
            elif course_list_info.platform == '优慕课':
                course_list_info = self.umooc_storage(course_list_info)
            elif course_list_info.platform == '人卫社MOOC':
                course_list_info = self.pmphmooc_storage(course_list_info)
            elif course_list_info.platform == '好大学在线':
                course_list_info = self.haodaxue_storage(course_list_info)
            elif course_list_info.platform == '华文慕课':
                course_list_info = self.chinesemooc_storage(course_list_info)
            elif course_list_info.platform == '智慧树':
                course_list_info = self.zhihuishu_storage(course_list_info)
            course_list_info.save_time = save_time
            crowd = course_list_info.list2crowd()
            crowd_id = self.course_crowd_repo.is_crowd_exsits(crowd)
            if not crowd_id:
                self.course_crowd_repo.create_course_crowd_weekly(crowd)
            else:
                self.course_crowd_repo.update_course_crowd_weekly(crowd, crowd_id)

    def process_xueyinonline(self, data: List[dict]):
        # 学银groupid修正函数
        print('modify xueyinonline groupid')
        course_data_list = []
        course_dict = {}
        for course_info in data:
            course_list_info = CourseListInfo()
            course_list_info.__dict__.update(course_info)
            if course_list_info.platform_course_id in course_dict:
                course_dict[course_list_info.platform_course_id].append(course_list_info)
            else:
                new_list = [course_list_info]
                course_dict[course_list_info.platform_course_id] = new_list
        for platform_course_id in course_dict:
            for course_info in course_dict[platform_course_id]:
                course_group_id = self.course_list_info_repo.get_group_id_by_term_id(course_info)
                if course_group_id:
                    for course in course_dict[platform_course_id]:
                        course.course_group_id = course_group_id
                    break
            for course_info in course_dict[platform_course_id]:
                course_data_list.append(course_info.__dict__)
        return course_data_list

    def huixuexi_storage(self, course_list_info: CourseListInfo):
        """
        荟学习源数据存储函数
        :param self:
        :param course_list_info:
        :return:
        """
        hash_str = course_list_info.course_name + course_list_info.school + course_list_info.teacher + course_list_info.platform
        hash_md5 = hashlib.md5(hash_str.encode("utf-8")).hexdigest()
        course_id = self.course_list_info_repo.get_courseid_by_courseinfo_except_date(course_list_info)
        print("--------------------" + course_list_info.course_name + "------------------")
        if course_id:
            course_list_info.course_group_id = hash_md5
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            course_id = self.course_list_info_repo.get_courseid_by_url_school(course_list_info)
            if course_id:
                course_list_info.course_group_id = self.course_list_info_repo.get_groupid_by_course_id(course_id)
                if course_list_info.course_group_id == '':
                    course_list_info.course_group_id = hash_md5
                self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
                course_list_info.course_id = course_id
            else:
                course_id = self.course_list_info_repo.get_courseid_by_name_teacher_term(course_list_info)
                if course_id:
                    course_list_info.course_group_id = self.course_list_info_repo.get_groupid_by_course_id(
                        course_id)
                    if course_list_info.course_group_id == '':
                        course_list_info.course_group_id = hash_md5
                    self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
                    course_list_info.course_id = course_id
                else:
                    course_list_info.course_group_id = hash_md5
                    self.course_list_info_repo.create_course_list_info(course_list_info)
                    course_list_info.course_id = self.course_list_info_repo.get_courseid_by_courseinfo(course_list_info)
        return course_list_info
        pass

    def youkelianmeng_storage(self, course_list_info: CourseListInfo):
        """
        优课联盟源数据存储函数
        :param course_list_info:
        :return:
        """
        course_list_info.platform_course_id = course_list_info.url.replace("http://www.uooc.net.cn/course/", '')
        if course_list_info.term == '':
            hash_str = course_list_info.platform_course_id + course_list_info.platform
            hash_md5 = hashlib.md5(hash_str.encode("utf-8")).hexdigest()
            course_list_info.course_group_id = hash_md5
            self.course_list_info_repo.update_status_by_url(course_list_info, 0)
        if course_list_info.term != '':
            hash_str = course_list_info.platform_course_id + course_list_info.platform
            hash_md5 = hashlib.md5(hash_str.encode("utf-8")).hexdigest()
            course_id = self.course_list_info_repo.get_courseid_by_url_term(course_list_info)
            if course_id:
                course_list_info.course_group_id = hash_md5
                self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
                course_list_info.course_id = course_id
            else:
                course_list_info.course_group_id = hash_md5
                self.course_list_info_repo.create_course_list_info(course_list_info)
                course_list_info.course_id = self.course_list_info_repo.get_courseid_by_courseinfo(
                    course_list_info)
        return course_list_info
        pass

    def xuetangzaixian_storage(self, course_list_info: CourseListInfo):
        """
        学堂在线源数据存储函数, 爬虫部分需修改
        :param course_list_info:
        :return:
        """
        course_list_info.course_name = re.sub(r'(20\d+春)|（20\d+春）|(20\d+秋)|（20\d+秋）', '',
                                              course_list_info.course_name).replace("()", '')
        if len(re.findall(r"live", course_list_info.url)):
            course_list_info.valid = 0
        elif len(re.findall(r"program", course_list_info.url)) and (
                len(re.findall(r"course", course_list_info.url)) == 0):
            course_list_info.valid = 0
        elif course_list_info.term == "体验版" or course_list_info.term == "体验课" \
                or course_list_info.term == "仅供预览请勿学习" \
                or course_list_info.term == "仅供预览":
            course_list_info.valid = 0
        elif course_list_info.course_name == "Studio测试资源勿删ww" or \
                course_list_info.course_name == "未开始报名" or \
                course_list_info.course_name == "测试用课程001" or \
                course_list_info.course_name == "结束报名" or \
                course_list_info.course_name == "开始报名，未开课" or \
                course_list_info.course_name == "微信支付":
            course_list_info.valid = 0
        elif course_list_info.term == '':
            course_list_info.valid = 0
        if course_list_info.valid != 0:
            try:
                course_group = course_list_info.url.replace("https://next.xuetangx.com/course/", '').split('/')[0]
                course_list_info.platform_course_id = "".join(
                    list(filter(str.isdigit, course_group))) + course_list_info.course_name
                course_list_info.platform_term_id = \
                    course_list_info.url.replace("https://next.xuetangx.com/course/", '').split('/')[1]
                hash_str = course_list_info.platform_course_id + course_list_info.platform
                hash_md5 = hashlib.md5(hash_str.encode("utf-8")).hexdigest()
                course_list_info.course_group_id = hash_md5
            except:
                pass
        else:
            course_list_info.platform_term_id = hashlib.md5(course_list_info.url.encode('utf-8')).hexdigest()
        course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id_except_term_id_number(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            if course_list_info.valid == 1 and course_list_info.term_id != 0:
                self.course_list_info_repo.create_course_list_info(course_list_info)
                course_list_group = self.course_list_info_repo.get_course_list_by_groupid(course_list_info)
                self.course_list_info_repo.update_term_id_number_by_groupid(course_list_group)
            else:
                self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        return course_list_info
        pass

    def zhejiangmooc_storage(self, course_list_info: CourseListInfo):
        """
        浙江高校源数据存储函数， 需改爬虫部分
        :param course_list_info:
        :return:
        """
        course_list_info.platform_term_id = course_list_info.url.replace("https://www.zjooc.cn/course/", "")
        hash_str = course_list_info.course_group_id + course_list_info.platform
        hash_md5 = hashlib.md5(hash_str.encode("utf-8")).hexdigest()
        course_list_info.course_group_id = hash_md5
        course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        return course_list_info
        pass

    def gaoxiaowaiyumuke_storage(self, course_list_info: CourseListInfo):
        """
        中国高校外语慕课平台， 需改爬虫部分
        :param course_list_info:
        :return:
        """
        course_list_info.platform_term_id = course_list_info.url.replace("http://moocs.unipus.cn/course/", "")
        hash_str = course_list_info.course_group_id + course_list_info.platform
        hash_md5 = hashlib.md5(hash_str.encode("utf-8")).hexdigest()
        course_list_info.course_group_id = hash_md5
        course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        return course_list_info
        pass

    def zhihuizhijiao_storage(self, course_list_info: CourseListInfo):
        """
        智慧职教平台， 需改爬虫部分
        :param course_list_info:
        :return:
        """
        if course_list_info.block == "MOOC学院":
            try:
                course_list_info.platform_course_id = \
                    course_list_info.url.replace("https://mooc.icve.com.cn/course.html?cid=", '').split("#oid=")[0]
                course_list_info.platform_term_id = course_list_info.platform_course_id + \
                                                    course_list_info.url.replace(
                                                        "https://mooc.icve.com.cn/course.html?cid=", '').split("#oid=")[
                                                        1]
                hash_str = course_list_info.platform_course_id + course_list_info.platform
                hash_md5 = hashlib.md5(hash_str.encode("utf-8")).hexdigest()
                course_list_info.course_group_id = hash_md5
            except:
                course_list_info.valid = 0
            course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
            if course_id:
                self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
                course_list_info.course_id = course_id
            else:
                self.course_list_info_repo.create_course_list_info(course_list_info)
                course_list_info.course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
            pass
        elif course_list_info.block == "资源库" or course_list_info.block == "数字课程":
            try:
                course_list_info.platform_course_id = \
                    course_list_info.url.replace(
                        "https://www.icve.com.cn/portal_new/courseinfo/courseinfo.html?courseid=", '').split("#oid=")[0]
                course_list_info.platform_term_id = course_list_info.platform_course_id
                hash_str = course_list_info.platform_course_id + course_list_info.platform
                hash_md5 = hashlib.md5(hash_str.encode("utf-8")).hexdigest()
                course_list_info.course_group_id = hash_md5
            except:
                course_list_info.valid = 0
            course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
            if course_id:
                self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
                course_list_info.course_id = course_id
            else:
                self.course_list_info_repo.create_course_list_info(course_list_info)
                course_list_info.course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
            pass
        return course_list_info
        pass

    def zhongkeyun_storage(self, course_list_info: CourseListInfo):
        """
        中科云，需要修改爬虫部分
        :param course_list_info:
        :return:
        """
        platform_course_id = course_list_info.url.split("params.courseId=")[1]
        course_list_info.platform_term_id = platform_course_id
        hash_str = course_list_info.course_name + course_list_info.school + course_list_info.teacher + course_list_info.platform
        hash_md5 = hashlib.md5(hash_str.encode("utf-8")).hexdigest()
        course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        if course_id:
            if course_list_info.valid:
                course_list_info.course_group_id = hash_md5
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            if course_list_info.valid:
                course_list_info.course_group_id = hash_md5
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        return course_list_info
        pass

    def livedu_storage(self, course_list_info: CourseListInfo):
        # 北京高校优质生成groupid
        kcid = course_list_info.url.split('kcid=', 1)[1]
        group = course_list_info.platform + kcid
        group_hash = hashlib.md5(group.encode('utf-8'))
        course_list_info.platform_course_id = kcid
        course_list_info.platform_term_id = kcid
        course_list_info.course_group_id = group_hash.hexdigest()
        # 北京高校优质学期id判断courseid
        course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        return course_list_info

    def icourse163_storage(self, course_list_info: CourseListInfo):
        # 爱课程使用url信息生成groupid
        url_valid = course_list_info.url.split('-', 1)[1]
        cid = url_valid.split('?', 1)[0]
        tid = url_valid.split('tid=', 1)[1]
        group = course_list_info.platform + cid
        group_hash = hashlib.md5(group.encode('utf-8'))
        course_list_info.platform_course_id = cid
        course_list_info.platform_term_id = tid
        course_list_info.course_group_id = group_hash.hexdigest()
        # 爱课程使用学期id来判断是否为新的courseid
        course_id = self.course_list_info_repo.get_courseid_by_url(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_url(course_list_info)
        return course_list_info

    def ulearning_storage(self, course_list_info: CourseListInfo):
        # 优学院groupid使用url中信息生成
        cid = course_list_info.url.split('courseID=', 1)[1]
        group = course_list_info.platform + cid
        group_hash = hashlib.md5(group.encode('utf-8'))
        course_list_info.platform_course_id = cid
        course_list_info.platform_term_id = cid
        course_list_info.course_group_id = group_hash.hexdigest()
        # 优学院使用学期id来判断
        course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        return course_list_info

    def gaoxiaobang_storage(self, course_list_info: CourseListInfo):
        cid = course_list_info.url.split('detail/', 1)[1]
        group = course_list_info.platform + cid
        group_hash = hashlib.md5(group.encode('utf-8'))
        course_list_info.platform_course_id = cid
        course_list_info.platform_term_id = cid
        course_list_info.course_group_id = group_hash.hexdigest()
        # 高校邦使用学期id来判断
        course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        return course_list_info

    def erya_storage(self, course_list_info: CourseListInfo):
        # 超星尔雅使用url生成groupid
        url_valid = course_list_info.url.split('course/', 1)[1]
        cid = url_valid.split('.html', 1)[0]
        group = course_list_info.platform + cid
        group_hash = hashlib.md5(group.encode('utf-8'))
        course_list_info.platform_course_id = cid
        course_list_info.platform_term_id = cid
        course_list_info.course_group_id = group_hash.hexdigest()
        # 超星尔雅使用学期id来判断courseid
        course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        return course_list_info

    def xueyinonline_storage(self, course_list_info: CourseListInfo):
        course_id = self.course_list_info_repo.get_courseid_by_url(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_url(course_list_info)
        return course_list_info

    def ehuixue_storage(self, course_list_info: CourseListInfo):
        course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        return course_list_info

    def cqooc_storage(self, course_list_info: CourseListInfo):
        course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        return course_list_info

    def umooc_storage(self, course_list_info: CourseListInfo):
        url = course_list_info.url
        group_id = url.split('=')[-1]
        hash_str = group_id + course_list_info.platform
        course_list_info.platform_course_id = group_id
        course_list_info.platform_term_id = group_id
        course_list_info.course_group_id = hashlib.md5(hash_str.encode('utf-8')).hexdigest()

        # 是否新学期
        course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_all_info(course_list_info)
        return course_list_info

    def wanke_storage(self, course_list_info: CourseListInfo):
        url = course_list_info.url
        if 'moocDetail' in url:
            group_id = url.split('/')[-2]
            term_id = url.split('/')[-1]
            course_list_info.platform_course_id = group_id
            course_list_info.platform_term_id = term_id
            hash_str = group_id + course_list_info.platform
            course_list_info.course_group_id = hashlib.md5(hash_str.encode('utf-8')).hexdigest()
            # 是否新学期
            course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
            if course_id:
                self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
                course_list_info.course_id = course_id
            else:
                self.course_list_info_repo.create_course_list_info(course_list_info)
                course_list_info.course_id = self.course_list_info_repo.get_courseid_by_all_info(course_list_info)
        else:
            group_id = url.split('/')[-1]
            course_list_info.platform_course_id = group_id
            course_list_info.platform_term_id = group_id
            hash_str = group_id + course_list_info.platform
            course_list_info.course_group_id = hashlib.md5(hash_str.encode('utf-8')).hexdigest()
            # 是否新学期
            course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
            if course_id:
                self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
                course_list_info.course_id = course_id
            else:
                self.course_list_info_repo.create_course_list_info(course_list_info)
                course_list_info.course_id = self.course_list_info_repo.get_courseid_by_all_info(course_list_info)
        return course_list_info

    def pmphmooc_storage(self, course_list_info: CourseListInfo):
        url = course_list_info.url
        term_id = url.split('=')[-1]
        course_list_info.platform_term_id = term_id
        hash_str = course_list_info.school + course_list_info.platform + course_list_info.team.split(',')[0]
        course_list_info.course_group_id = hashlib.md5(hash_str.encode('utf-8')).hexdigest()
        # 是否新学期
        course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_all_info(course_list_info)
        return course_list_info

    def chinesemooc_storage(self, course_list_info: CourseListInfo):
        url = course_list_info.url
        group_id = url.split('/')[-1]
        course_list_info.platform_course_id = group_id
        course_list_info.platform_term_id = group_id
        hash_str = group_id + course_list_info.platform
        course_list_info.course_group_id = hashlib.md5(hash_str.encode('utf-8')).hexdigest()
        # 是否新学期
        course_id = self.course_list_info_repo.get_courseid_by_url_term(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_all_info(course_list_info)
        return course_list_info

    def haodaxue_storage(self, course_list_info: CourseListInfo):
        url = course_list_info.url
        url = url.split('.mooc')[0]
        group_id = url.split('/')[-2]
        term_id = url.split('/')[-1]
        course_list_info.platform_course_id = group_id
        course_list_info.platform_term_id = term_id
        hash_str = group_id + course_list_info.platform
        course_list_info.course_group_id = hashlib.md5(hash_str.encode('utf-8')).hexdigest()
        # 是否新学期
        course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_all_info(course_list_info)
        return course_list_info


    def weizhiku_storage(self, course_list_info: CourseListInfo):
        url = course_list_info.url
        asub = url.split('?')[-1].split('&')[0]
        bsub = url.split('?')[-1].split('&')[1]
        group_id = bsub.split('=')[-1]
        course_list_info.platform_course_id = group_id
        course_list_info.platform_term_id = asub.split('=')[-1]
        hash_str = group_id + course_list_info.platform
        course_list_info.course_group_id = hashlib.md5(hash_str.encode('utf-8')).hexdigest()
        # 是否新学期
        course_id = self.course_list_info_repo.get_courseid_by_term_id(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id_except_term_id_number(course_list_info,
                                                                                           course_id)
            course_list_info.course_id = course_id
        else:
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_all_info(course_list_info)
            course_list_group = self.course_list_info_repo.get_course_list_by_groupid(course_list_info)
            self.course_list_info_repo.update_term_id_number_by_groupid_weizhiku(course_list_group)
        return course_list_info

    def zhihuishu_storage(self, course_list_info: CourseListInfo):
        # 是否新学期
        course_id = self.course_list_info_repo.get_courseid_by_url(course_list_info)
        if course_id:
            self.course_list_info_repo.update_list_info_by_course_id(course_list_info, course_id)
            course_list_info.course_id = course_id
        else:
            self.course_list_info_repo.create_course_list_info(course_list_info)
            course_list_info.course_id = self.course_list_info_repo.get_courseid_by_all_info(course_list_info)
        return course_list_info

    def process_course_list2task(self, data: List[dict]):
        # 1. 通过url判断更新的courselist课程是否存在task表中（查询结果是否等于fetcher数目）
        # 2. 等于则不添加，不等于则清空再添加
        # 3. schedule_task()
        FETCHER_NUM = 2
        for course_info in data:
            url = course_info['url']
            count = self.task_info_repo.get_tasknumber_info_by_url(url)
            if count == FETCHER_NUM:
                self.task_info_repo.set_task_update_time_by_url(url)
                continue
            elif course_info['status'] > 1:
                continue
            else:
                task_basic = Task()
                task_video = Task()
                task_forum = Task()
                task_basic.id = 0
                task_basic.course_id = course_info['course_id']
                task_basic.url = course_info['url']
                task_basic.course_name = course_info['course_name']
                task_basic.term = course_info['term']
                task_basic.platform = course_info['platform']
                task_basic.school = course_info['school']
                task_basic.teacher = course_info['teacher']
                task_basic.fetcher_type = 'basic'
                task_basic.crawled_plan_num = 1
                task_basic.crawled_finished_num = 0
                task_forum.crawled_time_gap = 600000
                task_forum = task_basic
                task_video = task_basic
                self.task_info_repo.delete_tasks_by_url(url)
                # self.task_info_repo.create_task_info(task_basic)
                task_forum.fetcher_type = 'forum'
                self.task_info_repo.create_task_info(task_forum)
                task_video.fetcher_type = 'video'
                self.task_info_repo.create_task_info(task_video)
        pass

    def process_error_list(self, error_list):
        if error_list:
            self.error_repo.create_error_list_infos(error_list)


if __name__ == '__main__':
    with open("cqooc20200524.pkl", "rb") as f:
        course_list = pickle.load(f)
    print(len(course_list['course_list_info']))
    config = PipelineConfig()
    config.task_info_repo = {
        "host": "192.168.232.254",
        "port": 3307,
        "username": "root",
        "password": "123qweASD!@#",
        "database": "mooc_test"
    }
    config.data_repo = {
        "host": "192.168.232.254",
        "port": 3307,
        "username": "root",
        "password": "123qweASD!@#",
        "database": "mooc_test"
    }
    test = DataPipeline(config)
    test.process_course_list_info(course_list['course_list_info'])
