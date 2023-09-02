from datetime import datetime
from persistence.model.basic_info import CourseListInfo


class CourseException:

    def __init__(self, course_list_info: CourseListInfo, post_num_now, post_num_yesterday, crowd_yesterday, total_crowd_yesterday):
        self.course_id = course_list_info.course_id
        self.url = course_list_info.url
        self.course_name = course_list_info.course_name
        self.term_id = course_list_info.term_id
        self.term = course_list_info.term
        self.team = course_list_info.team
        self.platform = course_list_info.platform
        self.school = course_list_info.school
        self.teacher = course_list_info.teacher
        self.start_date = course_list_info.start_date
        self.end_date = course_list_info.end_date
        self.save_time = course_list_info.save_time
        self.update_time = datetime.now()
        self.status = course_list_info.status
        self.crowd = course_list_info.crowd
        self.total_crowd = course_list_info.total_crowd
        self.crowd_num = course_list_info.crowd_num if course_list_info.crowd_num != "" else None
        self.total_crowd_num = course_list_info.total_crowd_num if course_list_info.total_crowd_num != "" else None
        self.term_number = course_list_info.term_number
        self.platform_course_id = course_list_info.platform_course_id
        self.platform_term_id = course_list_info.platform_term_id
        self.course_group_id = course_list_info.course_group_id
        self.valid = course_list_info.valid
        self.exception_type = ""
        self.post_num_now = post_num_now if post_num_now != "" else None
        self.post_num_yesterday = post_num_yesterday if post_num_yesterday != "" else None
        self.crowd_yesterday = crowd_yesterday if crowd_yesterday != "" else None
        self.total_crowd_yesterday = total_crowd_yesterday if total_crowd_yesterday != "" else None

    def generate_exception_type(self):
        if self.post_num_now is not None and self.post_num_yesterday is not None and self.post_num_now < \
                self.post_num_yesterday:
            self.exception_type = self.exception_type + "1"
        if self.post_num_now is not None and self.post_num_yesterday is not None and self.post_num_yesterday > 0 and \
                self.post_num_now > 1.5*self.post_num_yesterday:
            self.exception_type = self.exception_type + "2"
        if self.crowd_num is not None and self.crowd_yesterday is not None and self.crowd_yesterday > 0 and \
                self.crowd_num > 1.5*self.crowd_yesterday:
            self.exception_type = self.exception_type + "3"
        if self.total_crowd_num is not None and self.total_crowd_yesterday is not None and self.total_crowd_yesterday \
                > 0 and self.total_crowd_num > 1.5*self.total_crowd_yesterday:
            self.exception_type = self.exception_type + "4"