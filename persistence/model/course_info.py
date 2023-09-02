from datetime import datetime


class CourseBasicInfo:

    def __init__(self):
        self.course_group_id = 0
        self.course_name = ''
        self.course_termnum = 0
        self.course_mainplatformid = 0
        self.course_mainplatform = ''
        self.course_schoolid = 0
        self.course_school = ''
        self.course_teacherid = 0
        self.course_teacher = ''
        self.course_start_date = datetime(1999, 1, 1)
        self.course_isquality = False
        self.course_qualitytime = datetime(1999, 1, 1)


class SemesterBasicInfo:

    def __init__(self):
        self.semester_id = 0
        self.course_group_id = 0
        self.semester_seq = 0
        self.semester_url = ''
        self.semester_platformid = 0
        self.semester_platform = ''
        self.semester_label = 0
        self.semester_crawltask = True


class SemesterResultInfo:

    def __init__(self):
        self.semester_id = 0
        self.semester_start_date = datetime(1999, 1, 1)
        self.semester_end_date = datetime(1999, 1, 1)
        self.semester_teacherteam_info = ''
        self.semester_studentnum = 0
        self.semester_resource_info = ''
        self.semester_homework_info = ''
        self.semester_interact_info = ''
        self.semester_exam_info = ''
        self.semester_test_info = ''
        self.semester_extension_info = ''
        self.update_time = datetime(1999, 1, 1)