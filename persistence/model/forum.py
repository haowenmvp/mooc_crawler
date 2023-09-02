from datetime import datetime

class ForumBasicInfo:

    def __init__(self):
        self.forum_id = 0
        self.semester_id = 0
        self.forum_plate = ''


class ForumPostInfo:
    class TopicTypeEnum:
        kTypeUnknown = 0
        kTypeMain = 1
        kTypeReply = 2

    class OwnerRoleTypeEnum:
        kTypeUnknown = 0
        kTeacher = 1
        kStudent = 2
        kAssistant = 3

    def __init__(self):
        self.id = 0
        self.platform = ''
        self.forum_post_id = ''
        self.forum_id = ''
        self.forum_name = ''
        self.forum_subject = ''
        self.forum_post_type = ForumPostInfo.TopicTypeEnum.kTypeUnknown
        self.forum_reply_id = ''
        self.forum_reply_userid = ''
        self.forum_post_username = ''
        self.forum_post_userrole = ForumPostInfo.OwnerRoleTypeEnum.kTypeUnknown
        self.forum_post_content = ''
        self.forum_post_time = datetime(1999, 1, 1)
        self.update_time = datetime(1999, 1, 1)

class ForumNumInfo:
    def __init__(self):
        self.id = 0
        self.platform = ''
        self.course_id = 0
        self.forum_num = 0
        self.accumulate_forum_num = 0
        self.save_time = datetime(1999, 1, 1)