from datetime import datetime


class LoginInfo:
    def __init__(self):
        self.cookies = list()
        self.session = dict()
        self.user_agent = ''
        self.proxy = ''
        self.login_data = dict()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def load_info(self, logininfo: dict):
        self.login_data = logininfo["login_data"]
        self.cookies = logininfo["cookies"]
        self.session = logininfo["session"]
        self.proxy = logininfo["proxy"]
        self.user_agent = logininfo["user_agent"]


class Task:
    # 失败不重试
    kFailedStrategyCancel = 0
    # 失败立即重试
    kFailedStrategyRetryImmediately = 1
    # 失败下次重试
    kFailedStrategyRetryNextTime = 2

    def __init__(self):
        self.id = 0
        self.course_id = 0
        self.url = ''
        self.course_name = ''
        self.term = ''
        self.platform = ''
        self.school = ''
        self.teacher = ''
        self.save_time = datetime(1999, 1, 1)
        self.fetcher_type = ''
        self.is_need_crawled = False
        self.crawled_plan_num = 0
        self.crawled_finished_num = 0
        self.crawled_time_gap = 0
        self.crawled_next_time = datetime(1999, 1, 1)
        self.crawled_failed_restart = Task.kFailedStrategyCancel
        self.task_create_time = datetime.now()
        self.task_update_time = datetime.now()

    def __gt__(self, other):
        if isinstance(other, Task):
            return self.crawled_next_time > other.crawled_next_time
        elif isinstance(other, datetime):
            return self.crawled_next_time > other
        else:
            raise TypeError("Can only compare with Task or datetime.")

    def __lt__(self, other):
        if isinstance(other, Task):
            return self.crawled_next_time < other.crawled_next_time
        elif isinstance(other, datetime):
            return self.crawled_next_time < other
        else:
            raise TypeError("Can only compare with Task or datetime.")

    def __ge__(self, other):
        if isinstance(other, Task):
            return self.crawled_next_time >= other.crawled_next_time
        elif isinstance(other, datetime):
            return self.crawled_next_time >= other
        else:
            raise TypeError("Can only compare with Task or datetime.")

    def __le__(self, other):
        if isinstance(other, Task):
            return self.crawled_next_time <= other.crawled_next_time
        elif isinstance(other, datetime):
            return self.crawled_next_time <= other
        else:
            raise TypeError("Can only compare with Task or datetime.")


class ScheduleTask:
    kStatusWaitSchedule = 0
    kStatusInqueue = 1
    kStatusInProcess = 2
    kStatusFinished = 3
    kStatusFailed = 4

    def __init__(self):
        self.id = 0
        self.task_id = 0
        self.course_id = 0
        self.url = ''
        self.fetcher_type = ''
        self.status = ScheduleTask.kStatusWaitSchedule
        self.crawled_next_time = datetime(1999, 1, 1)
        self.login_info = LoginInfo()
        self.update_time = datetime.now()
        self.crawled_finished_num = 0
        self.crawled_failed_num = 0
        self.start_time = datetime(1999, 1, 1)
        self.finish_time = datetime(1999, 1, 1)
        self.create_time = datetime(1999, 1, 1)
        self.crawl_finish_time = datetime(1999, 1, 1)
        self.start_handle_time = datetime(1999, 1, 1)

    def load_task(self, task: Task):
        self.task_id = task.id
        self.course_id = task.course_id
        self.url = task.url
        self.fetcher_type = task.fetcher_type
        self.status = ScheduleTask.kStatusWaitSchedule
        self.crawled_next_time = task.crawled_next_time
        self.update_time = datetime.now()
