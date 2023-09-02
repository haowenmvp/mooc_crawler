import datetime
from typing import List, Tuple

from persistence.model.task import ScheduleTask
from persistence.db.api.mysql_api import MysqlApi


class ScheduleTaskInfoRepository:
    kTableName = 'schedule_task'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def create_schedule_task_info(self, schedule_task: ScheduleTask):
        assert isinstance(schedule_task, ScheduleTask)
        self.create_schedule_task_infos([schedule_task])

    def create_schedule_task_infos(self, schedule_task_list: List[ScheduleTask]):
        assert isinstance(schedule_task_list, list)
        data_lines = []
        for schedule_task in schedule_task_list:
            schedule_task = schedule_task.__dict__
            schedule_task.pop('login_info')
            schedule_task['create_time'] = datetime.datetime.now()
            data_lines.append(schedule_task)
        # data_lines = [schedule_task.__dict__ for schedule_task in schedule_task_list]
        self.__db_api.insert(self.kTableName, data_lines)

    # 表中只存在每个task最新一次的调度记录
    def get_schedule_task_info_by_task_id(self, task_id) -> ScheduleTask:
        fields = self.__get_schedule_task_info_field()
        data = self.__db_api.query(self.kTableName, fields, {"task_id = ?": (task_id,)})
        if len(data) == 0:
            # raise KeyError("Task %s not found.", task_id)
            return None
        data = data[0]
        return self.__construct_schedule_task_info(data)

    def get_schedule_task_info_by_id(self, id) -> ScheduleTask:
        fields = self.__get_schedule_task_info_field()
        data = self.__db_api.query(self.kTableName, fields, {"id = ?": (id,)})
        if len(data) == 0:
            # raise KeyError("Task %s not found.", task_id)
            return None
        data = data[0]
        return self.__construct_schedule_task_info(data)

    def get_task_status_info_by_id(self, id) -> int:
        res = self.__db_api.query(self.kTableName, ['status'], {"id = ?": (id,)})
        if not res:
            return ''
        else:
            return res[0][0]

    def update_task_status(self, id, status: int):
        self.__db_api.update(self.kTableName, [{"status": status}],
                             [{'id = ?': (id,)}])

    def update_task_start(self, id: int):
        self.__db_api.update(self.kTableName, [{"status": ScheduleTask.kStatusInProcess,
                                                "start_time": datetime.datetime.now()}],
                             [{'id = ?': (id,)}])

    def update_task_start_handle(self, id: int):
        self.__db_api.update(self.kTableName, [{"start_handle_time": datetime.datetime.now()}],
                             [{'id = ?': (id,)}])

    def update_after_task_finish(self, id: int):
        schedule_task = self.get_schedule_task_info_by_id(id)
        if schedule_task:
            schedule_task.update_time = datetime.datetime.now()
            schedule_task.crawled_finished_num += 1

            self.__db_api.update(self.kTableName,
                                 [{"status": schedule_task.kStatusFinished,
                                   "crawled_finished_num": schedule_task.crawled_finished_num,
                                   "update_time": schedule_task.update_time,
                                   "finish_time": datetime.datetime.now()}],
                                 [{'id = ?': (id,)}])

    def update_crawl_finish(self, id: int, crawl_num: int):
        schedule_task = self.get_schedule_task_info_by_id(id)
        if schedule_task:
            schedule_task.crawl_finish_time = datetime.datetime.now()

            self.__db_api.update(self.kTableName,
                                 [{"crawl_finish_time": schedule_task.crawl_finish_time, "crawl_num": crawl_num}],
                                 [{'id = ?': (id,)}])

    def update_after_task_failed(self, id: int):
        schedule_task = self.get_schedule_task_info_by_id(id)
        if schedule_task:
            schedule_task.update_time = datetime.datetime.now()
            schedule_task.crawled_failed_num += 1

            self.__db_api.update(self.kTableName,
                                 [{"status": schedule_task.kStatusFailed, "update_time": schedule_task.update_time,
                                   "crawled_failed_num": schedule_task.crawled_failed_num}],
                                 [{'id = ?': (id,)}])

    def get_scheduletask_by_taskid_crawltime(self, task_id: int, crawled_next_time: datetime):
        fields = self.__get_schedule_task_info_field()
        data = self.__db_api.query(self.kTableName, fields,
                                   {"task_id = ?": (task_id,), "crawled_next_time = ?": (crawled_next_time,)})
        if len(data) == 0:
            # raise KeyError("Task %s not found.", task_id)
            return None
        data = data[0]
        return self.__construct_schedule_task_info(data)

    def get_id_by_taskid_crawltime(self, task_id: int, crawled_next_time: datetime):
        res = self.__db_api.query(self.kTableName, ['id'],
                                  {"task_id = ?": (task_id,), "crawled_next_time = ?": (crawled_next_time,)})
        if not res:
            return ''
        else:
            return res[0][0]

    def is_scheduletask_exists(self, scheduletask: ScheduleTask):
        if self.get_id_by_taskid_crawltime(scheduletask.task_id, scheduletask.crawled_next_time) == '':
            return False
        else:
            return True

    @classmethod
    def __get_schedule_task_info_field(cls) -> list:
        fields = [
            'id',
            'task_id',
            'course_id',
            'url',
            'fetcher_type',
            'status',
            'crawled_finished_num',
            'crawled_next_time',
            'update_time',
            'crawled_failed_num',
            'start_time',
            'finish_time',
            'create_time',
            'crawl_finish_time',
            'start_handle_time'
        ]
        return fields

    @classmethod
    def __construct_schedule_task_info(cls, data: Tuple) -> ScheduleTask:
        schedule_task = ScheduleTask()
        schedule_task.id = data[0]
        schedule_task.task_id = data[1]
        schedule_task.course_id = data[2]
        schedule_task.url = data[3]
        schedule_task.fetcher_type = data[4]
        schedule_task.status = data[5]
        schedule_task.crawled_finished_num = data[6]
        schedule_task.crawled_next_time = data[7]
        schedule_task.update_time = data[8]
        schedule_task.crawled_failed_num = data[9]
        schedule_task.start_time = data[10]
        schedule_task.finish_time = data[11]
        schedule_task.create_time = data[12]
        schedule_task.crawl_finish_time = data[13]
        schedule_task.start_handle_time = data[14]
        return schedule_task
