import datetime
from typing import List, Tuple

from persistence.model.basic_info import CourseListInfo
from persistence.model.task import Task
from persistence.db.api.mysql_api import MysqlApi


class TaskInfoRepository:
    kTableName = 'task_info'

    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.__host = host
        self.__port = port
        self.__user = username
        self.__password = password
        self.__database = database

        self.api_type = MysqlApi
        self.__db_api = self.api_type(host=host, port=port, username=username, password=password, database=database)

    def get_incomplete_task_info_list(self) -> List[Task]:
        fields = self.__get_task_info_field()
        data_lines = self.__db_api.query(self.kTableName, fields,
                                         {"is_need_crawled = 1": (),
                                          "crawled_plan_num < 0 or crawled_finished_num < crawled_plan_num": ()
                                          })
        res = list()
        for line in data_lines:
            task = self.__construct_task_info('', line)
            res.append(task)
        return res

    def get_all_task_info(self) -> List[Task]:
        fields = ['id']
        fields.extend(self.__get_task_info_field())
        data_lines = self.__db_api.query(self.kTableName, fields, {})
        res = list()
        for line in data_lines:
            task = self.__construct_task_info('', line)
            res.append(task)
        return res

    def get_task_info_by_id(self, task_id) -> Task:
        fields = self.__get_task_info_field()
        data = self.__db_api.query(self.kTableName, fields, {"id = ?": (task_id,)})
        if len(data) == 0:
            raise KeyError("Task %s not found.", task_id)
        data = data[0]
        return self.__construct_task_info(task_id, data)

    def get_if_task_by_course_id(self, course_id: int) -> bool:
        fields = self.__get_task_info_field()
        data = self.__db_api.query(self.kTableName, fields, {"course_id = ?": (course_id,)})
        if len(data) == 0:
            return False
        else:
            return True

    def get_tasknumber_info_by_url(self, url: str) -> int:
        fields = self.__get_task_info_field()
        data = self.__db_api.query(self.kTableName, fields, {"url = ?": (url,)})
        return len(data)

    def delete_tasks_by_url(self, url: str):
        self.__db_api.delete(self.kTableName, {'url = ?': (url,)})

    def set_task_update_time_by_url(self, url):
        self.__db_api.update(self.kTableName,
                             [{'task_update_time': datetime.datetime.now()}],
                             [{'url = ?': (url,)}])

    def update_info_after_finish(self, task_id):
        task = self.get_task_info_by_id(task_id)
        task.save_time = datetime.datetime.now()
        task.crawled_finished_num += 1
        task.crawled_next_time = task.crawled_next_time + datetime.timedelta(seconds=task.crawled_time_gap)

        self.__db_api.update(self.kTableName, [{"save_time": task.save_time,
                                                "crawled_finished_num": task.crawled_finished_num,
                                                "crawled_next_time": task.crawled_next_time}], [{'id = ?': (task_id,)}])

    def create_task_info(self, task_info: Task):
        assert isinstance(task_info, Task)
        self.create_task_infos([task_info])

    def create_task_infos(self, task_info_list: List[Task]):
        assert isinstance(task_info_list, list)
        data_lines = [task_info.__dict__ for task_info in task_info_list]
        self.__db_api.insert(self.kTableName, data_lines)


    @classmethod
    def __get_task_info_field(cls) -> list:
        fields = ['id',
                  'course_id',
                  'url',
                  'course_name',
                  'term',
                  'platform',
                  'school',
                  'teacher',
                  'save_time',
                  'fetcher_type',
                  'is_need_crawled',
                  'crawled_plan_num',
                  'crawled_finished_num',
                  'crawled_time_gap',
                  'crawled_next_time',
                  'crawled_failed_restart',
                  'task_create_time',
                  'task_update_time'
                  ]
        return fields

    @classmethod
    def __construct_task_info(cls, task_id: str, data: Tuple) -> Task:
        task = Task()
        # if not task_id:
        #     task.id = data[0]
        #     id_offset = 1
        # else:
        #     task.id = task_id
        #     id_offset = 0
        task.id = data[0]
        id_offset = 1
        task.course_id = data[id_offset + 0]
        task.url = data[id_offset + 1]
        task.course_name = data[id_offset + 2]
        task.term = data[id_offset + 3]
        task.platform = data[id_offset + 4]
        task.school = data[id_offset + 5]
        task.teacher = data[id_offset + 6]
        task.save_time = data[id_offset + 7]
        task.fetcher_type = data[id_offset + 8]
        task.is_need_crawled = data[id_offset + 9]
        task.crawled_plan_num = data[id_offset + 10]
        task.crawled_finished_num = data[id_offset + 11]
        task.crawled_time_gap = data[id_offset + 12]
        task.crawled_next_time = data[id_offset + 13]
        task.crawled_failed_restart = data[id_offset + 14]
        task.task_create_time = data[id_offset + 15]
        task.task_update_time = data[id_offset + 16]
        return task
