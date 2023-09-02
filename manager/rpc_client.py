from typing import List

from persistence.model.platform_config import PlatformConfig
from rpc.http import JsonRPCClient


class ManagerClient(JsonRPCClient):
    def __init__(self, rpc_url: str):
        super().__init__(rpc_url)
        self.client_id = ''

    def schedule_task(self, task_id: str):
        return self.call("schedule_task", task_id=task_id)

    def ask_course_list_on(self, platform: str, url: str):
        return self.call("ask_course_list_on", platform=platform, url=url)

    def ask_login_info(self, task_id: str, login_info: dict):
        return self.call("ask_login_info", task_id=task_id, login_info=login_info)

    def report_task_start(self, task_id: int):
        return self.call("report_task_start", client_id=self.client_id, task_id=task_id)

    def report_crawl_finish(self, task_id: int, crawl_num: str):
        return self.call("report_crawl_finish", client_id=self.client_id, task_id=task_id, crawl_num=crawl_num)

    def report_task_finish(self, task_id: int):
        return self.call("report_task_finish", client_id=self.client_id, task_id=task_id)

    def report_start_pipeline(self, task_id: int):
        return self.call("report_start_pipeline", task_id=task_id)

    def report_task_failed(self, task_id: int):
        return self.call("report_task_failed", client_id=self.client_id, task_id=task_id)

    def report_task_interrupt(self, task_id: int):
        return self.call("report_task_start", client_id=self.client_id, task_id=task_id)

    def get_platform_config_list(self) -> List[PlatformConfig]:
        config_list = self.call('get_platform_config_list', client_id=self.client_id)

        res = list()

        for config_dict in config_list:
            config = PlatformConfig()
            config.__dict__.update(config_dict)
            res.append(config)

        return res

    def send_keep_alive(self):
        return self.call("keep_alive", client_id=self.client_id)

    def register(self):
        """
        Register to server
        :return: Register info
        """
        resp = self.call('register')
        self.client_id = resp
        return resp
