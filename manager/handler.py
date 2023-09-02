import logging
import uuid

from datetime import datetime, timedelta
from rpc.http import JsonRPCHandler

from persistence.model.task import Task


class ManagerHandler(JsonRPCHandler):
    def schedule_task_handler(self, task_id: str):
        task_info = self.server.task_info_repo.get_task_info_by_id(task_id)
        self.server.scheduler.schedule_task(task_info)
        return self.build_res()

    def ask_course_list_on_handler(self, platform, url):
        course_list = self.server.course_list_info_repo.get_on_course_list(platform)
        login_info = None
        for login_data in self.server.config.platform_login_data:
            if login_data["domain"] == url:
                login_info = login_data["login_infos"][0]
                break
        return self.build_res((course_list, login_info))

    def ask_login_info_handler(self, task_id: str, login_info: dict):
        task_info = self.server.task_info_repo.get_task_info_by_id(task_id)
        if not login_info["cookies"]:
            login_info = self.server.login_info_mgr.get_login_info_rand(task_info.url)
            logging.info(
                "[ManagerHandler.ask_login_info_handler] Old logininfo is None. Taskid: [%s],url: [%s]. Send a new one.",
                task_info.id,
                task_info.url)
        else:
            login_info = self.server.login_info_mgr.do_login_domain(task_info.url, login_info)
        if not login_info:
            logging.warning("[ManagerHandler.ask_login_info_handler] No login info of task: [%s],url: [%s]",
                            task_info.id,
                            task_info.url)
            return self.build_res()
        else:
            return self.build_res(login_info.__dict__)

    def report_task_start_handler(self, client_id: str, task_id: str):
        self.server.client_id_tasks_dict[client_id] = task_id
        task_info = self.server.task_info_repo.get_task_info_by_id(task_id)
        self.server.scheduler.lock.acquire()
        scheduletask = self.server.scheduler.schedule_task_repo.get_scheduletask_by_taskid_crawltime(task_id,
                                                                                                     task_info.crawled_next_time)
        if scheduletask:
            self.server.scheduler.schedule_task_repo.update_task_start(scheduletask.id)
        self.server.scheduler.lock.release()
        return self.build_res()

    def report_start_pipeline_handler(self, task_id: str):
        task_info = self.server.task_info_repo.get_task_info_by_id(task_id)
        self.server.scheduler.lock.acquire()
        scheduletask = self.server.scheduler.schedule_task_repo.get_scheduletask_by_taskid_crawltime(task_id,
                                                                                                     task_info.crawled_next_time)
        if scheduletask:
            self.server.scheduler.schedule_task_repo.update_task_start_handle(scheduletask.id)
        self.server.scheduler.lock.release()
        return self.build_res()

    def report_crawl_finish_handler(self, client_id: str, task_id: str, crawl_num: str):
        task_info = self.server.task_info_repo.get_task_info_by_id(task_id)
        self.server.scheduler.lock.acquire()
        scheduletask = self.server.scheduler.schedule_task_repo.get_scheduletask_by_taskid_crawltime(task_id,
                                                                                                     task_info.crawled_next_time)
        if scheduletask:
            self.server.scheduler.schedule_task_repo.update_crawl_finish(scheduletask.id, int(crawl_num))
        self.server.scheduler.lock.release()
        return self.build_res()

    def report_task_finish_handler(self, client_id: str, task_id: str):
        return self.build_res()

    def report_task_interrupt_handler(self, client_id: str, task_id: str):

        task_info = self.server.task_info_repo.get_task_info_by_id(task_id)
        self.server.scheduler.lock.acquire()
        scheduletask = self.server.scheduler.schedule_task_repo.get_scheduletask_by_taskid_crawltime(task_id,
                                                                                                     task_info.crawled_next_time)
        if scheduletask:
            self.server.scheduler.schedule_task_repo.update_task_status(scheduletask.id, scheduletask.kStatusInqueue)
        self.server.scheduler.lock.release()
        if client_id not in self.server.client_id_addr_dict.keys():
            logging.info("[ManagerHandler.report_task_interrupt_handler] Unregister client: [%s]", client_id)
            return self.build_err("Unregister client")
        try:
            if self.server.client_id_tasks_dict[client_id] == task_id:
                self.server.client_id_tasks_dict.pop(client_id)
        except KeyError:
            pass
        self.server.scheduler.lock.release()
        return self.build_res()

    def report_task_failed_handler(self, client_id: str, task_id: str):
        if self.server.client_id_tasks_dict[client_id] is None:
            return self.build_res()

        task_info = self.server.task_info_repo.get_task_info_by_id(task_id)
        self.server.scheduler.lock.acquire()
        scheduletask = self.server.scheduler.schedule_task_repo.get_scheduletask_by_taskid_crawltime(task_id,
                                                                                                     task_info.crawled_next_time)
        scheduletask.crawled_failed_num += 1
        if scheduletask:
            self.server.scheduler.schedule_task_repo.update_after_task_failed(scheduletask.id)
        assert isinstance(task_info, Task)
        self.server.scheduler.lock.release()
        if task_info.crawled_failed_restart == Task.kFailedStrategyCancel:
            pass
        elif task_info.crawled_failed_restart == Task.kFailedStrategyRetryImmediately:
            self.server.scheduler.schedule_task(task_info)
        elif task_info.crawled_failed_restart == Task.kFailedStrategyRetryNextTime:
            task_info.crawled_next_time += timedelta(seconds=task_info.crawled_time_gap)
            self.server.scheduler.schedule_task(task_info)
        return self.build_res()

    def get_platform_config_list_handler(self, client_id: str):
        if client_id not in self.server.client_id_addr_dict.keys():
            logging.info("[ManagerHandler.get_platform_config_list_handler] Unregister client: [%s]", client_id)
            return self.build_err("Unregister client")

        res = list()
        for config in self.server.platform_config_list:
            res.append(config.__dict__)
        return self.build_res(res)

    def keep_alive_handler(self, client_id: str):
        if client_id not in self.server.client_id_addr_dict.keys():
            logging.info("[ManagerHandler.keep_alive_handler] Unregister client: [%s]", client_id)
            return self.build_err("Unregister client")

        self.server.alive_id_time_dict[client_id] = datetime.now()
        logging.debug("[ManagerHandler.keep_alive_handler] Client [%s] heartbeats.", client_id)
        return self.build_res(None)

    def register_handler(self):
        remote_addr = self.client_address
        client_id = str(uuid.uuid1())

        self.server.client_id_addr_dict[client_id] = remote_addr
        self.server.alive_id_time_dict[client_id] = datetime.now()
        self.server.client_id_tasks_dict[client_id] = set()

        logging.info("[ManagerHandler.register_handler] Client register [%s].", client_id)
        return self.build_res(client_id)
