import abc
import logging
import traceback

from persistence.db.task_info_repo import TaskInfoRepository
from persistence.db.schedule_task_repo import ScheduleTaskInfoRepository
from persistence.model.pipeline_config import PipelineConfig
from manager.rpc_client import ManagerClient


class BasePipeline:
    def __init__(self, config: PipelineConfig):
        self.mgr_rpc_client = ManagerClient(config.manager_rpc_url)
        self.task_info_repo = TaskInfoRepository(**config.task_info_repo)
        self.schedule_task_repo = ScheduleTaskInfoRepository(**config.task_info_repo)

    def process(self, task_id: str, data):
        self.before_process(task_id, data)
        self.processing(task_id, data)
        self.after_process(task_id, data)

    def before_process(self, task_id: str, data):
        pass

    @abc.abstractmethod
    def processing(self, task_id: str, data):
        pass

    def after_process(self, task_id: str, data):
        self.__update_schedule_task_info(task_id)
        self.__update_task_info(task_id)
        self.__check_task_plan(task_id)

    def __update_task_info(self, task_id: str):
        logging.info("[__update_task_info] start update task info")
        try:
            self.task_info_repo.update_info_after_finish(task_id)
        except Exception as e:
            logging.error("[__update_task_info] update task info failed. err: %s", e)
            logging.error('[__update_task_info] %s', traceback.format_exc())
            raise e

    def __check_task_plan(self, task_id: str):
        logging.info("[__check_task_plan] start check task plan")
        task = self.task_info_repo.get_task_info_by_id(task_id)
        try:
            if task.crawled_plan_num < 0 or task.crawled_finished_num < task.crawled_plan_num:
                self.mgr_rpc_client.schedule_task(task_id)
        except Exception as e:
            logging.error("[__check_task_plan] cannot contact manager. err: %s", e)
            logging.error('[__check_task_plan] %s', traceback.format_exc())
            raise e

    def __update_schedule_task_info(self, task_id: str):
        logging.info("[__update_schedule_task_info] start update schedule task info")
        task_info = self.task_info_repo.get_task_info_by_id(task_id)
        scheduletask = self.schedule_task_repo.get_scheduletask_by_taskid_crawltime(task_id,
                                                                                    task_info.crawled_next_time)
        if scheduletask:
            try:
                self.schedule_task_repo.update_after_task_finish(scheduletask.id)
            except Exception as e:
                print(scheduletask.__dict__)
                logging.warning(
                    "[__update_schedule_task_info] scheduletask is not in the schedule list : task_id is %s,crawled_next_time is %s",
                    task_id, task_info.crawled_next_time)
                raise e
        else:
            logging.warning(
                "[__update_schedule_task_info] scheduletask is not in the schedule list : task_id is %s,crawled_next_time is %s",
                task_id, task_info.crawled_next_time)
