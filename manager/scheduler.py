import logging
import datetime

from . import timer

from manager.login_info_mgr import LoginInfoManager
from persistence.model.task import Task, ScheduleTask
from persistence.db.schedule_task_repo import ScheduleTaskInfoRepository
import threading

class Scheduler:
    def __init__(self, queue_push_func, login_info_mgr: LoginInfoManager,
                 schedule_task_repo: ScheduleTaskInfoRepository):
        self.queue_push_func = queue_push_func
        self.login_info_mgr = login_info_mgr

        self.task_timer_dict = dict()
        self.schedule_task_repo = schedule_task_repo
        self.lock = threading.Lock()

    def add_task(self, scheduletask: ScheduleTask):
        scheduletask.login_info = self.login_info_mgr.get_login_info_rand(scheduletask.url)
        if not scheduletask.login_info:
            logging.warning("[Scheduler.add_task] No login info of task: [%s],url: [%s]", scheduletask.task_id,
                            scheduletask.url)

        now = datetime.datetime.now()
        if scheduletask.crawled_next_time <= now:
            self.lock.acquire()
            logging.info("[Scheduler.add_task] task [%s] : %s <= %s", scheduletask.task_id,
                         scheduletask.crawled_next_time.strftime("%Y-%m-%d %H:%M:%S"),
                         datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            if not self.schedule_task_repo.is_scheduletask_exists(scheduletask):
                scheduletask.status = scheduletask.kStatusInqueue
                self.schedule_task_repo.create_schedule_task_info(scheduletask)
                scheduletask.id = self.schedule_task_repo.get_id_by_taskid_crawltime(scheduletask.task_id,
                                                                                     scheduletask.crawled_next_time)
                self.queue_push_func(scheduletask)
                logging.info(
                    "[Scheduler.add_task] task [%s] is not in schedule_task list,add it to schedule_task list and put it into task_queue.",
                    scheduletask.task_id)
            else:
                scheduletask.id = self.schedule_task_repo.get_id_by_taskid_crawltime(scheduletask.task_id,
                                                                                     scheduletask.crawled_next_time)

            if self.schedule_task_repo.get_task_status_info_by_id(scheduletask.id) == scheduletask.kStatusWaitSchedule:
                self.queue_push_func(scheduletask)
                scheduletask.status = scheduletask.kStatusInqueue
                self.schedule_task_repo.update_task_status(scheduletask, scheduletask.kStatusInqueue)
                logging.info("[Scheduler.add_task] task [%s] is not scheduled, put it into task_queue.",
                             scheduletask.task_id)
                self.lock.release()
            elif self.schedule_task_repo.get_task_status_info_by_id(scheduletask.id) == scheduletask.kStatusInqueue:
                logging.info("[Scheduler.add_task] task [%s] is in task_queue.", scheduletask.task_id)
                self.lock.release()
            elif self.schedule_task_repo.get_task_status_info_by_id(scheduletask.id) == scheduletask.kStatusInProcess:
                logging.info("[Scheduler.add_task] task [%s] is in process now.", scheduletask.task_id)
                self.lock.release()
            elif self.schedule_task_repo.get_task_status_info_by_id(scheduletask.id) == scheduletask.kStatusFinished:
                logging.info("[Scheduler.add_task] task [%s] is finished.", scheduletask.task_id)
                self.lock.release()
            elif self.schedule_task_repo.get_task_status_info_by_id(scheduletask.id) == scheduletask.kStatusFailed:
                logging.info("[Scheduler.add_task] task [%s] is failed, retry.", scheduletask.task_id)
                self.queue_push_func(scheduletask)
                scheduletask.status = scheduletask.kStatusInqueue
                self.schedule_task_repo.update_task_status(scheduletask.id, scheduletask.kStatusInqueue)
                self.lock.release()

        else:
            t = timer.at(scheduletask.crawled_next_time, self.timer_clock, scheduletask)
            logging.info("[Scheduler.add_task] task [%s] start at %s",
                         scheduletask.task_id, scheduletask.crawled_next_time.strftime("%Y-%m-%d %H:%M:%S"))
            self.task_timer_dict[scheduletask.task_id] = t

    def timer_clock(self, scheduletask: ScheduleTask):
        self.add_task(scheduletask)
        self.task_timer_dict.pop(scheduletask.task_id, 'none')

    def schedule_task(self, task_info: Task):
        logging.info("[Scheduler.schedule_task] scheduling task [%s]", task_info.id)
        scheduletask = ScheduleTask()
        scheduletask.load_task(task_info)
        self.add_task(scheduletask)
