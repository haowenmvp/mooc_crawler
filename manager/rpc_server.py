import time
import logging
import threading
import datetime

from manager.login_info_mgr import LoginInfoManager
from manager.handler import ManagerHandler
from manager.scheduler import Scheduler

from rpc.http import JsonRPCServer
from message_queue.mq_producer import Producer
from persistence.db.task_info_repo import TaskInfoRepository
from persistence.db.schedule_task_repo import ScheduleTaskInfoRepository
from persistence.db.course_list_info_repo import CourseListInfoRepository
from persistence.config.json_config import JsonPlatformConfig
from persistence.model.manager_config import ManagerConfig


class ManagerRPCServer(JsonRPCServer):
    def __init__(self, config: ManagerConfig):
        super().__init__(config.bind_addr, config.bind_port, config.rpc_path, ManagerHandler)
        self.config = config
        self.platform_config_list = list()

        self.login_info_mgr = LoginInfoManager(self.config.platform_login_data)
        self.course_list_info_repo = CourseListInfoRepository(**config.db_config)
        self.task_info_repo = TaskInfoRepository(**config.db_config)
        self.scheduler_task_repo = ScheduleTaskInfoRepository(**config.db_config)
        self.producer = Producer(**config.mq_config)
        self.scheduler = Scheduler(self.producer.publish, self.login_info_mgr,
                                   self.scheduler_task_repo)

        self.heartbeat_monitor = HeartbeatMonitor(self)
        self.schedule_timer = ScheduleTimer(self)
        # {"client_id": (addr, port)}
        self.client_id_addr_dict = dict()
        # {"client_id": task_id}
        self.client_id_tasks_dict = dict()
        # {"client_id": datetime}
        self.alive_id_time_dict = dict()

    def serve_forever(self, poll_interval=0.5):
        config_reader = JsonPlatformConfig()
        # Read all platform config
        for filename in self.config.platform_config_files:
            config = config_reader.read(filename)
            self.platform_config_list.append(config)

        # get login info
        # self.login_info_mgr.do_login()

        self.schedule_timer.start()
        self.heartbeat_monitor.start()
        return super().serve_forever(poll_interval)


class ScheduleTimer(threading.Thread):
    # set schedule interval = 1 day
    kWaitDays = 1
    kWaitSeconds = 86400

    def __init__(self, server: ManagerRPCServer):
        super().__init__()
        self.server = server
        self.setName("Schedule Timer")

    def run(self):
        logging.info("[ScheduleTimer] ScheduleTimer start.")
        # initial last_schedule_time earlier than now
        last_schedule_time = datetime.datetime.now() + datetime.timedelta(days=-2)
        while True:
            now = datetime.datetime.now()
            if (now - last_schedule_time).days >= self.kWaitDays:
                logging.info("[ScheduleTimer] ScheduleTimer scheduled at %s", now.strftime("%Y-%m-%d %H:%M:%S"))

                # load all incomplete task and schedule them
                incomplete_task_info_list = self.server.task_info_repo.get_incomplete_task_info_list()
                for task_info in incomplete_task_info_list:
                    self.server.scheduler.schedule_task(task_info)
                last_schedule_time = now
            else:
                pass
            time.sleep(self.kWaitSeconds)


class HeartbeatMonitor(threading.Thread):
    kTimeoutSeconds = 60 * 5
    kSleepSeconds = kTimeoutSeconds / 2

    def __init__(self, server: ManagerRPCServer):
        super().__init__()
        self.server = server
        self.setName("Heartbeat Monitor")

    def run(self):
        logging.info("[HeartbeatMonitor] monitor start.")
        while True:
            now = datetime.datetime.now()
            for client_id, last_beat_time in self.server.alive_id_time_dict.items():
                if (now - last_beat_time).seconds > self.kTimeoutSeconds:
                    logging.info("[HeartbeatMonitor] monitor found a dead client [%s]: %s.",
                                 client_id, self.server.client_id_addr_dict[client_id])
                    task_id = self.server.client_id_tasks_dict[client_id]
                    self.server.client_id_addr_dict.pop(client_id)
                    self.server.client_id_tasks_dict.pop(client_id)
                    self.server.alive_id_time_dict.pop(client_id)

                    task_info = self.server.task_info_repo.get_task_info_by_id(task_id)
                    self.server.scheduler.schedule_task(task_info)
                else:
                    pass
            time.sleep(self.kSleepSeconds)


if __name__ == '__main__':
    kConfigPath = '../config/manager.json'
    from persistence.config.json_config import JsonManagerConfig

    conf = JsonManagerConfig().read(kConfigPath)
    s = ManagerRPCServer(conf)
    s.serve_forever()
