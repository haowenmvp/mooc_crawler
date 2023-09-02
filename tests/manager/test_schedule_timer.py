import time
import logging
import threading
import datetime


class ScheduleTimer(threading.Thread):
    # kWaitSeconds = 10800
    kWaitSeconds = 5

    def __init__(self):
        super().__init__()
        self.setName("Schedule Timer")

    def run(self):
        logging.info("[ScheduleTimer] monitor start.")
        last_schedule_time = datetime.datetime.now() + datetime.timedelta(minutes=-1)
        while True:
            now = datetime.datetime.now()

            if (now - last_schedule_time).seconds >= self.kWaitSeconds:
                print("[ScheduleTimer] ScheduleTimer scheduled at", now.strftime("%Y-%m-%d %H:%M:%S"))

                # load all incomplete task and schedule them
                # incomplete_task_info_list = self.server.task_info_repo.get_incomplete_task_info_list()
                # for task_info in incomplete_task_info_list:
                #     self.server.scheduler.schedule_task(task_info)
                last_schedule_time = now
            else:
                pass
            time.sleep(self.kWaitSeconds)


timer = ScheduleTimer()
timer.start()
