import time
import datetime
import unittest

from unittest.mock import patch, MagicMock
from manager.scheduler import Scheduler
from persistence.model.task import Task


class SchedulerTestCase(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        super().__init__(methodName)

        task0 = Task()
        task1 = Task()
        task2 = Task()

        task0.id = 0
        task0.crawled_next_time = datetime.datetime(2010, 1, 1)
        task1.id = 1
        task1.crawled_next_time = datetime.datetime.now() + datetime.timedelta(seconds=5)
        task2.id = 2
        task2.crawled_next_time = datetime.datetime.now() + datetime.timedelta(seconds=2)

        self.task_list = [task0, task1, task2]

    def test_schedule_task(self):
        mock_queue_func = MagicMock()
        with patch('manager.login_info_mgr.LoginInfoManager') as mock_login_info:
            scheduler = Scheduler(mock_queue_func, mock_login_info)
            for task in self.task_list:
                scheduler.schedule_task(task)
            time.sleep(3)

            assert mock_queue_func.call_args_list[0][0][0].id == 0
            assert mock_queue_func.call_args_list[1][0][0].id == 2


if __name__ == '__main__':
    unittest.main()
