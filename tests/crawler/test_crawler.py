import unittest
from unittest.mock import patch, MagicMock

from crawler import Crawler
from crawler.fetcher import icourse163

from persistence.model.platform_config import PlatformConfig
from persistence.model.crawler_config import CrawlerConfig
from persistence.model.task import ScheduleTask


class CrawlerTestCase(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        self.crawler_config = CrawlerConfig()
        self.crawler_config.manager_rpc_url = 'http://localhost:8008/rpc/manager'
        self.crawler_config.pipeline_rpc_url = 'http://localhost:8008/rpc/pipeline'

        self.mock_config_list = list()
        platform_config = PlatformConfig()
        platform_config.name = 'icourse163'
        platform_config.domain = 'www.icourse163.org'
        platform_config.fetchers = {
            'forum': {
                "module": "crawler.fetcher.icourse163.ForumFetcher",
                "pipeline": {
                    "module": "pipeline.pipelines.PrintPipeline",
                    "init_args": {
                        "out_stream": "stdout"
                    }
                }
            },
            'video': {
                "module": "crawler.fetcher.icourse163.VideoFetcher",
                "pipeline": {
                    "module": "pipeline.pipelines.PrintPipeline",
                    "pipeline_args": {
                    }
                }
            }
        }
        self.mock_config_list.append(platform_config)

    def test_run_once(self):
        task = ScheduleTask()
        task.status = ScheduleTask.kStatusWaitSchedule
        task.id = 0
        task.task_id = 0
        task.course_id = 0
        task.login_info.cookies = list()
        task.login_info.headers = dict()
        task.url = 'https://www.icourse163.org/learn/HIT-1001515007'
        task.fetcher_type = 'forum'

        mock_fetched_data = {'forum_post_info': [{"key": "val"}], 'extra': 'testing'}

        with patch('crawler.rpc_client.ManagerClient.get_platform_config_list', return_value=self.mock_config_list), \
             patch('crawler.crawler.Crawler.create_driver', return_value=MagicMock()), \
             patch('crawler.crawler.Crawler.fetch_data', return_value=mock_fetched_data) as mock_fetch_call,\
             patch('crawler.rpc_client.PipelineClient.put_data', return_value=None) as mock_put_data:
            crawler = Crawler(self.crawler_config)
            crawler.get_platform_config_from_manager()
            res = crawler.run_once(task)

            assert isinstance(mock_fetch_call.call_args[0][0], icourse163.ForumFetcher)
            assert len(res['forum_post_info']) == 1
            assert res['extra'] == 'testing'
            assert mock_put_data.call_args[0][0] == 'pipeline.pipelines.PrintPipeline'
            assert mock_put_data.call_args[0][1] == {'out_stream': 'stdout'}
            assert mock_put_data.call_args[0][2] == mock_fetched_data


if __name__ == '__main__':
    unittest.main()
