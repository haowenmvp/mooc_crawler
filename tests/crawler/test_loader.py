import unittest

from crawler.loader import FetcherLoader
from crawler.fetcher.icourse163 import ForumFetcher

from persistence.model.platform_config import PlatformConfig
from persistence.model.task import ScheduleTask


class MyTestCase(unittest.TestCase):
    def test_loader(self):
        config = PlatformConfig()
        config.name = 'icourse163'
        config.domain = 'www.icourse163.org'
        config.fetchers = {
            'forum': {
                "module": "crawler.fetcher.icourse163.ForumFetcher",
                "pipeline": {
                    "module": "pipeline.pipeline.PrintPipeline",
                    "pipeline_args": {
                    }
                }
            },
            'video': {
                "module": "crawler.fetcher.icourse163.VideoFetcher",
                "pipeline": {
                    "module": "pipeline.pipeline.db_saver",
                    "pipeline_args": {
                    }
                }
            }
        }

        task = ScheduleTask()
        task.fetcher_type = 'forum'
        task.url = 'https://www.icourse163.org/learn/HIT-1001515007'

        loader = FetcherLoader.instance()
        loader.add_platform(config)
        self.assertEqual(ForumFetcher, loader.load(task))


if __name__ == '__main__':
    unittest.main()
