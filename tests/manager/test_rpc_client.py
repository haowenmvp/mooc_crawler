import unittest
from unittest.mock import patch

from manager.rpc_client import ManagerClient


class ManagerClientTestCase(unittest.TestCase):
    def test_get_platform_config_list(self):
        resp = [{'name': 'icourse163',
                 'domain': 'www.icourse163.org',
                 'fetchers': {
                     'forum': {
                         "module": "crawler.fetcher.icourse163.ForumFetcher",
                         "pipeline": {
                             "module": "pipeline.pipeline.db_saver",
                             "pipeline_args": {}
                         }
                     },
                     'video': {
                         "module": "crawler.fetcher.icourse163.VideoFetcher",
                         "pipeline": {
                             "module": "pipeline.pipeline.db_saver",
                             "pipeline_args": {}
                         }
                     }
                 }
                 },
                {'name': 'xueyinonline',
                 'domain': 'www.xueyinonline.org',
                 'fetchers': {}
                 }]
        with patch('manager.rpc_client.ManagerClient.call', return_value=resp):
            client = ManagerClient('http://localhost:8008/rpc/manager')
            configs = client.get_platform_config_list()

            assert len(configs) == 2
            assert configs[0].name == 'icourse163'
            assert configs[0].fetchers['forum']['module'] == 'crawler.fetcher.icourse163.ForumFetcher'
            assert len(configs[1].fetchers) == 0

    def test_register(self):
        resp = 12345

        with patch('manager.rpc_client.ManagerClient.call', return_value=resp):
            client = ManagerClient('http://localhost:8008/rpc/manager')
            res = client.register()

            assert res == 12345


if __name__ == '__main__':
    unittest.main()
