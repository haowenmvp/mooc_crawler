import unittest
from unittest.mock import patch, mock_open

from persistence.config import JsonPlatformConfig


class MyTestCase(unittest.TestCase):
    def test_read(self):
        json_s = '''
        {
          "name": "icourse163",
          "domain": "www.icourse163.org",
          "fetchers": {
            "forum": {
              "module": "crawler.fetcher.icourse163.ForumFetcher",
              "pipeline": {
                "module": "pipeline.pipeline.PrintPipeline",
                "pipeline_args": {
                }
              }
            }
          }
        }
        '''

        with patch('builtins.open', mock_open(read_data=json_s)):
            config_reader = JsonPlatformConfig()
            config = config_reader.read('icourse163.json')
            assert config.name == 'icourse163'
            assert config.domain == 'www.icourse163.org'
            assert 'forum' in config.fetchers


if __name__ == '__main__':
    unittest.main()
