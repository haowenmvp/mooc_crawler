import unittest

from unittest.mock import patch
from pipeline.rpc_client import PipelineClient


class PipelineClientTestCase(unittest.TestCase):
    def test_put_data(self):
        client = PipelineClient('http://localhost:8009/rpc/pipeline')
        with patch('pipeline.rpc_client.PipelineClient.call') as mock:
            client.put_data('pipeline.pipelines.PrintPipeline', {"arg1": "val1"}, "123", {"key": "val"})
            assert mock.call_args[1] == {'pipeline_module': 'pipeline.pipelines.PrintPipeline',
                                         'pipeline_args': {"arg1": "val1"},
                                         'task_id': '123',
                                         'data': {"key": "val"}}


if __name__ == '__main__':
    unittest.main()
