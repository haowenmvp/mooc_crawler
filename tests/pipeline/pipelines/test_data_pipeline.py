import unittest
import datetime

from unittest.mock import patch
from pipeline.pipelines import DataPipeline

from persistence.model.pipeline_config import PipelineConfig
from persistence.model.task import Task


class DataPipelineTestCase(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        self.config = PipelineConfig()
        self.config.data_repo = {
            'host': '',
            'port': 0,
            'username': '',
            'password': '',
            'database': '',
        }
        self.config.task_info_repo = self.config.data_repo
        self.task = Task()
        self.data = [
            {
                'forum_post_id': '0',
                'forum_id': '',
                'forum_name': 'name_of_1234',
                'forum_subject': '',
                'forum_post_type': 0,
                'forum_reply_id': '',
                'forum_reply_userid': '',
                'forum_post_username': 'user 1',
                'forum_post_userrole': 0,
                'forum_post_content': 'hello',
                'forum_post_time': datetime.datetime.now(),
                'update_time': datetime.datetime.now(),
            }, {
                'forum_post_id': '1',
                'forum_id': '',
                'forum_name': 'name_of_1234',
                'forum_subject': '',
                'forum_post_type': 0,
                'forum_reply_id': '',
                'forum_reply_userid': '',
                'forum_post_username': 'user 2',
                'forum_post_userrole': 0,
                'forum_post_content': 'world',
                'forum_post_time': datetime.datetime.now(),
                'update_time': datetime.datetime.now(),
            }
        ]

    def test_process_forum_post_info_create_exist_info(self):
        with patch('persistence.db.api.hive_api.HiveApi.__init__', return_value=None), \
             patch('persistence.db.forum_basic_info_repo.ForumBasicInfoRepository.get_forum_id_by_semester_id_and_name', return_value='1234'), \
             patch('persistence.db.forum_basic_info_repo.ForumBasicInfoRepository.create_forum_basic_info') as mock_basic_create, \
                patch('persistence.db.forum_post_info_repo.ForumPostInfoRepository.create_forum_post_infos') as mock_create:
            pipeline = DataPipeline(self.config)
            pipeline.process_forum_post_info(self.task, self.data)
            info_list = mock_create.call_args_list[0].args[0]

            assert info_list[0].forum_post_id == '0'
            assert info_list[0].forum_name == 'name_of_1234'
            assert info_list[0].forum_id == '1234'
            assert info_list[1].forum_post_id == '1'
            assert info_list[1].forum_name == 'name_of_1234'
            assert info_list[1].forum_id == '1234'
            assert mock_basic_create.call_count == 0

    def test_process_forum_post_info_create_basic_info(self):
        with patch('persistence.db.api.hive_api.HiveApi.__init__', return_value=None), \
             patch('persistence.db.forum_basic_info_repo.ForumBasicInfoRepository.get_forum_id_by_semester_id_and_name', return_value=''), \
             patch('persistence.db.forum_basic_info_repo.ForumBasicInfoRepository.create_forum_basic_info') as mock_basic_create, \
             patch('persistence.db.forum_post_info_repo.ForumPostInfoRepository.create_forum_post_infos') as mock_create:
            pipeline = DataPipeline(self.config)
            pipeline.process_forum_post_info(self.task, self.data)
            info_list = mock_create.call_args_list[0].args[0]

            assert info_list[0].forum_post_id == '0'
            assert info_list[0].forum_name == 'name_of_1234'
            assert info_list[0].forum_id != ''
            assert info_list[1].forum_post_id == '1'
            assert info_list[1].forum_name == 'name_of_1234'
            assert info_list[1].forum_id != ''
            assert mock_basic_create.call_count == 2


if __name__ == '__main__':
    unittest.main()
