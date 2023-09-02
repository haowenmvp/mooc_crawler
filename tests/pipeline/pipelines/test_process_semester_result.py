import json

from pipeline.pipelines.data_pipeline import DataPipeline
from persistence.model.task import Task
import uuid
from datetime import datetime
from persistence.model.pipeline_config import PipelineConfig
from persistence.db.task_info_repo import TaskInfoRepository

data = {}

data['semester_result_info'] = {'update_time': datetime(2019, 11, 22, 16, 43, 48, 201053),
                                        'semester_resource_info': '',
                                        'semester_homework_info': '',
                                        'semester_interact_info': '',
                                        'semester_exam_info': '',
                                        'semester_extension_info': '',
                                        'semester_teacherteam_info': '{"course_director_name": "王铂", "teacher_team": ["王铂","sdn"]}',
                                        'semester_start_date': datetime(2019, 9, 1, 0, 0),
                                        'semester_end_date': datetime(2020, 3, 1, 0, 0),
                                        'semester_studentnum': 32,
                                        'semester_test_info': '{"test_total_times": 1, "test_total_questions": 30}'}

# conn = TaskInfoRepository(host='222.20.95.42', port=3307, username='root', password='123qweASD!@#',
#                                database='mooc_test')
pipeline_config = PipelineConfig()
pipeline_config.data_repo = {
    'host': '222.20.95.42',
    'port': 3307,
    'username': 'root',
    'password': '123qweASD!@#',
    'database': 'mooc_test', }
pipeline_config.task_info_repo = pipeline_config.data_repo
task = Task()
task.id = str(uuid.uuid1())
task.course_id = str(uuid.uuid1())
task.course_name = 'C++'
task.school = 'HUST'
# conn.create_task_info(task)
data_pipeline = DataPipeline(pipeline_config)
data_pipeline.process_semester_result_info(task, data)
