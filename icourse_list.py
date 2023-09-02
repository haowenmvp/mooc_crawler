from crawler.fetcher.icourse163.list_fetcher import ListFetcher
from persistence.model.pipeline_config import PipelineConfig

from persistence.model.basic_info import CourseListInfo

#from utils.utils import process_course_list_info
import selenium.webdriver
import selenium.common.exceptions
import pickle
import sys
import os
from persistence.db.course_list_info_repo import CourseListInfoRepository
from pipeline.pipelines.data_pipeline import DataPipeline
with open('17.pkl','rb') as f:
    course_list = pickle.load(f)
print(len(course_list['course_list_info']))
config = PipelineConfig()
config.task_info_repo = {
    "host": "192.168.232.100",
    "port": 3306,
    "username": "root",
    "password": "123qweASD!@#",
    "database": "mooc_test"
}
config.data_repo = {
    "host": "192.168.232.100",
    "port": 3306,
    "username": "root",
    "password": "123qweASD!@#",
    "database": "mooc_test"
}
test = DataPipeline(config)
test.process_course_list_info(course_list['course_list_info'])
'''
file_list = []
for dir in os.listdir('.'):
  path = os.path.join('.',dir)
  if os.path.isfile(path):
    if os.path.splitext(path)[1] == '.pkl':
      file_list.append(path)
print(file_list)
for pkl in file_list:
    with open(pkl,'rb') as f:
        data = pickle.load(f)
        print(len(data))
    process_course_list_info(data)
    print(pkl+' is done.')
# for course in data:
#     print(course)
'''
