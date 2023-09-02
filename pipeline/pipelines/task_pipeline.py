import logging

from .base_pipeline import BasePipeline


class TaskPipeline(BasePipeline):
    def processing(self, task_id: str, data):
        # TODO
        print(data)
