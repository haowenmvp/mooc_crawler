from .base_pipeline import BasePipeline


class PrintPipeline(BasePipeline):
    def processing(self, task_id: str, data):
        print(data)
