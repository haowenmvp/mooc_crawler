import logging
import traceback
import pickle
from rpc.http import JsonRPCHandler


class PipelineHandler(JsonRPCHandler):
    def put_data_handler(self, pipeline_module: str, pipeline_args: dict, schedule_task_id: str, task_id: str, data):
        logging.info('[put_data_handler] start handle')
        try:
            pipeline = self.server.get_pipeline(pipeline_module, pipeline_args)
        except Exception as e:
            logging.error('[put_data_handler] failed handle. err: %s', e)
            logging.error('[put_data_handler] %s', traceback.format_exc())
            return self.build_err("Get pipeline failed. err: %s", e)
        logging.info("[put_data_handler] loaded pipeline: [%s: %s]", pipeline_module, str(pipeline))
        pipeline.mgr_rpc_client.report_start_pipeline(task_id)
        try:
            if type(data) == str:
                pipeline.process(task_id, data)
            # 列表爬虫返回课程dict列表
            elif type(data) == list:
                pipeline.process_course_list_info(data)
                pipeline.after_process(task_id, data)
            elif type(data) == dict:
                pipeline.process(task_id, data)
                with open(str(task_id) + ".pkl", 'wb') as f:
                    pickle.dump(data, f)
        except Exception as e:
            with open(str(task_id) + ".pkl", 'wb') as f:
                pickle.dump(data, f)
            logging.error('[put_data_handler] failed handle. err: %s', e)
            logging.error('[put_data_handler] %s', traceback.format_exc())
            return self.build_err("Process failed. err: %s", e)
        return self.build_res()
