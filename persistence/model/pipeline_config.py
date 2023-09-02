class PipelineConfig:
    def __init__(self):
        self.bind_addr = 'localhost'
        self.bind_port = 8009
        self.rpc_path = '/rpc/pipeline'

        self.manager_rpc_url = ''
        self.task_info_repo = {}
        self.data_repo = {}
