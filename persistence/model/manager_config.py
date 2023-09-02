class ManagerConfig:
    def __init__(self):
        self.bind_addr = 'localhost'
        self.bind_port = 8008
        self.rpc_path = '/rpc/manager'
        self.db_config = {}
        self.mq_config = {}
        self.platform_config_files = []
        self.platform_login_data = []
