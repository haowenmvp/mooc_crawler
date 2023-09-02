import abc
from persistence.model.task import ScheduleTask


class BaseQueue:
    def __init__(self):
        pass

    @abc.abstractmethod
    def get(self) -> ScheduleTask:
        pass

    @abc.abstractmethod
    def put(self, task: ScheduleTask):
        pass


class BaseConsumer:
    def __init__(self, addr: str, port: int, username: str, password: str):
        self.remote_addr = addr
        self.remote_port = port
        self.user = username
        self.passwd = password

    @abc.abstractmethod
    def add_callback(self, on_message_callback):
        pass

    @abc.abstractmethod
    def start_consuming(self):
        pass


class BaseProducer:
    def __init__(self, addr: str, port: int, username: str, password: str):
        self.remote_addr = addr
        self.remote_port = port
        self.user = username
        self.passwd = password

    @abc.abstractmethod
    def publish(self, task: ScheduleTask):
        pass

    @abc.abstractmethod
    def close(self):
        pass

