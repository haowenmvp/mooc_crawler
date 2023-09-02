import abc


class Config(metaclass=abc.ABCMeta):

    @classmethod
    @abc.abstractmethod
    def read(cls, filename: str) -> dict:
        pass

    @classmethod
    @abc.abstractmethod
    def write(cls, filename: str, config: dict):
        pass
