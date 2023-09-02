import pickle
from persistence.model.task import ScheduleTask
from message_queue.base_queue import BaseProducer
import pika
import logging
import traceback


class Producer(BaseProducer):
    def __init__(self,
                 addr,
                 port,
                 user,
                 passwd,
                 queue_name,
                 auto_ack=False):
        super().__init__(addr, port, user, passwd)
        self.queue_name = queue_name
        self.auto_ack = auto_ack
        self.user_pwd = pika.PlainCredentials(self.user, self.passwd)
        self.connection = self.open_connection()
        self.open_connection()

    def open_connection(self):
        logging.info('[Producer.open_connection] opening connection.')
        return pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.remote_addr,
                port=self.remote_port,
                credentials=self.user_pwd, heartbeat=0, socket_timeout=None, blocked_connection_timeout=None))

    def publish(self, task: ScheduleTask):
        if self.connection.is_closed:
            logging.error("[Producer] connection closed.")
            self.connection = self.open_connection()
        try:
            channel = self.connection.channel()
        except Exception as e:
            logging.error("[Producer] connection err: [%s]", e)
            logging.error("[Producer] %s", traceback.format_exc())
            logging.info("[Producer] connection reopening.")
            self.connection = self.open_connection()
            logging.info("[Producer] reopen connection: %s.", self.connection.is_open)
            channel = self.connection.channel()
            logging.info("[Producer] reopen channel: %s", channel.is_open)

        channel.queue_declare(queue=self.queue_name, durable=True)
        task_info = pickle.dumps(task)
        channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body=task_info,
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
        print(" [x] Sent Task:%r" % task_info)
        logging.info("Task sent. [%s]", task.task_id)
        channel.close()

    def close(self):
        self.connection.close()
