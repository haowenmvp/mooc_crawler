import pika
from pika import exceptions
import pickle
import logging
import traceback
from persistence.model.task import ScheduleTask
from message_queue.base_queue import BaseConsumer
from crawler.crawler import Crawler
from persistence.config.json_config import JsonCrawlerConfig


class Consumer(BaseConsumer):
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
        self.task = ScheduleTask()
        self.crawl = None

    def open_connection(self):
        logging.info('[Consumer.open_connection] opening connection.')
        return pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.remote_addr,
                port=self.remote_port,
                credentials=self.user_pwd, heartbeat=0, socket_timeout=None, blocked_connection_timeout=None))

    def get_channel(self):
        channel = self.connection.channel()
        channel.queue_declare(queue=self.queue_name, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback)
        return channel

    def add_callback(self, on_message_callback):
        self.crawl = on_message_callback

    def callback(self, ch, method, properties, body):
        print(body)
        self.task = pickle.loads(body)
        logging.info("[Consumer.callback] Start consuming task : [%s]", self.task.__dict__)
        try:
            self.crawl(self.task)
        # except KeyboardInterrupt as e:
        #     raise e
        #     ch.basic_nack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error("[Consumer.callback] crawl failed. will not send ack. err: [%s]", e)
            logging.error('[Consumer.callback] %s', traceback.format_exc())
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consuming(self):
        if self.crawl is None:
            raise ValueError("Callback function not added")
        if self.connection.is_closed:
            logging.error("[Consumer] connection  closed.")
        channel = self.get_channel()
        print(' [*] Waiting for tasks.')

        while True:
            try:
                channel.start_consuming()
            except pika.exceptions.StreamLostError as e:
                logging.error("[Consumer] consuming err: %s", e)
                logging.error("[Consumer] %s", traceback.format_exc())
                logging.info("[Consumer] reopening")
                self.connection = self.open_connection()
                logging.info("[Consumer] connection reopened: %s", self.connection.is_open)
                channel = self.get_channel()
                logging.info("[Consumer] channel reopened: %s", channel.is_open)
            except KeyboardInterrupt as e:
                raise e
            except Exception as e:
                logging.error("[Consumer] consuming err: %s", e)
                logging.error("[Consumer] %s", traceback.format_exc())
