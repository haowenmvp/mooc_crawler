import sys
import getopt
import logging
import threading

import graypy

from persistence.config.json_config import JsonCrawlerConfig, JsonManagerConfig, JsonPipelineConfig, JsonReportConfig

from message_queue.mq_consumer import Consumer
from crawler.crawler import Crawler

from manager.rpc_server import ManagerRPCServer
from pipeline.rpc_server import PipelineRPCServer
from report_excel.report import Report
from persistence.model.process_config import ProcessConfig
from data_process.processor import Processor


def init_logging(role: str):
    handler = graypy.GELFRabbitHandler('amqp://logger:logger@222.20.95.42/', exchange='log-messages',
                                       facility='mooc_' + role + '_test')

    logging.basicConfig(format="[%(threadName)s: %(thread)d] [%(levelname)s]:"
                               " %(message)s", level=logging.INFO, handlers=[handler, logging.StreamHandler(), ])


def start_crawler(config_path: str):
    init_logging('crawler')
    config = JsonCrawlerConfig().read(config_path)
    crawler = Crawler(config)
    consumer = Consumer(**config.message_queue)
    consumer.add_callback(crawler.run_once)

    if not crawler.register():
        print("*** Crawler cannot register. Exiting. ***")
        sys.exit(1)
    try:
        consumer.start_consuming()
    except KeyboardInterrupt:
        crawler.mgr_rpc_client.report_task_interrupt(consumer.task.task_id)


def start_manager(config_path: str):
    init_logging('manager')
    config = JsonManagerConfig().read(config_path)
    server = ManagerRPCServer(config)
    server.serve_forever()


def start_pipeline(config_path: str):
    init_logging('pipeline')
    config = JsonPipelineConfig().read(config_path)
    server = PipelineRPCServer(config)
    with server:
        server_thread = threading.Thread(target=server.serve_forever())
        server_thread.daemon = True
        server_thread.start()


def start_report(config_path: str, report_type: str):
    init_logging('report')
    config = JsonReportConfig().read(config_path)
    report = Report(config)
    report.run(report_type)


def start_process():
    init_logging('data_process')
    config = ProcessConfig()
    processor = Processor(config)
    processor.run()


def print_help():
    print("Usage: start.py [Options]\n"
          "\n"
          "[Options]\n"
          "  -r, --role=<Role>\t\tManager, Crawler,Report or Pipeline.\n"
          "  -c, --config=<File>\t\tConfig file.\n"
          "  -h, --help\t\t\tPrint this message.\n"
          "  -p, --param=<Parameter>\t\t\tReport type")


def main(argv):
    role = ''
    config_file = ''
    param = ''

    if not argv:
        print_help()
        sys.exit()

    try:
        opts, args = getopt.getopt(argv, "hr:c:p:", ["help", "role=", 'config=', 'param='])
    except getopt.GetoptError:
        print("Invalid arguments. Use -h to see usage.")
        sys.exit(1)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print_help()
            sys.exit()
        elif opt in ("-c", "--config"):
            config_file = arg
        elif opt in ("-r", "--role"):
            role = arg
        elif opt in ("-p", "--param"):
            param = arg

    if not role:
        print("Role is needed.")
        sys.exit(1)

    if role == 'Manager':
        print("*** Manager start ***")
        print("Using config file: ", config_file)
        start_manager(config_file)
    elif role == 'Crawler':
        print("*** Crawler start ***")
        print("Using config file: ", config_file)
        start_crawler(config_file)
    elif role == 'Pipeline':
        print("*** Pipeline start ***")
        print("Using config file: ", config_file)
        start_pipeline(config_file)
    elif role == 'Report':
        print("*** Report start ***")
        print("Using config file: ", config_file)
        start_report(config_file, param)
    elif role == 'Processor':
        print("*** Process start ***")
        start_process()
    else:
        print("Role should be one of [Manager, Crawler, Pipeline, Report, Processor]")
        sys.exit(1)


if __name__ == '__main__':
    main(sys.argv[1:])
