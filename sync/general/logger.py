import os
import json
import datetime
import logging

from general.connect import db_engine

class MongoLoggingHandler(logging.Handler):
    """
    自定义logging.Handler模块，自定义将日志输出到指定位置(这里是输出到mongo)
    """
    def __init__(self, name: str):
        super(MongoLoggingHandler, self).__init__()
        self.temp_db = db_engine
        self.name = name

    @staticmethod
    def on_send_success(record_metadata):
        # 如果消息成功写入Kafka，broker将返回RecordMetadata对象（包含topic，partition和offset
        print("Success: [{}] send success".format(record_metadata))

    @staticmethod
    def on_send_error(excp):
        # 如果失败broker将返回error。这时producer收到error会尝试重试发送消息几次，直到producer返回error
        print("INFO " + "send info failed, cause: {}".format(excp))

    def emit(self, record):
        """
        重写logging.Handler的emit方法
        :param record: 传入的日志信息
        :return:
        """
        # 对日志信息进行格式化
        value = self.format(record)
        # 转成json格式，注意ensure_ascii参数设置为False，否则中文乱码
        value = json.dumps(value, ensure_ascii=False).encode("utf-8")
        future = self.producer.send(topic=self.topic, value=value)
        try:
            record_metadata = future.get(timeout=10)
            self.on_send_success(record_metadata)
        except KafkaError as e:
            self.on_send_error(e)

# 节点日志
def node_logger(
                name: str, 
                path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "log"), 
                level_console:int = logging.DEBUG, 
                level_file:int = logging.DEBUG
                ) -> logging.Logger:
    
    file_path = os.path.join(path, name, datetime.datetime.now().strftime('%Y-%m-%d--%H-%M-%S') + ".log")
    dirs1 = os.path.dirname(file_path)
    if not os.path.exists(dirs1):
        os.makedirs(dirs1)

    LOG = logging.getLogger(name)
    LOG.setLevel(logging.DEBUG)

    # 设置log日志的标准输出打印
    console = logging.StreamHandler()
    console.setLevel(level_console)
    formatter = logging.Formatter('%(levelname)s: %(asctime)s ' + name + ' %(message)s')
    console.setFormatter(formatter)
    LOG.addHandler(console)
    # 设置log日志文件
    fileio = logging.FileHandler(file_path, mode='a+', encoding="utf-8", delay=True)
    fileio.setLevel(level_file)
    formatter = logging.Formatter('%(levelname)s: %(asctime)s ' + name + ' %(message)s')
    fileio.setFormatter(formatter)
    LOG.addHandler(fileio)
    return LOG
