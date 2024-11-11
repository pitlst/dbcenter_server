import datetime
import logging

from general.connect import db_engine

class momgo_handler(logging.Handler):
    """
    mongo日志handler
    """
    def __init__(self, name: str) -> None:
        logging.Handler.__init__(self)
        temp_db = db_engine.get_nosql("数据处理服务文档存储")
        self.collection = temp_db["logger"][name]
 
    def emit(self, record) -> None:
        """
        发出记录(Emit a record)
        """
        try:
            msg = self.format(record)
            temp_msg = msg.split(":")
            level = temp_msg[0]
            msg = ":".join(temp_msg[1:])
            self.collection.insert_one({ 
                "时间": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "等级": level,
                "消息": msg
                })
        except Exception:
            self.handleError(record)

def node_logger(name: str, level:int = logging.DEBUG) -> logging.Logger:
    '''获取日志记录器'''
    LOG = logging.getLogger(name)
    LOG.setLevel(logging.DEBUG)

    # 设置log日志的标准输出打印
    console = logging.StreamHandler()
    console.setLevel(level)
    formatter = logging.Formatter('%(levelname)s: %(asctime)s ' + name + ' %(message)s')
    console.setFormatter(formatter)
    LOG.addHandler(console)
    # 设置log的mongo日志输出
    mongoio = momgo_handler(name)
    mongoio.setLevel(level)
    formatter = logging.Formatter('%(levelname)s:%(message)s')
    mongoio.setFormatter(formatter)
    LOG.addHandler(mongoio)
    return LOG
