import datetime
import logging

from general.connect import db_engine

class momgo_handler(logging.Handler):
    """
    mongo日志handler
    """
    def __init__(self, name: str) -> None:
        logging.Handler.__init__(self)
        temp_db = db_engine.get_mongo()
        self.collection = temp_db["logger"][name]
 
    def emit(self, record) -> None:
        """
        发出记录(Emit a record)
        """
        try:
            msg = self.format(record)
            self.collection.insert_one({ 
                "时间": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "等级": self.trans_level_to_name(self.level),
                "消息": msg
                })
        except Exception:
            self.handleError(record)
    
    @staticmethod
    def trans_level_to_name(self_lavel: int) -> str:
        if self_lavel == 0:
            return "notset"
        elif self_lavel == 10:
            return "debug"
        elif self_lavel == 20:
            return "info"
        elif self_lavel == 30:
            return "warning"
        elif self_lavel == 40:
            return "error"
        elif self_lavel == 50:
            return "critical"
        else:
            return ""

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
    LOG.addHandler(mongoio)
    return LOG
