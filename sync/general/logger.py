import datetime
import logging
import colorlog
from general.connect import db_engine
            
class momgo_handler(logging.Handler):
    """
    mongo日志handler
    """
    def __init__(self, name: str) -> None:
        logging.Handler.__init__(self)
        database = db_engine.get_nosql("数据处理服务存储")["logger"]
        time_series_options = {
            "timeField": "timestamp",
            "metaField":"message"
        }
        if name not in database.list_collection_names():
            self.collection = database.create_collection(name, timeseries=time_series_options)
        else:
            self.collection = database[name]
 
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
                "timestamp": datetime.datetime.now(),
                "message": {                
                    "等级": level,
                    "消息": msg
                    }
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
    # 定义颜色输出格式
    color_formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(levelname)s: %(asctime)s ' + name + ' %(message)s',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        },
        datefmt='## %Y-%m-%d %H:%M:%S'
    )
    console.setFormatter(color_formatter)
    LOG.addHandler(console)
    # 设置log的mongo日志输出
    mongoio = momgo_handler(name)
    mongoio.setLevel(level)
    formatter = logging.Formatter('%(levelname)s:%(message)s')
    mongoio.setFormatter(formatter)
    LOG.addHandler(mongoio)
    return LOG
