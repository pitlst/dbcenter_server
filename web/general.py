import pymongo
import datetime
import logging
import sqlalchemy
import pandas as pd
from io import BytesIO
from urllib.parse import quote_plus as urlquote

# mongo
uri = "mongodb://%s:%s@%s" % (urlquote("cheakf"), urlquote("Swq8855830."), "localhost:27017")
mongo_client = pymongo.MongoClient(uri)
# mysql
connect_str = "mysql+mysqldb://" + "root" + ":" + urlquote("123456") + "@" + "localhost" + ":" + "3306" + "/" + "dataframe_web"
mysql_client = sqlalchemy.create_engine(connect_str)


class momgo_handler(logging.Handler):
    """
    mongo日志handler
    """

    def __init__(self, input_mongo_client: pymongo.MongoClient, name: str):
        logging.Handler.__init__(self)
        self.collection = input_mongo_client["logger"][name]

    def emit(self, record):
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


def node_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    '''获取日志记录器'''
    LOG = logging.getLogger(name)
    LOG.setLevel(logging.DEBUG)

    # 设置log日志的标准输出打印
    console = logging.StreamHandler()
    console.setLevel(level)
    formatter = logging.Formatter(
        '%(levelname)s: %(asctime)s ' + name + ' %(message)s')
    console.setFormatter(formatter)
    LOG.addHandler(console)
    # 设置log的mongo日志输出
    mongoio = momgo_handler(mongo_client, name)
    mongoio.setLevel(level)
    LOG.addHandler(mongoio)
    return LOG


def get_file(path: str) -> BytesIO:
    # 将文件转换为字节流
    df = pd.read_excel(path, sheet_name=None, header=0, index_col=0)
    excel_keys = list(df.keys())
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='openpyxl')
    for k in range(len(excel_keys)):
        df = pd.read_excel(
            path, sheet_name=excel_keys[k], header=0, index_col=0)
        df.to_excel(writer, sheet_name=excel_keys[k])
    writer.close()
    return output
