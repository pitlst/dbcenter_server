import os
import json
import toml
import pymongo
import logging
import datetime
import colorlog
from urllib.parse import quote_plus

# ------------------------------------------节点配置------------------------------------------
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "config", "tasks.json"), "r", encoding="utf-8") as file:
    node_json = json.load(file)
name_set = set()
for ch in node_json:
    if ch["name"] not in name_set:
        name_set.add(ch["name"])
    else:
        raise ValueError("节点名称重复" + ch["name"])
# ------------------------------------------节点依赖------------------------------------------
NODE_DEPEND: dict[str, list[str]] = {}
for ch in node_json:
    NODE_DEPEND[ch["name"]] = ch["next_name"]
# ------------------------------------------数据库连接配置------------------------------------------
CONNECT_CONFIG = toml.load(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "config", "connect.toml"))["数据处理服务存储"]
if CONNECT_CONFIG["user"] != "" and CONNECT_CONFIG["password"] != "":
    url = "mongodb://" + CONNECT_CONFIG["user"] + ":" + quote_plus(CONNECT_CONFIG["password"]) + "@" + CONNECT_CONFIG["ip"] + ":" + str(CONNECT_CONFIG["port"])
else:
    url = "mongodb://" + CONNECT_CONFIG["ip"] + ":" + str(CONNECT_CONFIG["port"])
MONGO_CLIENT = pymongo.MongoClient(url)
# ------------------------------------------日志配置------------------------------------------
class momgo_handler(logging.Handler):
    def __init__(self) -> None:
        logging.Handler.__init__(self)
        database = MONGO_CLIENT["logger"]
        time_series_options = {
            "timeField": "timestamp",
            "metaField": "message"
        }
        if "scheduler" not in database.list_collection_names():
            self.collection = database.create_collection("scheduler", timeseries=time_series_options)
        else:
            self.collection = database["scheduler"]

    def emit(self, record) -> None:
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

LOG = logging.getLogger("scheduler")
LOG.setLevel(logging.DEBUG)
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
console.setFormatter(
    colorlog.ColoredFormatter(
        '%(log_color)s%(levelname)s: %(asctime)s %(message)s',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        },
        datefmt='## %Y-%m-%d %H:%M:%S'
    ))
LOG.addHandler(console)
mongoio = momgo_handler()
mongoio.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s:%(message)s')
mongoio.setFormatter(formatter)
LOG.addHandler(mongoio)

# ------------------------------------------管道配置------------------------------------------
class pipeline:
    '''管道的抽象，负责发送和维护在mongo中实现的消息队列'''
    def __init__(self) -> None:
        database = MONGO_CLIENT["public"]
        time_series_options = {
            "timeField": "timestamp",
            "metaField": "message"
        }
        coll_list = database.list_collection_names()
        if "mq_send" not in coll_list:
            self.coll_send = database.create_collection("mq_send", timeseries=time_series_options)
        else:
            self.coll_send = database["mq_send"]
        if "mq_recv" not in coll_list:
            self.coll_recv = database.create_collection("mq_recv", timeseries=time_series_options)
        else:
            self.coll_recv = database["mq_recv"]
            
    def send(self, node_name: str) -> None:
        self.coll_send.insert_one({
            "timestamp": datetime.datetime.now(),
            "message": node_name 
        })
        
    def recv(self, node_name: str) -> list:
        
        return self.coll_recv.find({"node_name": node_name}).to_list()
    
    def recv_all(self) -> list:
        return self.coll_recv.find().to_list()