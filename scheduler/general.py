import os
import json
import toml
import pymongo
import logging
import datetime
import colorlog
from urllib.parse import quote_plus

# ------------------------------------------任务配置------------------------------------------
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source", "config", "tasks.json"), "r", encoding="utf-8") as file:
    node_json = json.load(file)
name_set = set()
for ch in node_json:
    if ch["name"] not in name_set:
        name_set.add(ch["name"])
    else:
        raise ValueError("节点名称重复" + ch["name"])
# ------------------------------------------任务依赖------------------------------------------
NODE_DEPEND: dict[str, list[str]] = {}
for ch in node_json:
    NODE_DEPEND[ch["name"]] = ch["next_name"]
# ------------------------------------------数据库连接配置------------------------------------------
CONNECT_CONFIG = toml.load(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source", "config", "connect.toml"))["数据处理服务存储"]
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
            self.collection = database.create_collection("scheduler", timeseries=time_series_options, expireAfterSeconds=604800)
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
        coll_list = database.list_collection_names()
        if "mq_send" not in coll_list:
            self.coll_send = database.create_collection("mq_send")
        else:
            self.coll_send = database["mq_send"]
        if "mq_recv" not in coll_list:
            self.coll_recv = database.create_collection("mq_recv")
        else:
            self.coll_recv = database["mq_recv"]
            
    def send(self, node_name: str) -> None:
        self.coll_send.insert_one({
            "timestamp": datetime.datetime.now(),
            "node_name": node_name,
            "is_process": False
        })
        
    def clean_send_history(self, request_time: datetime.datetime) -> int:
        '''清除对应时间之前的消息'''
        return self.coll_send.delete_many({'timestamp': {'$lt': request_time}}).deleted_count
        
    def clean_recv_history(self, request_time: datetime.datetime) -> int:
        '''清除对应时间之前的消息'''
        return self.coll_recv.delete_many({'timestamp': {'$lt': request_time}}).deleted_count
    
    def recv(self, node_name: str) -> list:
        '''获取对应节点的消息'''
        temp_doc_list = self.coll_recv.find({"node_name": node_name, "is_process":False}).to_list()
        for tdoc in temp_doc_list:
            self.coll_recv.update_one({"_id":tdoc["_id"]}, {"$set": {"is_process":True}})
        return temp_doc_list

    def recv_history(self, node_name: str) -> list:
        '''获取对应节点的历史消息'''
        return self.coll_recv.find({"node_name": node_name, "is_process":True}).to_list()
    
# 全局单例
PPL = pipeline()
# ------------------------------------------上下文配置------------------------------------------
class context:
    def __init__(self) -> None:
        self.coll = MONGO_CLIENT["public"]["context"]
        self.coll.drop()
        self.tasks_status : dict[str, int] = {}
        for task in NODE_DEPEND:
            self.tasks_status[task] = 0 # 0是未运行，1是正在运行，2是已经运行完成
            self.coll.insert_one({"name":task, "status":"未运行"})
            
    def is_final(self) -> bool:
        is_final = True
        for ch in self.tasks_status:
            if self.tasks_status[ch] != 2:
                is_final = False
                break
        return is_final
        
    def get_ready_to_run(self) -> list[str]:
        temp_tasks = []
        for task in NODE_DEPEND:
            # 节点没运行的
            if self.tasks_status[task] == 0:
                # 没依赖的
                if len(NODE_DEPEND[task]) == 0:
                    temp_tasks.append(task)
                    self.tasks_status[task] = 1
                    self.coll.update_one({"name":task}, {'$set': {"status":"正在运行"}})
                else:
                    # 有依赖但是都运行完了的
                    dep_total_is_runned = True
                    for task_ in NODE_DEPEND[task]:
                        if self.tasks_status[task_] != 2:
                            dep_total_is_runned = False
                            break
                    if dep_total_is_runned:
                        temp_tasks.append(task)
                        self.tasks_status[task] = 1
                        self.coll.update_one({"name":task}, {'$set': {"status":"正在运行"}})
        return temp_tasks
    
    def get_not_finish(self) -> list[str]:
        temp_tasks = []
        for task in self.tasks_status:
            if self.tasks_status[task] == 1:
                temp_tasks.append(task)
        return temp_tasks
        
    def update(self, task_name: str) -> None:
        assert task_name in self.tasks_status.keys(), "名称不在注册的节点中"
        self.tasks_status[task_name] = 2
        self.coll.update_one({"name":task_name}, {'$set': {"status":"已经运行完成"}})
    