import os
import toml
import json
import pymongo
import time
from urllib.parse import quote_plus

# ------------------------------------------数据库连接配置------------------------------------------
CONNECT_CONFIG = toml.load(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "config", "connect.toml"))
# ------------------------------------------mongo数据库连接------------------------------------------
MONGO_CONNECT_CONFIG = CONNECT_CONFIG["数据处理服务存储"]
if MONGO_CONNECT_CONFIG["user"] != "" and MONGO_CONNECT_CONFIG["password"] != "":
    url = "mongodb://" + MONGO_CONNECT_CONFIG["user"] + ":" + quote_plus(MONGO_CONNECT_CONFIG["password"]) + "@" + MONGO_CONNECT_CONFIG["ip"] + ":" + str(MONGO_CONNECT_CONFIG["port"])
else:
    url = "mongodb://" + MONGO_CONNECT_CONFIG["ip"] + ":" + str(MONGO_CONNECT_CONFIG["port"])
MONGO_CLIENT = pymongo.MongoClient(url)
# ------------------------------------------任务配置------------------------------------------
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "config", "tasks.json"), "r", encoding="utf-8") as file:
    TASKS_CONFIG = json.load(file)
name_set = set()
for ch in TASKS_CONFIG:
    if ch["name"] not in name_set:
        name_set.add(ch["name"])
    else:
        raise ValueError("节点名称重复" + ch["name"])

# 日志
from general.logger import LOG 
# 执行器的全局单例
from general.executer import EXECUTER, SQL_PATH, TABLE_PATH, JS_PATH
# ------------------------------------------时间计算装饰器------------------------------------------
def log_run_time(func):
    def _log_run_time(*args, **kwargs):
        t = time.perf_counter()
        res = func(*args, **kwargs)
        t = time.perf_counter() - t
        LOG.info("计算耗时为" + str(t) + "s")
        return res
    return _log_run_time
# 节点类定义
from general.node import node
from general.pipeline import PPL