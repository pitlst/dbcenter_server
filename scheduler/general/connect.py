import os
import toml
import pymongo
from urllib.parse import quote_plus

CONNECT_CONFIG = toml.load(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "config", "connect.toml"))["数据处理服务存储"]
if CONNECT_CONFIG["user"] != "" and CONNECT_CONFIG["password"] != "":
    url = "mongodb://" + CONNECT_CONFIG["user"] + ":" + quote_plus(CONNECT_CONFIG["password"]) + "@" + CONNECT_CONFIG["ip"] + ":" + str(CONNECT_CONFIG["port"])
else:
    url = "mongodb://" + CONNECT_CONFIG["ip"] + ":" + str(CONNECT_CONFIG["port"])
MONGO_CLIENT = pymongo.MongoClient(url)