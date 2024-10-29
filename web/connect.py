import pymongo
import sqlalchemy
import logging
from urllib.parse import quote_plus as urlquote

import sqlalchemy.connectors

# 连接存储配置信息的本地mongo数据库的url
PYMONGO_CONNECT_URL = "mongodb://localhost:27017/"

class database_connect:
    # 关系型数据库连接获取
    def __init__(self) -> None:
        self.engine_dict: dict[str, sqlalchemy.Connection|pymongo.MongoClient] = {}
        # mongo类型的连接做特殊处理
        self.mongo_list = ["数据仓库缓存_mongo"]
        self.engine_dict["数据仓库缓存_mongo"] = pymongo.MongoClient(PYMONGO_CONNECT_URL)
        # 强制转list获取所有数据
        config_dict = list(self.engine_dict["数据仓库缓存_mongo"]["dataframe"]["config_connect"].find())
        # 连接关系型数据库并保存引擎
        for ch in config_dict:
            if ch["type"] == "oracle":
                connect_str = "oracle+cx_oracle://" + ch["user"] + ":" + urlquote(ch["password"]) + "@" + ch["ip"] + ":" + str(ch["port"]) + "/?service_name=" + ch["mode"]
                self.engine_dict[ch["name"]] = sqlalchemy.create_engine(connect_str)
            elif ch["type"] == "sqlserver":
                connect_str = "mssql+pymssql://" + ch["user"] + ":" + urlquote(ch["password"]) + "@" + ch["ip"] + ":" + str(ch["port"]) + "/" + ch["mode"]
                self.engine_dict[ch["name"]] = sqlalchemy.create_engine(connect_str)
            elif ch["type"] == "mysql":
                connect_str = "mysql+mysqldb://" + ch["user"] + ":" + urlquote(ch["password"]) + "@" + ch["ip"] + ":" + str(ch["port"]) + "/" + ch["mode"]
                self.engine_dict[ch["name"]] = sqlalchemy.create_engine(connect_str)
            else:
                raise Exception("不支持的数据库类型：" + ch["type"])
        
    def get(self, logger: logging.Logger, name: str) -> sqlalchemy.Connection|pymongo.MongoClient:
        if name in self.mongo_list:
            return self.engine_dict[name]
        temp_connect = self.engine_dict[name]
        if not temp_connect is None:
            return temp_connect.connect()
        else:
            logger.warning("获取了未知的连接:" + name)
            raise Exception("获取了未知的连接:" + name)

# 全局单例
db_engine = database_connect()