import os
import toml
import pymongo
import sqlalchemy
from general.config import CONNECT_CONFIG
from urllib.parse import quote_plus as urlquote



# 继承Exception自定义异常，用于在处理异常时筛选
class connect_error(Exception):
     def __init__(self, msg) -> None:
        self.msg = msg
    
     def __str__(self) -> str:
        return self.msg
     

class database_connect:
    # 关系型数据库连接获取
    def __init__(self) -> None:
        self.nosql_dict: dict[str, pymongo.MongoClient] = {}
        self.engine_dict: dict[str, sqlalchemy.Connection] = {}
        # 连接非关系型数据库并保存引擎
        for ch in CONNECT_CONFIG:
            temp = CONNECT_CONFIG[ch]
            if temp["type"] in ["oracle", "sqlserver", "mysql", "pgsql"]:
                pass
            elif temp["type"] == "mongo":
                uri = "mongodb://%s:%s@%s" % (temp["user"]), urlquote(temp["password"]), temp["ip"] + ":" + str(temp["port"])
                self.nosql_dict[ch] = pymongo.MongoClient(uri)
            else:
                raise connect_error("不支持的数据库类型：" + temp["type"])
        # 连接关系型数据库并保存引擎
        for ch in CONNECT_CONFIG:
            temp = CONNECT_CONFIG[ch]
            if temp["type"] == "oracle":
                connect_str = "oracle+cx_oracle://" + temp["user"] + ":" + urlquote(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/?service_name=" + temp["mode"]
                self.engine_dict[ch] = sqlalchemy.create_engine(connect_str)
            elif temp["type"] == "sqlserver":
                connect_str = "mssql+pymssql://" + temp["user"] + ":" + urlquote(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["mode"]
                self.engine_dict[ch] = sqlalchemy.create_engine(connect_str)
            elif temp["type"] == "mysql":
                connect_str = "mysql+mysqldb://" + temp["user"] + ":" + urlquote(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["mode"]
                self.engine_dict[ch] = sqlalchemy.create_engine(connect_str)
            elif temp["type"] == "pgsql":
                connect_str = "postgresql://" + temp["user"] + ":" + urlquote(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["mode"]
                self.engine_dict[ch] = sqlalchemy.create_engine(connect_str)
            elif temp["type"] == "mongo":
                pass
            else:
                raise connect_error("不支持的数据库类型：" + temp["type"])
        
    def get_sql(self, name: str) -> sqlalchemy.Connection:
        temp_connect = self.engine_dict[name]
        if not temp_connect is None:
            return temp_connect.connect()
        else:
            raise connect_error("获取了未知的连接:" + name)
    
    def get_nosql(self, name: str) -> pymongo.MongoClient:
        temp_connect = self.nosql_dict[name]
        if not temp_connect is None:
            return temp_connect.connect()
        else:
            raise connect_error("获取了未知的连接:" + name)
        

# 全局单例
db_engine = database_connect()