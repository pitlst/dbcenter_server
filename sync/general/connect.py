import os
import toml
import pymongo
import sqlalchemy
from urllib.parse import quote_plus as urlquote

CONNECT_CONFIG = toml.load(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "config", "connect.toml"))

# 继承Exception自定义异常，用于在处理异常时筛选
class connect_error(Exception):
     def __init__(self, msg) -> None:
        self.msg = msg
    
     def __str__(self) -> str:
        return self.msg
     

class database_connect:
    # 数据库连接获取
    def __init__(self) -> None:
        self.nosql_dict: dict[str, pymongo.MongoClient] = {}
        self.engine_dict: dict[str, sqlalchemy.Engine] = {}
        # 连接非关系型数据库并保存引擎
        for ch in CONNECT_CONFIG:
            temp = CONNECT_CONFIG[ch]
            if temp["type"] in ["oracle", "sqlserver", "mysql", "pgsql"]:
                pass
            elif temp["type"] == "mongo":
                if temp["user"] != "" and temp["password"] != "":
                    url = "mongodb://" + temp["user"] + ":" + urlquote(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"])
                else:
                    url = "mongodb://" + temp["ip"] + ":" + str(temp["port"])
                self.nosql_dict[ch] = pymongo.MongoClient(url)
            else:
                raise connect_error("不支持的数据库类型：" + temp["type"])
        # 连接关系型数据库并保存引擎
        for ch in CONNECT_CONFIG:
            temp = CONNECT_CONFIG[ch]
            if temp["type"] == "oracle":
                connect_str = "oracle+cx_oracle://" + temp["user"] + ":" + urlquote(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/?service_name=" + temp["mode"]
                self.engine_dict[ch] = sqlalchemy.create_engine(connect_str, pool_size=20)
            elif temp["type"] == "sqlserver":
                connect_str = "mssql+pyodbc://" + temp["user"] + ":" + urlquote(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["mode"] + "?driver=ODBC+Driver+17+for+SQL+Server"
                self.engine_dict[ch] = sqlalchemy.create_engine(connect_str, pool_size=20)
            elif temp["type"] == "mysql":
                connect_str = "mysql+mysqldb://" + temp["user"] + ":" + urlquote(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["mode"]
                self.engine_dict[ch] = sqlalchemy.create_engine(connect_str, pool_size=20)
            elif temp["type"] == "pgsql":
                connect_str = "postgresql://" + temp["user"] + ":" + urlquote(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["mode"]
                self.engine_dict[ch] = sqlalchemy.create_engine(connect_str, pool_size=20)
            elif temp["type"] == "mongo":
                pass
            else:
                raise connect_error("不支持的数据库类型：" + temp["type"])
        
    def get_sql(self, name: str) -> sqlalchemy.Connection:
        temp_engine = self.engine_dict[name]
        if not temp_engine is None:
            return temp_engine.connect()
        else:
            raise connect_error("获取了未知的连接:" + name)
    
    def get_nosql(self, name: str) -> pymongo.MongoClient:
        temp_connect = self.nosql_dict[name]
        if not temp_connect is None:
            return temp_connect
        else:
            raise connect_error("获取了未知的连接:" + name)
        
# 全局单例
db_engine = database_connect()
