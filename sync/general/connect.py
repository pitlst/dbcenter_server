import pymongo
import sqlalchemy
from urllib.parse import quote_plus as urlquote

# 继承Exception自定义异常，用于在处理异常时筛选
class connect_error(Exception):
     def __init__(self, msg):
        self.msg = msg
    
     def __str__(self):
        return self.msg

# 连接存储配置信息的本地mongo数据库的url
PYMONGO_CONNECT_URL = "mongodb://localhost:27017/"

class database_connect:
    # 关系型数据库连接获取
    def __init__(self) -> None:
        self.engine_dict: dict[str, sqlalchemy.Connection|pymongo.MongoClient] = {}
        # mongo类型的连接做特殊处理
        self.mongo_db = pymongo.MongoClient(PYMONGO_CONNECT_URL)
        # 强制转list获取所有数据
        config_dict = list(self.mongo_db["my_config"]["connect"].find())
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
                raise connect_error("不支持的数据库类型：" + ch["type"])
        
    def get(self, name: str) -> sqlalchemy.Connection:
        temp_connect = self.engine_dict[name]
        if not temp_connect is None:
            return temp_connect.connect()
        else:
            raise connect_error("获取了未知的连接:" + name)
    
    def get_mongo(self) -> pymongo.MongoClient:
        return self.mongo_db

# 全局单例
db_engine = database_connect()