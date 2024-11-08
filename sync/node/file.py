import json
import pymongo
import pandas as pd
import pymongo.collection
from sync.node.base import node_base
from general.connect import db_engine


class table_read_node(node_base):
    '''
    读取表格类似文件类到数据库
    '''
    def __init__(self, node_define: dict) -> None:
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.allow_type = ["read_excel", "read_csv"]
        self.data: pd.DataFrame|None = None
        super().__init__(node_define["name"], db_engine, node_define["type"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.target_connect = self.temp_db.get_sql(self.target["connect"])
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source["path"] + "的" + self.source["sheet"])
        if self.type == "read_excel":
            self.data = pd.read_excel(self.source["path"], sheet_name=self.source["sheet"], dtype=object)
        elif self.type == "read_csv":
            self.data = pd.read_excel(self.source["path"], sheet_name=self.source["sheet"], dtype=object)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target["table"])
        self.data.to_sql(name=self.target["table"], con=self.target_connect, index=False, if_exists='replace', chunksize=1000)
        
    def release(self) -> None:
        self.data = None
        self.target_connect.close()
        

class table_write_node(node_base):
    '''
    将数据库表格写到本地
    '''
    def __init__(self, node_define: dict) -> None:
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.allow_type = ["write_excel", "write_csv"]
        self.data: pd.DataFrame|None = None
        super().__init__(node_define["name"], db_engine, node_define["type"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_sql(self.source["connect"])
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source["table"])
        self.data = pd.read_sql_table(name=self.target["table"], con=self.source_connect, chunksize=1000)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target["path"] + "的" + self.target["sheet"])
        if self.type == "write_excel":
            self.data.to_excel(self.target["path"], self.target["sheet"], index=False)
        elif self.type == "write_csv":
            self.data.to_csv(self.target["path"], index=False)
        
    def release(self) -> None:
        self.data = None
        self.source_connect.close()


class json_read_node(node_base):
    '''
    读取json类似文件类到数据库
    '''
    def __init__(self, node_define: dict) -> None:
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.allow_type = ["read_json"]
        self.data: pymongo.collection.Collection = None
        super().__init__(node_define["name"], db_engine, node_define["type"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.target_connect = self.temp_db.get_nosql(self.target["connect"])
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source["database"] + "的" + self.source["collection"])
        self.data = self.target_connect[self.source["database"]][self.source["collection"]]
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target["table"])
        self.data.to_sql(name=self.target["table"], con=self.target_connect, index=False, if_exists='replace', chunksize=1000)
        
    def release(self) -> None:
        self.data = None
        self.target_connect.close()
        

class json_write_node(node_base):
    '''
    将数据库表格写到本地
    '''
    def __init__(self, node_define: dict) -> None:
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data: pd.DataFrame|None = None
        super().__init__(node_define["name"], db_engine, node_define["type"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_sql(self.source["connect"])
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source["table"])
        self.data = pd.read_sql_table(name=self.target["table"], con=self.source_connect, chunksize=1000)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target["path"] + "的" + self.target["sheet"])
        self.data.to_excel(self.target["path"], self.target["sheet"], index=False)
        
    def release(self) -> None:
        self.data = None
        self.source_connect.close()
