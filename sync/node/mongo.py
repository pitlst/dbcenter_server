import os 
import json
import bson
from sys import getsizeof
from node.base import node_base
from general.connect import db_engine

class mongo_node(node_base):
    '''
    mongo数据库同步
    '''
    allow_type = ["mongo_sync", "js_sync"]
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data = None

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_nosql(self.source["connect"])
        self.target_connect = self.temp_db.get_nosql(self.target["connect"])
        if self.type == "mongo_sync":
            ...
        elif self.type == "js_sync":
            ...

    def read(self) -> list[int]:
        ...

    def write(self) -> None:
        ...

    def release(self) -> None:
        self.data = None
        self.source_connect = None
        self.target_connect = None

    def get_data_size(self) -> int:
        '''获取data的内存占用,用于计算同步时间间隔'''
        return int(getsizeof(self.data) / 1024**2)