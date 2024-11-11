import requests
from sync.general.connect import database_connect
from sys import getsizeof
from sync.node.base import node_base  # 用于类型标注
from general.connect import db_engine

class heyform_node(node_base):
    '''使用api获取数据存放到数据库'''
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.allow_type = ["api_table", "api_json"]
        self.data = None
        super().__init__(node_define["name"], db_engine, node_define["type"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        if self.type == "api_table":
            self.target_connect = self.temp_db.get_sql(self.target["connect"])
        elif self.type == "api_json":
            self.target_connect = self.temp_db.get_nosql(self.target["connect"])[self.target["database"]]
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source["url"])
        
        return self.get_data_size()

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target["database"] + "的" + self.target["collection"])
        self.target_connect[self.target["collection"]].insert_many(self.data)
        
    def release(self) -> None:
        self.data = None
        self.target_connect = None

    def get_data_size(self) -> int:
        '''获取data的内存占用,用于计算同步时间间隔'''
        return int(getsizeof(self.data) / 1024**2)