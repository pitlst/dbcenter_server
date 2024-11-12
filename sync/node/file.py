import json
import pandas as pd
from sys import getsizeof
from node.base import node_base
from general.connect import db_engine


class table_read_node(node_base):
    '''
    读取表格类似文件类到数据库
    '''
    allow_type = ["read_excel", "read_csv"]
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data: pd.DataFrame|None = None

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.target_connect = self.temp_db.get_sql(self.target["connect"])
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source["path"] + "的" + self.source["sheet"])
        if self.type == "read_excel":
            self.data = pd.read_excel(self.source["path"], sheet_name=self.source["sheet"], dtype=object)
        elif self.type == "read_csv":
            self.data = pd.read_csv(self.source["path"], sheet_name=self.source["sheet"], dtype=object)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))
        return self.get_data_size(self.data)

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target["table"])
        schema = self.target["schema"] if "schema" in self.target.keys() else None
        self.data.to_sql(name=self.target["table"], con=self.target_connect, schema=schema, index=False, if_exists='replace', chunksize=1000)
        
    def release(self) -> None:
        self.data = None
        self.target_connect = None
        
    def get_data_size(self) -> int:
        '''获取dataframe的内存占用,用于计算同步时间间隔'''
        return int(self.data.values.nbytes / 1024**2)
        

class json_read_node(node_base):
    '''
    读取json到数据库
    '''
    allow_type = ["write_json"]
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data = None
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.target_connect = self.temp_db.get_nosql(self.target["connect"])[self.target["database"]]
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source["path"])
        with open(self.source["path"], "r+", encoding='utf8') as file:
            self.data = json.load(file)
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
        

