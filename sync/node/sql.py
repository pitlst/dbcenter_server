import os
import pandas as pd
from sqlalchemy import text
from node.base import node_base
from general.connect import db_engine

SQL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "sql")

class sql_node(node_base):
    '''
    数据库sql同步
    '''
    allow_type = ["sql_sync", "tabel_sync"]
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data: pd.DataFrame|None = None
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_sql(self.source["connect"])
        self.target_connect = self.temp_db.get_sql(self.target["connect"])
        if self.type == "sql_sync":
            with open(os.path.join(SQL_PATH, self.source["sql"]), 'r', encoding='utf8') as file:
                # 确保输入没有参数匹配全是字符串
                self.source_sql = text(file.read())
        elif self.type == "tabel_sync":
            self.source_sql = text("select * from " + self.source["schema"] + "." + self.source["table"])

    def read(self) -> list[int]:
        if self.type == "sql_sync":
            self.LOG.info("正在执行sql:" + str(os.path.join(SQL_PATH, self.source["sql"])))
        elif self.type == "tabel_sync":
            self.LOG.info("正在执行sql:" + str(self.source_sql))
        self.data = pd.read_sql_query(self.source_sql, self.source_connect)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))
        return self.get_data_size()

    def write(self) -> None:
        self.LOG.info("正在写入表:" + self.target["table"])
        schema = self.target["schema"] if "schema" in self.target.keys() else None
        self.data.to_sql(name=self.target["table"], con=self.target_connect, schema=schema, index=False, if_exists='replace', chunksize=1000)
        
    def release(self) -> None:
        self.data = None
        self.source_connect = None
        self.target_connect = None

    def get_data_size(self) -> int:
        '''获取dataframe的内存占用,用于计算同步时间间隔'''
        return int(self.data.values.nbytes / 1024**2)