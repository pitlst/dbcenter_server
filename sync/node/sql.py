import pandas as pd
import sqlalchemy as sqla
from general.base import node_base  # 用于类型标注
from general.connect import db_engine

class sql_node(node_base):
    '''
    数据库sql同步
    '''
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data = None
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get(self.source["connect"])
        self.target_connect = self.temp_db.get(self.target["connect"])
        if self.type == "sql_sync":
            with open(self.source["sql"], 'r', encoding='utf8') as file:
                # 确保输入没有参数匹配全是字符串
                self.source_sql = sqla.text(file.read())
            self.target_name = self.target["table"]
        elif self.type == "tabel_sync":
            self.source_name = self.source["table"]
            self.target_name = self.target["table"]

    def read(self) -> list[int]:
        if self.type == "sql_sync":
            self.LOG.info("正在执行sql:" + self.source["sql"])
            self.data = pd.read_sql_query(self.source_sql, self.source_connect)
        elif self.type == "tabel_sync":
            self.LOG.info("正在执行表同步，表名为:" + self.source_name)
            self.data = pd.read_sql_table(self.source_name, self.source_connect)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))
        return self.get_data_size(self.data)

    def write(self) -> None:
        self.LOG.info("正在写入表:" + self.target_name)
        self.data.to_sql(name=self.target_name, con=self.target_connect,index=False, if_exists='replace', chunksize=1000)
        
    def release(self) -> None:
        self.data = None
        if self.type == "sql_sync":
            self.source_sql = None
            self.target_name = None
        elif self.type == "tabel_sync":
            self.source_name = None
            self.target_name = None
        self.source_connect.close()
        self.target_connect.close()
        self.source_connect = None
        self.target_connect = None

    @staticmethod
    def get_data_size(input_df: pd.DataFrame) -> int:
        '''获取dataframe的内存占用,用于计算同步时间间隔'''
        return int(input_df.values.nbytes / 1024**2)