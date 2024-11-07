import json
import pandas as pd
from sync.node.base import node_base
from general.connect import db_engine


class table_read_node(node_base):
    '''
    读取表格类似文件类到数据库
    '''
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data = None
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.target_connect = self.temp_db.get(self.target["connect"])
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source_file_path + "的" + self.source_sheet)
        self.data = pd.read_excel(self.source_file_path, sheet_name=self.source_sheet, dtype=object)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target_table_name)
        self.data.to_sql(name=self.target_table_name, con=self.target, index=False, if_exists='replace', chunksize=1000)
        
    def process(self) -> None:
        self.LOG.info("file节点无处理部分")
        
    def release(self) -> None:
        self.data = None
        self.target.close()
        
class table_write_node(node_base):
    '''
    将数据库表输出成文件
    '''
    def __init__(self, name: str, next_name: list[str], source_db_name:str, target_file_path:str, source_table_name: str, target_sheet:str) -> None:
        super().__init__(name, next_name, type_name="file")
        self.source_db_name = source_db_name
        self.source_table_name = source_table_name
        
        self.target_file_path = target_file_path
        self.target_sheet = target_sheet

        self.data = None
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source = self.temp_db.get(self.LOG, self.source_db_name)
        
    def read(self) -> None:
        self.LOG.info("正在获取表:" + self.source_table_name)
        self.data = pd.read_sql_table(self.source_table_name, con=self.source)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target_file_path + "  sheet为:" + self.target_sheet)
        writer = pd.ExcelWriter(self.target_file_path, engine="openpyxl")
        self.data.to_excel(writer, sheet_name=self.target_sheet, index=False)
        writer.close()
        
    def process(self) -> None:
        self.LOG.info("file节点无处理部分")
        
    def release(self) -> None:
        self.data = None
        self.source.close()

class json_read_node(node_base):
    '''
    读取json文件到数据库
    '''
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], type_name="file")

        self.data = None
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.target = self.temp_db.get(self.LOG, self.target_db_name)
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source_file_path + "的" + self.source_sheet)
        self.data = pd.read_excel(self.source_file_path, sheet_name=self.source_sheet, dtype=object)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target_table_name)
        self.data.to_sql(name=self.target_table_name, con=self.target, index=False, if_exists='replace', chunksize=1000)
        
    def process(self) -> None:
        self.LOG.info("file节点无处理部分")
        
    def release(self) -> None:
        self.data = None
        self.target.close()
        
class json_write_node(node_base):
    '''
    将json输出成文件
    '''
    def __init__(self, name: str, next_name: list[str], source_db_name:str, target_file_path:str, source_table_name: str, target_sheet:str) -> None:
        super().__init__(name, next_name, type_name="file")
        self.source_db_name = source_db_name
        self.source_table_name = source_table_name
        
        self.target_file_path = target_file_path
        self.target_sheet = target_sheet

        self.data = None
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source = self.temp_db.get(self.LOG, self.source_db_name)
        
    def read(self) -> None:
        self.LOG.info("正在获取表:" + self.source_table_name)
        self.data = pd.read_sql_table(self.source_table_name, con=self.source)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target_file_path + "  sheet为:" + self.target_sheet)
        writer = pd.ExcelWriter(self.target_file_path, engine="openpyxl")
        self.data.to_excel(writer, sheet_name=self.target_sheet, index=False)
        writer.close()
        
    def process(self) -> None:
        self.LOG.info("file节点无处理部分")
        
    def release(self) -> None:
        self.data = None
        self.source.close()
