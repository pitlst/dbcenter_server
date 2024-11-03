import pymongo
import pandas as pd
from general.base import db_process_base
from general.connect import db_engine
from general.commont import RUN_TYPE

class excel_read(db_process_base):
    '''
    读取excel文件的的基类
    这类节点仅同步表
    '''
    def __init__(self, name: str, next_name: list[str], source_file_path:str, target_db_name:str, source_sheet: str, target_table_name:str) -> None:
        super().__init__(name, next_name, type_name="file")
        self.target_db_name = target_db_name
        self.target_table_name = target_table_name
        
        self.source_file_path = source_file_path
        self.source_sheet = source_sheet

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
        
class excel_write(db_process_base):
    '''
    写入excel文件的的基类
    这类节点仅同步表
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


def init_task(type_name:str):
    if type_name not in RUN_TYPE:
        raise ValueError("不正确的运行类型")
    table_name = "config_" + type_name + "_file_task"
    results = db_engine["数据仓库缓存_mongo"]["dataframe"][table_name].find()
    tasks_list = []
    for ch in results:
        if "path" in ch["source"]:
            tasks_list.append(excel_read(
                name=ch["name"],
                next_name=ch["next_name"],
                source_file_path=ch["source"]["path"],
                target_db_name=ch["target"]["connect"],
                source_sheet=ch["source"]["sheet"],
                target_table_name=ch["target"]["table"]))
        else:
            tasks_list.append(excel_write(
                name=ch["name"],
                next_name=ch["next_name"],
                source_db_name=ch["source"]["connect"],
                target_file_path=ch["target"]["path"],
                source_table_name=ch["source"]["table"],
                target_sheet=ch["target"]["sheet"]))
    return tasks_list