import os
import tqdm
import pymongo
import pandas as pd
from sqlalchemy import text
from general.base import db_process_base
from general.connect import db_engine
from general.commont import RUN_TYPE
from general.logger import node_logger

class sql_base(db_process_base):
    '''
    数据库sql的基类
    这类节点仅执行sql，主要用途为跨数据库同步，数据贯通用
    '''
    def __init__(self, name: str, next_name: list[str], source_db_name: str, target_db_name: str, source_sql: str, target_table: str) -> None:
        super().__init__(name, next_name, type_name="sql")
        self.source_db_name = source_db_name
        self.target_db_name = target_db_name
        self.source_name = source_sql
        self.target_name = target_table
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source = self.temp_db.get(self.LOG, self.source_db_name)
        self.target = self.temp_db.get(self.LOG, self.target_db_name)

    def read(self) -> None:
        self.LOG.info("正在执行sql:" + self.sql_path)
        self.data = pd.read_sql_query(self.sql, self.source)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))

    def write(self) -> None:
        self.LOG.info("正在写入表:" + self.table_name)
        self.data.to_sql(name=self.table_name, con=self.target,index=False, if_exists='replace', chunksize=1000)

    def process(self) -> None:
        self.LOG.info("sql节点无处理部分")
        
    def release(self) -> None:
        self.data = None
        self.source.close()
        self.target.close()
 



def init_task(type_name:str):
    LOG = node_logger("sql-init")
    if type_name not in RUN_TYPE:
        LOG.error("不正确的运行类型")
        raise ValueError("不正确的运行类型")
    ROOT_SQL_DIR = os.path.join(os.path.dirname(__file__), "file")
    table_name = "config_" + type_name + "_sql_task"
    results = db_engine.get(LOG, "数据仓库缓存_mongo")["dataframe"][table_name].find()
    tasks_list = []
    for ch in results:
        if ch["type"] == "common":
            tasks_list.append(sql_base(
                name=ch["name"],
                next_name=ch["next_name"],
                source_db_name=ch["source"]["connect"],
                target_db_name=ch["target"]["connect"],
                sql_path=os.path.join(ROOT_SQL_DIR, ch["source"]["sql"]),
                table_name=ch["target"]["table"]))
        elif ch["type"] == "large":
            tasks_list.append(large_sql_base(
                name=ch["name"],
                next_name=ch["next_name"],
                source_db_name=ch["source"]["connect"],
                target_db_name=ch["target"]["connect"],
                sql_path=os.path.join(ROOT_SQL_DIR, ch["source"]["sql"]),
                table_name=ch["target"]["table"]))
        else:
            LOG.error("未知的sql同步节点类型")
            raise ValueError("未知的sql同步节点类型")
    return tasks_list
