import os
import json
import pandas as pd
import pymongo
import sqlalchemy as sl
import traceback
from urllib.parse import quote_plus
from general import CONNECT_CONFIG
from general.logger import LOG

SELECT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "select")
TABLE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "file")

class executer:
    '''执行器抽象，用于限制对目标系统的影响'''
    def __init__(self, name: str) -> None:
        assert name in CONNECT_CONFIG.keys(), "连接要求的名称不在数据库连接的配置文件中"
        self.name = name
        for ch in CONNECT_CONFIG:
            if ch == name:
                temp = CONNECT_CONFIG[ch]
                if temp["type"] == "oracle":
                    self.db_type = "sql"
                    connect_str = "oracle+cx_oracle://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/?service_name=" + temp["mode"]
                    self.engine = sl.create_engine(connect_str, poolclass=sl.NullPool)
                elif temp["type"] == "sqlserver":
                    self.db_type = "sql"
                    connect_str = "mssql+pyodbc://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["mode"] + "?driver=ODBC+Driver+17+for+SQL+Server"
                    self.engine = sl.create_engine(connect_str, poolclass=sl.NullPool)
                elif temp["type"] == "mysql":
                    self.db_type = "sql"
                    connect_str = "mysql+mysqldb://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["mode"]
                    self.engine = sl.create_engine(connect_str, poolclass=sl.NullPool)
                elif temp["type"] == "pgsql":
                    self.db_type = "sql"
                    connect_str = "postgresql://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["mode"]
                    self.engine = sl.create_engine(connect_str, poolclass=sl.NullPool)
                elif temp["type"] == "mongo":
                    self.db_type = "nosql"
                    if temp["user"] != "" and temp["password"] != "":
                        url = "mongodb://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"])
                    else:
                        url = "mongodb://" + temp["ip"] + ":" + str(temp["port"])
                    self.engine = pymongo.MongoClient(url)
                elif temp["type"] == "empty":
                    self.db_type = "empty"
                else:
                    raise ValueError("不支持的数据库类型：" + temp["type"])
    
    def read_from_sql(self, __sql: str) -> pd.DataFrame:
        assert self.db_type == "sql", "该执行器不支持这个操作"
        with self.engine.connect() as connection:
            try:
                return pd.read_sql_query(sl.text(__sql), connection)
            except:
                LOG.error(traceback.format_exc())
                connection.rollback()
        
    def read_from_table(self, __table_name: str, __schema: str = None) -> pd.DataFrame:
        assert self.db_type == "sql", "该执行器不支持这个操作"
        if __schema is None:
            with self.engine.connect() as connection:
                try:
                    return pd.read_sql_table(__table_name, connection)
                except:
                    LOG.error(traceback.format_exc())
                    connection.rollback()
        else:
            return self.read_from_sql("select * from " + __schema + "." + __table_name)
    
    def read_from_nosql(self, __database_name: str, __coll_name: str) -> dict:
        assert self.db_type == "nosql", "该执行器不支持这个操作"
        try:
            return self.engine[__database_name][__coll_name].find().to_list()
        except:
            LOG.error(traceback.format_exc())
        
    def read_from_json(self, __file_path: str) -> dict:
        try:
            with open(os.path.join(SELECT_PATH, __file_path), mode='r', encoding='utf-8') as file:
                return json.load(file)
        except:
            LOG.error(traceback.format_exc())
        
    def read_from_excel(self, __file_path: str, __sheet_name: str) -> pd.DataFrame:
        try:
            return pd.read_excel(os.path.join(TABLE_PATH, __file_path), sheet_name=__sheet_name, dtype=object)
        except:
            LOG.error(traceback.format_exc())
            
    def read_from_csv(self, __file_path: str) -> pd.DataFrame:
        try:
            return pd.read_csv(os.path.join(TABLE_PATH, __file_path), dtype=object)
        except:
            LOG.error(traceback.format_exc())
        
    def write_to_table(self, __data: pd.DataFrame, __table_name: str, __schema: str = None) -> None:
        assert self.db_type == "sql", "该执行器不支持这个操作"
        with self.engine.connect() as connection:
            try:
                __data.to_sql(name=__table_name, con=connection, schema=__schema, index=False, if_exists='replace', chunksize=1000)
            except:
                LOG.error(traceback.format_exc())
                connection.rollback()
                
    def write_to_nosql(self, __data: list[dict], __database_name: str, __coll_name: str, is_update: bool = False) -> None:
        assert self.db_type == "nosql", "该执行器不支持这个操作"
        try:
            if not is_update:
                self.engine[__database_name][__coll_name].drop()
            self.engine[__database_name][__coll_name].insert_many(__data)
        except:
            LOG.error(traceback.format_exc())
        
    def write_to_excel(self, __data: pd.DataFrame, __file_path: str, __sheet_name: str) -> None:
        temp_path = os.path.join(TABLE_PATH, __file_path)
        try:
            # 保留原有excel数据并追加
            with pd.ExcelWriter(temp_path) as writer:
                __data.to_excel(writer, sheet_name=__sheet_name, index=False)
        except:
            LOG.error(traceback.format_exc())
        
    def write_to_csv(self, __data: pd.DataFrame, __file_path: str) -> None:
        temp_path = os.path.join(TABLE_PATH, __file_path)
        try:
            # 保留原有excel数据并追加
            with pd.ExcelWriter(temp_path) as writer:
                __data.to_csv(writer, index=False)
        except:
            LOG.error(traceback.format_exc())
        
    def write_to_json(self, __data: dict, __file_path: str) -> None:
        try:
            with open(os.path.join(SELECT_PATH, __file_path), mode='w', encoding='utf-8') as file:
                file.write(str(__data))
        except:
            LOG.error(traceback.format_exc())
        
def make_executer() -> dict[str, executer]:
    '''执行器的工厂方法'''
    temp = {}
    for ch in CONNECT_CONFIG:
        temp[ch] = executer(ch)
    return temp

# 全局单例
EXECUTER = make_executer()