import abc
import time
import json
import os
import pandas as pd
from sys import getsizeof
from openpyxl import load_workbook
from sqlalchemy import text
from general.connect import database_connect
from general.logger import node_logger
from general.connect import db_engine

# 当前服务的所有节点数的计数
__total_node_num__ = 0

SQL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "sql")


class node_base(abc.ABC):
    '''
    节点的基类
    对于该类处理，其输入输出均需要在数据库中备份
    所以这类节点的输入输出都应当是数据库和文件这类io,不提供变量模式的传递
    '''

    def __init__(self, name: str, next_name: list[str], temp_db: database_connect, type_name: str) -> None:
        global __total_node_num__
        # 每调用一次就加一
        __total_node_num__ += 1

        self.name = name
        self.next_name = next_name
        self.type = type_name
        # 检查解析的节点类型是否正确
        temp_allow = getattr(self, "allow_type", [])
        assert self.type in temp_allow, "节点的类型不正确"
        # 数据库连接
        self.temp_db = temp_db
        # 日志
        self.LOG = node_logger(self.name)
        # 运行标志位
        self.need_run = True

    def run(self) -> tuple[str, int]:
        self.LOG.info("开始计算")
        data_size = -1
        try:
            t = time.perf_counter()
            self.connect()
            data_size = self.read()
            self.process()
            self.write()
            t = time.perf_counter() - t
            self.LOG.info("计算耗时为" + str(t) + "s")
        except Exception as me:
            self.LOG.error(str(me))
        self.release()
        self.LOG.info("已释放资源")
        self.need_run = False
        self.LOG.info("计算结束")
        return self.name, data_size

    @abc.abstractmethod
    def connect(self):
        ...

    @abc.abstractmethod
    def read(self):
        ...

    @abc.abstractmethod
    def write(self):
        ...

    @abc.abstractmethod
    def release(self):
        ...

    def process(self) -> None:
        ...


def get_data_size(data) -> int:
    '''获取内存占用,用于计算同步时间间隔'''
    return int(getsizeof(data) / 1024**2)


class sql_to_table(node_base):
    allow_type = ["sql_to_table"]

    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data: pd.DataFrame | None = None

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_sql(self.source["connect"])
        self.target_connect = self.temp_db.get_sql(self.target["connect"])
        with open(os.path.join(SQL_PATH, self.source["sql"]), 'r', encoding='utf8') as file:
            # 确保输入没有参数匹配全是字符串
            self.source_sql = text(file.read())

    def read(self) -> list[int]:
        self.LOG.info("正在执行sql:" + str(os.path.join(SQL_PATH, self.source["sql"])))
        self.data = pd.read_sql_query(self.source_sql, self.source_connect)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))
        return get_data_size(self.data)

    def write(self) -> None:
        self.LOG.info("正在写入表:" + self.target["table"])
        schema = self.target["schema"] if "schema" in self.target.keys() else None
        self.data.to_sql(name=self.target["table"], con=self.target_connect, schema=schema, index=False, if_exists='replace', chunksize=1000)

    def release(self) -> None:
        self.data = None
        self.source_connect = None
        self.target_connect = None


class table_to_table(node_base):
    allow_type = ["table_to_table"]

    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data: pd.DataFrame | None = None

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_sql(self.source["connect"])
        self.target_connect = self.temp_db.get_sql(self.target["connect"])
        if "schema" in self.source.keys():
            self.source_sql = text("select * from " + self.source["schema"] + "." + self.source["table"])
        else:
            self.source_sql = text("select * from " + self.source["table"])

    def read(self) -> list[int]:
        self.LOG.info("正在执行sql:" + str(os.path.join(SQL_PATH, self.source["sql"])))
        self.data = pd.read_sql_query(self.source_sql, self.source_connect)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))
        return get_data_size()

    def write(self) -> None:
        self.LOG.info("正在写入表:" + self.target["table"])
        schema = self.target["schema"] if "schema" in self.target.keys() else None
        self.data.to_sql(name=self.target["table"], con=self.target_connect, schema=schema, index=False, if_exists='replace', chunksize=1000)

    def release(self) -> None:
        self.data = None
        self.source_connect = None
        self.target_connect = None


class sql_to_nosql(node_base):
    allow_type = ["sql_to_nosql"]

    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data: pd.DataFrame | None = None

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_sql(self.source["connect"])
        self.target_connect = self.temp_db.get_nosql(self.target["connect"])[self.target["database"]]
        with open(os.path.join(SQL_PATH, self.source["sql"]), 'r', encoding='utf8') as file:
            # 确保输入没有参数匹配全是字符串
            self.source_sql = text(file.read())

    def read(self) -> list[int]:
        self.LOG.info("正在执行sql:" + str(os.path.join(SQL_PATH, self.source["sql"])))
        self.data = pd.read_sql_query(self.source_sql, self.source_connect)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))
        return get_data_size()

    def write(self) -> None:
        self.LOG.info("正在写入表:" + self.target["table"])
        self.target_connect[self.target["table"]].insert_many(self.data.to_dict('records'))

    def release(self) -> None:
        self.data = None
        self.source_connect = None
        self.target_connect = None


class table_to_nosql(node_base):
    allow_type = ["table_to_nosql"]

    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data: pd.DataFrame | None = None

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_sql(self.source["connect"])
        self.target_connect = self.temp_db.get_nosql(self.target["connect"])[self.target["database"]]
        with open(os.path.join(SQL_PATH, self.source["sql"]), 'r', encoding='utf8') as file:
            # 确保输入没有参数匹配全是字符串
            self.source_sql = text(file.read())
        if "schema" in self.source.keys():
            self.source_sql = text("select * from " + self.source["schema"] + "." + self.source["table"])
        else:
            self.source_sql = text("select * from " + self.source["table"])

    def read(self) -> list[int]:
        self.LOG.info("正在执行sql:" + str(os.path.join(SQL_PATH, self.source["sql"])))
        self.data = pd.read_sql_query(self.source_sql, self.source_connect)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))
        return get_data_size()

    def write(self) -> None:
        self.LOG.info("正在写入表:" + self.target["table"])
        self.target_connect[self.target["table"]].insert_many(self.data.to_dict('records'))

    def release(self) -> None:
        self.data = None
        self.source_connect = None
        self.target_connect = None


class excel_to_table(node_base):
    allow_type = ["excel_to_table", "csv_to_table"]
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data: pd.DataFrame|None = None

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.target_connect = self.temp_db.get_sql(self.target["connect"])
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source["path"] + "的" + self.source["sheet"])
        if self.type == "excel_to_table":
            self.data = pd.read_excel(self.source["path"], sheet_name=self.source["sheet"], dtype=object)
        elif self.type == "csv_to_table":
            self.data = pd.read_csv(self.source["path"], dtype=object)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))
        return get_data_size(self.data)

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target["table"])
        schema = self.target["schema"] if "schema" in self.target.keys() else None
        self.data.to_sql(name=self.target["table"], con=self.target_connect, schema=schema, index=False, if_exists='replace', chunksize=1000)
        
    def release(self) -> None:
        self.data = None
        self.target_connect = None
        

class excel_to_nosql(node_base):
    allow_type = ["excel_to_nosql", "csv_to_nosql"]
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data: pd.DataFrame|None = None

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.target_connect = self.temp_db.get_nosql(self.target["connect"])[self.target["database"]]
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source["path"] + "的" + self.source["sheet"])
        if self.type == "excel_to_nosql":
            self.data = pd.read_excel(self.source["path"], sheet_name=self.source["sheet"], dtype=object)
        elif self.type == "csv_to_nosql":
            self.data = pd.read_csv(self.source["path"], dtype=object)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))
        return get_data_size(self.data)

    def write(self) -> None:
        self.LOG.info("正在写入表:" + self.target["table"])
        self.target_connect[self.target["table"]].insert_many(self.data.to_dict('records'))
        
    def release(self) -> None:
        self.data = None
        self.target_connect = None
        

class table_to_excel(node_base):
    allow_type = ["table_to_excel", "table_to_csv"]
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data: pd.DataFrame|None = None

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_sql(self.source["connect"])
        if "schema" in self.source.keys():
            self.source_sql = text("select * from " + self.source["schema"] + "." + self.source["table"])
        else:
            self.source_sql = text("select * from " + self.source["table"])
        
    def read(self) -> None:
        self.LOG.info("正在执行sql:" + str(os.path.join(SQL_PATH, self.source["sql"])))
        self.data = pd.read_sql_query(self.source_sql, self.source_connect)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))
        return get_data_size()

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target["path"])
        if self.type == "table_to_excel":
            # 保留原有excel数据并追加
            book = load_workbook(self.target["path"])
            with pd.ExcelWriter(self.target["path"]) as writer:
                writer.book = book
                self.data.to_excel(self.target["path"], sheet_name=self.target["sheet_name"], index=False)
        elif self.type == "table_to_csv":
            self.data.to_csv(self.target["path"], index=False)
            
    def release(self) -> None:
        self.data = None
        self.source_connect = None
        
        
class json_to_nosql(node_base):
    allow_type = ["json_to_nosql"]
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data = None

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.target_connect = self.temp_db.get_nosql(self.target["connect"])[self.target["database"]]
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + str(self.source["sql"]))
        with open(self.source["sql"], mode='r', encoding='utf-8') as file:
            self.data = json.load(file)
        return get_data_size(self.data)

    def write(self) -> None:
        self.LOG.info("正在写入表:" + self.target["table"])
        self.target_connect[self.target["table"]].insert_many(self.data)
        
    def release(self) -> None:
        self.data = None
        self.target_connect = None
        
        
class nosql_to_json(node_base):
    allow_type = ["nosql_to_json"]
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data = None

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_nosql(self.source["connect"])[self.source["database"]]
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source["table"])
        self.data = self.source_connect[self.source["table"]].find().to_list()
        return get_data_size(self.data)

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target["path"])
        with open(self.source["sql"], mode='w', encoding='utf-8') as file:
            file.write(str(self.data))
        
    def release(self) -> None:
        self.data = None
        self.source_connect = None
        
class nosql_to_nosql(node_base):
    allow_type = ["nosql_to_nosql"]
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data = None

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_nosql(self.source["connect"])[self.source["database"]]
        self.target_connect = self.temp_db.get_nosql(self.target["connect"])[self.target["database"]]
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source["table"])
        self.data = self.source_connect[self.source["table"]].find().to_list()
        return get_data_size(self.data)

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.source["table"])
        self.target_connect[self.target["table"]].insert_many(self.data)
        
    def release(self) -> None:
        self.data = None
        self.source_connect = None
        self.target_connect = None
        
class nosql_to_table(node_base):
    allow_type = ["nosql_to_table"]
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data = None

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_nosql(self.source["connect"])[self.source["database"]]
        self.target_connect = self.temp_db.get_sql(self.target["connect"])
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source["table"])
        temp_data = self.source_connect[self.source["table"]].find().to_list()
        self.data = pd.read_json(temp_data, orient="records")
        return get_data_size(self.data)

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target["table"])
        schema = self.target["schema"] if "schema" in self.target.keys() else None
        self.data.to_sql(name=self.target["table"], con=self.target_connect, schema=schema, index=False, if_exists='replace', chunksize=1000)
        
    def release(self) -> None:
        self.data = None
        self.source_connect = None
        self.target_connect = None