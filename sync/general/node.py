import abc
import time
import json
import os
import traceback
import pandas as pd
# 这个不能删
import openpyxl
from sys import getsizeof
from sqlalchemy import text
from general.connect import database_connect
from general.logger import node_logger
from general.connect import db_engine

SQL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "sql")
TABLE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "table")
JS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "mongo_js")


class node_base(abc.ABC):
    '''
    节点的基类
    对于该类处理，其输入输出均需要在数据库中备份
    所以这类节点的输入输出都应当是数据库和文件这类io,不提供变量模式的传递
    '''

    def __init__(self, name: str, next_name: list[str], temp_db: database_connect, type_name: str) -> None:
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

    def run(self) -> tuple[str, int]:
        self.LOG.info("开始计算")
        data_size = -1
        try:
            t = time.perf_counter()
            self.connect()
            data_size = self.read()
            self.write()
            t = time.perf_counter() - t
            self.LOG.info("计算耗时为" + str(t) + "s")
            self.release()
            self.LOG.info("已释放资源")
        except Exception as me:
            self.LOG.error(traceback.format_exc())
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


def get_data_size(data) -> int:
    '''获取内存占用,用于计算同步时间间隔'''
    return int(getsizeof(data) / 1024 **2)

class sql_to_table(node_base):
    allow_type = ["sql_to_table"]

    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data: pd.DataFrame | None = None
        # 检查节点格式是否符合要求
        source_key = self.source.keys()
        assert "connect" in source_key, "节点的格式不符合要求：source中没有connect"
        assert "sql" in source_key, "节点的格式不符合要求：source中没有sql"
        target_key = self.target.keys()
        assert "connect" in target_key, "节点的格式不符合要求：target中没有connect"
        assert "table" in target_key, "节点的格式不符合要求：target中没有table"

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
        return self.data.shape[0] * self.data.shape[1]

    def write(self) -> None:
        self.LOG.info("正在写入表:" + self.target["table"])
        schema = self.target["schema"] if "schema" in self.target.keys() else None
        self.data.to_sql(name=self.target["table"], con=self.target_connect, schema=schema, index=False, if_exists='replace', chunksize=1000)

    def release(self) -> None:
        self.data = None
        self.source_connect.close()
        self.target_connect.close()
        self.source_connect = None
        self.target_connect = None


class table_to_table(node_base):
    allow_type = ["table_to_table"]

    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data: pd.DataFrame | None = None
        # 检查节点格式是否符合要求
        source_key = self.source.keys()
        assert "connect" in source_key, "节点的格式不符合要求：source中没有connect"
        assert "table" in source_key, "节点的格式不符合要求：source中没有table"
        target_key = self.target.keys()
        assert "connect" in target_key, "节点的格式不符合要求：target中没有connect"
        assert "table" in target_key, "节点的格式不符合要求：target中没有table"

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_sql(self.source["connect"])
        self.target_connect = self.temp_db.get_sql(self.target["connect"])
        if "schema" in self.source.keys():
            self.source_sql = text("select * from " + self.source["schema"] + "." + self.source["table"])
        else:
            self.source_sql = text("select * from " + self.source["table"])

    def read(self) -> list[int]:
        self.LOG.info("正在执行sql:" + str(self.source_sql))
        self.data = pd.read_sql_query(self.source_sql, self.source_connect)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))
        return self.data.shape[0] * self.data.shape[1]

    def write(self) -> None:
        self.LOG.info("正在写入表:" + self.target["table"])
        schema = self.target["schema"] if "schema" in self.target.keys() else None
        self.data.to_sql(name=self.target["table"], con=self.target_connect, schema=schema, index=False, if_exists='replace', chunksize=1000)

    def release(self) -> None:
        self.data = None
        self.source_connect.close()
        self.target_connect.close()
        self.source_connect = None
        self.target_connect = None


class sql_to_nosql(node_base):
    allow_type = ["sql_to_nosql"]

    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data: pd.DataFrame | None = None
        # 检查节点格式是否符合要求
        source_key = self.source.keys()
        assert "connect" in source_key, "节点的格式不符合要求：source中没有connect"
        assert "sql" in source_key, "节点的格式不符合要求：source中没有sql"
        target_key = self.target.keys()
        assert "connect" in target_key, "节点的格式不符合要求：target中没有connect"
        assert "database" in target_key, "节点的格式不符合要求：target中没有database"
        assert "table" in target_key, "节点的格式不符合要求：target中没有table"

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
        for col_name in self.data.columns:
            self.data[[col_name]] = self.data[[col_name]].astype(object).where(self.data[[col_name]].notnull(), None)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))
        return self.data.shape[0] * self.data.shape[1]

    def write(self) -> None:
        self.LOG.info("正在清空表:" + self.target["table"])
        self.target_connect[self.target["table"]].drop()
        self.LOG.info("正在写入表:" + self.target["table"])
        self.data = self.data.to_dict('records')
        self.target_connect[self.target["table"]].insert_many(self.data)

    def release(self) -> None:
        self.data = None
        self.source_connect.close()
        self.source_connect = None
        self.target_connect = None


class table_to_nosql(node_base):
    allow_type = ["table_to_nosql"]

    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data: pd.DataFrame | None = None
        # 检查节点格式是否符合要求
        source_key = self.source.keys()
        assert "connect" in source_key, "节点的格式不符合要求：source中没有connect"
        assert "table" in source_key, "节点的格式不符合要求：source中没有table"
        target_key = self.target.keys()
        assert "connect" in target_key, "节点的格式不符合要求：target中没有connect"
        assert "database" in target_key, "节点的格式不符合要求：target中没有database"
        assert "table" in target_key, "节点的格式不符合要求：target中没有table"

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_sql(self.source["connect"])
        self.target_connect = self.temp_db.get_nosql(self.target["connect"])[self.target["database"]]
        if "schema" in self.source.keys():
            self.source_sql = text("select * from " + self.source["schema"] + "." + self.source["table"])
        else:
            self.source_sql = text("select * from " + self.source["table"])

    def read(self) -> list[int]:
        self.LOG.info("正在执行sql:" + str(self.source_sql))
        self.data = pd.read_sql_query(self.source_sql, self.source_connect)
        for col_name in self.data.columns:
            self.data[[col_name]] = self.data[[col_name]].astype(object).where(self.data[[col_name]].notnull(), None)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))
        return self.data.shape[0] * self.data.shape[1]

    def write(self) -> None:
        self.LOG.info("正在清空表:" + self.target["table"])
        self.target_connect[self.target["table"]].drop()
        self.LOG.info("正在写入表:" + self.target["table"])
        self.data = self.data.to_dict('records')
        self.target_connect[self.target["table"]].insert_many(self.data)

    def release(self) -> None:
        self.data = None
        self.source_connect.close()
        self.source_connect = None
        self.target_connect = None


class excel_to_table(node_base):
    allow_type = ["excel_to_table", "csv_to_table"]
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data: pd.DataFrame|None = None
        # 检查节点格式是否符合要求
        source_key = self.source.keys()
        assert "path" in source_key, "节点的格式不符合要求：source中没有path"
        target_key = self.target.keys()
        assert "connect" in target_key, "节点的格式不符合要求：target中没有connect"
        assert "table" in target_key, "节点的格式不符合要求：target中没有table"

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.target_connect = self.temp_db.get_sql(self.target["connect"])
        
    def read(self) -> None:
        if self.type == "excel_to_table":
            self.LOG.info("正在获取:" + self.source["path"] + "的" + self.source["sheet"])
            self.data = pd.read_excel(os.path.join(TABLE_PATH, self.source["path"]), sheet_name=self.source["sheet"], dtype=object)
        elif self.type == "csv_to_table":
            self.LOG.info("正在获取:" + self.source["path"])
            self.data = pd.read_csv(os.path.join(TABLE_PATH, self.source["path"]), dtype=object)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))
        return self.data.shape[0] * self.data.shape[1]

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target["table"])
        schema = self.target["schema"] if "schema" in self.target.keys() else None
        self.data.to_sql(name=self.target["table"], con=self.target_connect, schema=schema, index=False, if_exists='replace', chunksize=1000)
        
    def release(self) -> None:
        self.data = None
        self.target_connect.close()
        self.target_connect = None
        

class excel_to_nosql(node_base):
    allow_type = ["excel_to_nosql", "csv_to_nosql"]
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data: pd.DataFrame|None = None
        # 检查节点格式是否符合要求
        source_key = self.source.keys()
        assert "path" in source_key, "节点的格式不符合要求：source中没有path"
        target_key = self.target.keys()
        assert "connect" in target_key, "节点的格式不符合要求：target中没有connect"
        assert "database" in target_key, "节点的格式不符合要求：target中没有database"
        assert "table" in target_key, "节点的格式不符合要求：target中没有table"

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.target_connect = self.temp_db.get_nosql(self.target["connect"])[self.target["database"]]
        
    def read(self) -> None:
        if self.type == "excel_to_nosql":
            self.LOG.info("正在获取:" + self.source["path"] + "的" + self.source["sheet"])
            self.data = pd.read_excel(os.path.join(TABLE_PATH, self.source["path"]), sheet_name=self.source["sheet"], dtype=object)
        elif self.type == "csv_to_nosql":
            self.LOG.info("正在获取:" + self.source["path"])
            self.data = pd.read_csv(os.path.join(TABLE_PATH, self.source["path"]), dtype=object)
        for col_name in self.data.columns:
            self.data[[col_name]] = self.data[[col_name]].astype(object).where(self.data[[col_name]].notnull(), None)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))
        return self.data.shape[0] * self.data.shape[1]

    def write(self) -> None:
        self.LOG.info("正在清空表:" + self.target["table"])
        self.target_connect[self.target["table"]].drop()
        self.LOG.info("正在写入表:" + self.target["table"])
        self.data = self.data.to_dict('records')
        self.target_connect[self.target["table"]].insert_many(self.data)
        
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
        # 检查节点格式是否符合要求
        source_key = self.source.keys()
        assert "connect" in source_key, "节点的格式不符合要求：source中没有connect"
        assert "table" in source_key, "节点的格式不符合要求：source中没有table"
        target_key = self.target.keys()
        assert "path" in target_key, "节点的格式不符合要求：target中没有path"

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_sql(self.source["connect"])
        if "schema" in self.source.keys():
            self.source_sql = text("select * from " + self.source["schema"] + "." + self.source["table"])
        else:
            self.source_sql = text("select * from " + self.source["table"])
        
    def read(self) -> None:
        self.LOG.info("正在执行sql:" + str(self.source_sql))
        self.data = pd.read_sql_query(self.source_sql, self.source_connect)
        self.LOG.info("数据形状为: " + str(self.data.shape[0]) + "," + str(self.data.shape[1]))
        return self.data.shape[0] * self.data.shape[1]

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target["path"])
        temp_path = os.path.join(TABLE_PATH, self.target["path"])
        if self.type == "table_to_excel":
            # 保留原有excel数据并追加
            with pd.ExcelWriter(temp_path) as writer:
                self.data.to_excel(writer, sheet_name=self.target["sheet"], index=False)
        elif self.type == "table_to_csv":
            self.data.to_csv(temp_path, index=False)
            
    def release(self) -> None:
        self.data = None
        self.source_connect.close()
        self.source_connect = None
        
        
class json_to_nosql(node_base):
    allow_type = ["json_to_nosql"]
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], node_define["next_name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data = None
        # 检查节点格式是否符合要求
        source_key = self.source.keys()
        assert "path" in source_key, "节点的格式不符合要求：source中没有path"
        target_key = self.target.keys()
        assert "connect" in target_key, "节点的格式不符合要求：target中没有connect"
        assert "database" in target_key, "节点的格式不符合要求：target中没有database"
        assert "table" in target_key, "节点的格式不符合要求：target中没有table"

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.target_connect = self.temp_db.get_nosql(self.target["connect"])[self.target["database"]]
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + os.path.join(JS_PATH, self.source["path"]))
        with open(os.path.join(JS_PATH, self.source["path"]), mode='r', encoding='utf-8') as file:
            self.data = json.load(file)
        return get_data_size(str(self.data))

    def write(self) -> None:
        self.LOG.info("正在清空表:" + self.target["table"])
        self.target_connect[self.target["table"]].drop()
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
        # 检查节点格式是否符合要求
        source_key = self.source.keys()
        assert "connect" in source_key, "节点的格式不符合要求：source中没有connect"
        assert "database" in source_key, "节点的格式不符合要求：source中没有database"
        assert "table" in source_key, "节点的格式不符合要求：source中没有table"
        target_key = self.target.keys()
        assert "path" in target_key, "节点的格式不符合要求：target中没有path"

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_nosql(self.source["connect"])[self.source["database"]]
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source["table"])
        self.data = self.source_connect[self.source["table"]].find().to_list()
        return get_data_size(str(self.data))

    def write(self) -> None:
        self.LOG.info("正在写入:" + os.path.join(JS_PATH, self.target["path"]))
        with open(os.path.join(JS_PATH, self.target["path"]), mode='w', encoding='utf-8') as file:
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
        # 检查节点格式是否符合要求
        source_key = self.source.keys()
        assert "connect" in source_key, "节点的格式不符合要求：source中没有connect"
        assert "database" in source_key, "节点的格式不符合要求：source中没有database"
        assert "table" in source_key, "节点的格式不符合要求：source中没有table"
        target_key = self.target.keys()
        assert "connect" in target_key, "节点的格式不符合要求：target中没有connect"
        assert "database" in target_key, "节点的格式不符合要求：target中没有database"
        assert "table" in target_key, "节点的格式不符合要求：target中没有table"

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_nosql(self.source["connect"])[self.source["database"]]
        self.target_connect = self.temp_db.get_nosql(self.target["connect"])[self.target["database"]]
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source["table"])
        # 查询时排除id字段
        self.data = self.source_connect[self.source["table"]].find({}, {'_id': 0})
        return get_data_size(str(self.data))

    def write(self) -> None:
        self.LOG.info("正在清空表:" + self.target["table"])
        self.target_connect[self.target["table"]].drop()
        self.LOG.info("正在写入表:" + self.target["table"])
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
        # 检查节点格式是否符合要求
        source_key = self.source.keys()
        assert "connect" in source_key, "节点的格式不符合要求：source中没有connect"
        assert "database" in source_key, "节点的格式不符合要求：source中没有database"
        assert "table" in source_key, "节点的格式不符合要求：source中没有table"
        target_key = self.target.keys()
        assert "connect" in target_key, "节点的格式不符合要求：target中没有connect"
        assert "table" in target_key, "节点的格式不符合要求：target中没有table"

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get_nosql(self.source["connect"])[self.source["database"]]
        self.target_connect = self.temp_db.get_sql(self.target["connect"])
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source["table"])
        temp_data = self.source_connect[self.source["table"]].find().to_list()
        self.data = pd.read_json(temp_data, orient="records")
        return get_data_size(str(self.data))

    def write(self) -> None:
        self.LOG.info("正在写入:" + self.target["table"])
        schema = self.target["schema"] if "schema" in self.target.keys() else None
        self.data.to_sql(name=self.target["table"], con=self.target_connect, schema=schema, index=False, if_exists='replace', chunksize=1000)
        
    def release(self) -> None:
        self.data = None
        self.target_connect.close()
        self.source_connect = None
        self.target_connect = None