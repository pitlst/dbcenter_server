import os
from typing import Callable
from general import log_run_time
from general.logger import LOG
from general.executer import EXECUTER

SELECT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "select")
TABLE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "file")

class node:
    '''节点的执行实现'''
    allow_type = [
        "sql_to_table",
        "sql_to_excel",
        "sql_to_csv",
        "sql_to_nosql",
        "sql_to_json",
        "table_to_table",
        "table_to_excel",
        "table_to_csv",
        "table_to_nosql",
        "table_to_json",
        "excel_to_table",
        "excel_to_csv",
        "excel_to_nosql",
        "excel_to_json",
        "csv_to_table",
        "csv_to_nosql",
        "csv_to_excel",
        "csv_to_json",
        "nosql_to_nosql",
        "nosql_to_json",
        "nosql_to_table",
        "nosql_to_excel",
        "nosql_to_csv",
        "json_to_nosql"
    ]
    def __init__(self, node_define: dict, preprocess_func: Callable = None, postprocess_func: Callable = None) -> None:
        self.node = node_define
        assert self.node["type"] in self.allow_type, "未知的节点类型"
        self.type = self.node["type"]
        self.preprocess_func = preprocess_func
        self.postprocess_func = postprocess_func
        
    
    @log_run_time
    def run(self) -> str:
        LOG.info(self.node["name"] + "开始计算")
        if not self.postprocess_func is None:
            self.write(self.postprocess_func(self.read()))
        else:
            self.write(self.read())
        LOG.info(self.node["name"] + "计算结束")
        return self.node["name"]
        
    def read(self):
        temp_data = None
        if self.type.split("_to_")[0] == "sql":
            temp_sql = self.node["source"]["sql"] if self.preprocess_func is None else self.preprocess_func(os.path.join(SELECT_PATH, self.node["source"]["sql"]))
            temp_data = EXECUTER[self.node["source"]["connect"]].read_from_sql(temp_sql)
        elif self.type.split("_to_")[0] == "table":
            if not self.preprocess_func is None:
                temp_table, temp_schema = self.preprocess_func(self.node["source"]["table"], self.node["source"]["schema"])
            else:
                temp_table, temp_schema = self.node["source"]["table"], self.node["source"]["schema"]
            temp_data = EXECUTER[self.node["source"]["connect"]].read_from_table(temp_table, temp_schema)
        elif self.type.split("_to_")[0] == "excel":
            if not self.preprocess_func is None:
                temp_path, temp_sheet = self.preprocess_func(os.path.join(TABLE_PATH, self.node["source"]["path"]), self.node["source"]["sheet"])
            else:
                temp_path, temp_sheet = os.path.join(TABLE_PATH, self.node["source"]["path"]), self.node["source"]["sheet"]
            temp_data = EXECUTER[self.node["source"]["connect"]].read_from_excel(temp_path, temp_sheet)
        elif self.type.split("_to_")[0] == "csv":
            if not self.preprocess_func is None:
                temp_path = self.preprocess_func(os.path.join(TABLE_PATH, self.node["source"]["path"]))
            else:
                temp_path = os.path.join(TABLE_PATH, self.node["source"]["path"])
            temp_data = EXECUTER[self.node["source"]["connect"]].read_from_csv(temp_path)
        elif self.type.split("_to_")[0] == "nosql":
            if not self.preprocess_func is None:
                temp_database, temp_table = self.preprocess_func(self.node["source"]["database"], self.node["source"]["table"])
            else:
                temp_database, temp_table = self.node["source"]["database"], self.node["source"]["table"]
            temp_data = EXECUTER[self.node["source"]["connect"]].read_from_nosql(temp_database, temp_table)
        elif self.type.split("_to_")[0] == "json":
            if not self.preprocess_func is None:
                temp_path = self.preprocess_func(os.path.join(SELECT_PATH, self.node["source"]["path"]))
            else:
                temp_path = os.path.join(SELECT_PATH, self.node["source"]["path"])
            temp_data = EXECUTER[self.node["source"]["connect"]].read_from_json(temp_path)
        return temp_data
    
    def write(self, temp_data):
        if not temp_data is None and len(temp_data) != 0:
            if self.type.split("_to_")[1] == "table":
                if "schema" in self.node["target"].keys():
                    EXECUTER[self.node["target"]["connect"]].write_to_table(temp_data, self.node["target"]["table"], self.node["target"]["schema"])
                else:
                    EXECUTER[self.node["target"]["connect"]].write_to_table(temp_data, self.node["target"]["table"])
            elif self.type.split("_to_")[1] == "excel":
                EXECUTER[self.node["target"]["connect"]].write_to_excel(temp_data, os.path.join(TABLE_PATH, self.node["target"]["path"]), self.node["target"]["sheet"])
            elif self.type.split("_to_")[1] == "csv":
                EXECUTER[self.node["target"]["connect"]].write_to_csv(temp_data, os.path.join(TABLE_PATH, self.node["target"]["path"]))
            elif self.type.split("_to_")[1] == "nosql":
                EXECUTER[self.node["target"]["connect"]].write_to_nosql(temp_data, self.node["target"]["database"], self.node["target"]["table"])
            elif self.type.split("_to_")[1] == "json":
                EXECUTER[self.node["target"]["connect"]].write_to_json(temp_data, os.path.join(SELECT_PATH, self.node["target"]["path"]))