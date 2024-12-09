import os
import json
import pandas as pd
import numpy as np 
from general.logger import node_logger
from general.connect import db_engine

SQL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "sql")
TABLE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "table")
JS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "mongo_js")

class executer:
    def __init__(self, node_define: dict) -> None:
        self.LOG = node_logger(node_define["name"])
    
    def run(self) -> None:
        ...
        
    def read(self) -> None: 
        ...
        
    def write(self) -> None: 
        ...
        
    @staticmethod
    def __trans_table_to_json(__data: pd.DataFrame) -> dict:
        ...
        
    @staticmethod
    def __trans_json_to_table(__data: dict) -> pd.DataFrame:
        ...
    
    @staticmethod
    def __read_from_sql__(__sql: str) -> pd.DataFrame:
        ...
        
    @staticmethod
    def __read_from_table__(__table_name: str) -> pd.DataFrame:
        ...
    
    @staticmethod
    def __read_from_nosql__(__coll_name: str) -> dict:
        ...
    
    @staticmethod
    def __read_from_excel__(__file_path: str) -> pd.DataFrame:
        ...
    
    @staticmethod
    def __read_from_csv__(__file_path: str) -> pd.DataFrame:
        ...
        
    @staticmethod
    def __read_from_json__(__file_path: str) -> dict:
        ...
        
    @staticmethod
    def __write_to_table__(__data: pd.DataFrame, __table_name: str) -> None:
        ...
        
    @staticmethod
    def __write_to_nosql__(__data: dict,  __coll_name: str) -> None:
        ...
        
    @staticmethod
    def __write_to_excel__(__data: pd.DataFrame, __file_path: str) -> None:
        ...
        
    @staticmethod
    def __write_to_csv__(__data: pd.DataFrame, __file_path: str) -> None:
        ...
        
    @staticmethod
    def __write_to_json__(__data: dict, __file_path: str) -> None:
        ...