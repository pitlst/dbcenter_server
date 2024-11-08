import requests
from sync.general.connect import database_connect
from sync.node.base import node_base  # 用于类型标注
from general.connect import db_engine

class api_node(node_base):
    ...
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]