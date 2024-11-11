import requests
import pandas as pd
from io import StringIO
from sys import getsizeof
from node.base import node_base  # 用于类型标注
from general.connect import db_engine
from general.config import SYNC_CONFIG

# 构造请求头
requests_head = SYNC_CONFIG["request_head"]
headers = {
    'Accept': requests_head["Accept"],
    'Accept-Encoding': requests_head["Accept-Encoding"],
    'Accept-Language': requests_head["Accept-Language"],
    'Connection': requests_head["Connection"],
    'Cookie': requests_head["Cookie"],
    'DNT': requests_head["DNT"],
    'Host': requests_head["Host"],
    'If-None-Match': requests_head["If-None-Match"],
    'Sec-Fetch-Dest': requests_head["Sec-Fetch-Dest"],
    'Sec-Fetch-Mode': requests_head["Sec-Fetch-Mode"],
    'Sec-Fetch-Site': requests_head["Sec-Fetch-Site"],
    'Sec-Fetch-User': requests_head["Sec-Fetch-User"],
    'Upgrade-Insecure-Requests': requests_head["Upgrade-Insecure-Requests"],
    'User-Agent': requests_head["User-Agent"],
    'sec-ch-ua-mobile': requests_head["sec-ch-ua-mobile"],
    'sec-ch-ua-platform': requests_head["sec-ch-ua-platform"]
}

class heyform_node(node_base):
    '''使用api获取数据存放到数据库'''
    allow_type = ["api_table", "api_json"]
    def __init__(self, node_define: dict) -> None:
        super().__init__(node_define["name"], db_engine, node_define["type"])
        self.source: dict = node_define["source"]
        self.target: dict = node_define["target"]
        self.data = None
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        if self.type == "api_table":
            self.target_connect = self.temp_db.get_sql(self.target["connect"])
        elif self.type == "api_json":
            self.target_connect = self.temp_db.get_nosql(self.target["connect"])[self.target["database"]]
        
    def read(self) -> None:
        self.LOG.info("正在获取:" + self.source["url"])
        temp_csv = requests.get(self.source["url"], headers=headers).text
        self.data = pd.read_csv(StringIO(temp_csv))
        return self.get_data_size()

    def write(self) -> None:
        if self.type == "api_table":
            self.LOG.info("正在写入:" + self.target["connect"] + "的" + self.target["table"])
            self.data.to_sql(name=self.target["table"], con=self.target_connect,index=False, if_exists='replace', chunksize=1000)
        elif self.type == "api_json":
            self.LOG.info("正在写入:" + self.target["database"] + "的" + self.target["collection"])
            self.target_connect[self.target["collection"]].insert_many(self.data)
        
    def release(self) -> None:
        self.data = None
        self.target_connect = None

    def get_data_size(self) -> int:
        '''获取data的内存占用,用于计算同步时间间隔'''
        if self.type == "api_table":
            return int(self.data.values.nbytes / 1024**2)
        elif self.type == "api_json":
            return int(getsizeof(self.data) / 1024**2)
    