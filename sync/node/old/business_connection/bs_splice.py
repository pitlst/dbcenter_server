import tqdm
import datetime
import pandas as pd
from general.base import db_process_base

tqdm.tqdm.pandas()

class shop_exection_splice(db_process_base):
    def __init__(self) -> None:
        super().__init__("车间执行单拼接", [])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.source_connect = self.temp_db.get(self.LOG, "数据仓库缓存_新")
        self.target_connect = self.temp_db.get(self.LOG, "数据仓库缓存_mongo")
        
    def read(self) -> None:
        self.LOG.info("开始读取")
        ...
        
    def write(self) -> None:
        self.LOG.info("开始写入")
        ...
        
    def process(self) -> None:
        self.LOG.info("开始处理")
        ...
        
    def release(self) -> None:
        self.source_connect.close()