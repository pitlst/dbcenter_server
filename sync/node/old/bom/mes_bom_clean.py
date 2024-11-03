import tqdm
import pymongo
import pandas as pd
from general.base import db_process_base
from general.connect import PYMONGO_CONNECT_URL

tqdm.tqdm.pandas()

class order_bom_clean(db_process_base):
    def __init__(self) -> None:
        super().__init__("mes订单bom清洗", ["mes订单bom同步"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.client = pymongo.MongoClient(PYMONGO_CONNECT_URL)
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")
        
    def read(self) -> None:
        self.LOG.info("开始读取")
        self.collection = self.client["dataframe"]["bom_mes_order"]
        self.data_bom = pd.read_sql_table("ods_mes_order_bom", self.connection)
    
    def write(self) -> None:
        self.LOG.info("开始写入")
        # 删所有旧的
        self.collection.delete_many({})
        self.collection.insert_many(self.data)
    
    def process(self) -> None:
        self.LOG.info("开始处理")
        self.data = []
        ...
        
    def release(self) -> None:
        self.collection = None
        self.data = None
        self.data_head = None
        self.data_entry = None
        self.data_run = None
        self.connection.close()