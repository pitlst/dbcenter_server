import tqdm
import datetime
import pandas as pd
from general.base import db_process_base

tqdm.tqdm.pandas()

class travel_clean(db_process_base):
    def __init__(self):
        super().__init__("差旅数据清洗", ["差旅信息同步", "差旅筛选单据同步"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")
        
    def read(self):
        self.LOG.info("开始读取")
        self.data_travel = pd.read_sql_table("ods_travel", self.connection)
        self.data_travel_fillter = pd.read_sql_table("ods_travel_fillter", self.connection)
        
        self.now = datetime.datetime.now()
        
    def write(self):
        self.LOG.info("开始写入")
        self.data_travel.to_sql(name="dwd_travel", con=self.connection, index=False, if_exists='replace', chunksize=1000)

    def process(self):
        self.LOG.info("开始处理")
        
        self.LOG.debug("筛选数据为今年")
        def temp_apply(_row):
            return str(_row['申请单单据编号'])[5:9] == str(datetime.date.today().year) or str(_row['报销单单据编号'])[6:10] == str(datetime.date.today().year)
        self.data_travel = self.data_travel.loc[lambda x: x.apply(temp_apply, axis=1), :]
        self.data_travel = self.data_travel.loc[~(self.data_travel['申请单单据编号'].isin(list(self.data_travel_fillter['排除的单据编号'])))]

        self.LOG.debug("筛选费用承担部门为城轨")
        self.data_travel = self.data_travel[self.data_travel['费用承担部门'] == '城轨事业部']  
        
    def release(self) -> None:
        self.data_travel = None
        self.data_travel_fillter = None
        self.connection.close()
    
        
        
class travel_process(db_process_base):
    def __init__(self):
        super().__init__("差旅数据处理", ["差旅数据清洗", "差旅数据-差旅交通与住宿指标", "差旅数据-部门指标"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")

    def read(self):
        self.LOG.info("开始读取")
        self.data_travel_common_index = pd.read_sql_table("ods_travel_common_index", self.connection)
        self.data_travel_common_index = pd.read_sql_table("ods_travel_department_index", self.connection)
        
    def write(self):
        self.LOG.info("开始写入")

    def process(self):
        self.LOG.info("开始处理")
        
        
    def release(self) -> None:
        self.connection.close()