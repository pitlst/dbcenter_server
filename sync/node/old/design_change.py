import tqdm
import pymongo
import pandas as pd
from general.base import db_process_base
from general.connect import PYMONGO_CONNECT_URL

tqdm.tqdm.pandas()

class design_change_clean(db_process_base):
    def __init__(self) -> None:
        super().__init__("设计变更数据清洗", [])

    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.client = pymongo.MongoClient(PYMONGO_CONNECT_URL)
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")
        
    def read(self) -> None:
        self.LOG.info("开始读取")
        self.collection = self.client["dataframe"]["tree_process_flow"]
        # self.data_head = pd.read_sql_table("ods_tk_crrc_craftchangebill", self.connection)
        # self.data_entry = pd.read_sql_table("ods_tk_crrc_flowentry", self.connection)
        # self.data_run = pd.read_sql_table("ods_tk_crrc_chgbill", self.connection)
    
    def write(self) -> None:
        self.LOG.info("开始写入")
        # 删所有旧的
        # self.collection.delete_many({})
        # self.collection.insert_many(self.data)
    
    def process(self) -> None:
        self.LOG.info("开始处理")
        # self.data = []
        # def temp_apply(row):
        #     temp_data = {}
        #     temp_data["工艺流程单id"] = row["id"]
        #     temp_data["工艺流程单号"] = row["变更单号"]
        #     temp_data["工艺流程单名称"] = row["变更单名称"]
        #     temp_data["关联设计变更单编号"] = row["关联设计变更单编号"]
        #     temp_data["关联设计变更单名称"] = row["关联设计变更单名称"]
        #     temp_data["项目号"] = row["项目号"]
        #     temp_data["项目名称"] = row["项目名称"]
        #     temp_data["发起时间"] = row["发起时间"] if pd.notna(row["发起时间"]) and pd.notnull(row["发起时间"]) else ""
        #     temp_data["工艺经理工号"] = row["工艺经理工号"] if pd.notna(row["工艺经理工号"]) and pd.notnull(row["工艺经理工号"]) else ""
        #     temp_data["工艺经理姓名"] = row["工艺经理姓名"] if pd.notna(row["工艺经理姓名"]) and pd.notnull(row["工艺经理姓名"]) else ""
        #     temp_data["任务流程"] = []
        #     temp_data_entry = self.data_entry[self.data_entry["id"] == row["id"]]
        #     for index_ ,row_ in temp_data_entry.iterrows():
        #         temp_data_2 = {}
        #         temp_data_2["单据体id"] = row_["单据体顺序"] 
        #         temp_data_2["操作人工号"] = row_["操作人编码"]
        #         temp_data_2["操作人姓名"] = row_["操作人名称"]
        #         temp_data_2["操作时间"] = row_["操作时间"]
        #         temp_data_2["操作动作"] = row_["操作动作"]
        #         temp_data_2["被分配人工号"] = row_["被分配人编码"] if pd.notna(row_["被分配人编码"]) and pd.notnull(row_["被分配人编码"]) else ""
        #         temp_data_2["被分配人姓名"] = row_["被分配人名称"] if pd.notna(row_["被分配人名称"]) and pd.notnull(row_["被分配人名称"]) else "" 
        #         temp_data_2["操作意见"] = row_["意见"] if pd.notna(row_["意见"]) and pd.notnull(row_["意见"]) and str(row_["意见"]).replace(" ", "") != "" else ""
        #         temp_data["任务流程"].append(temp_data_2)
        #     self.data.append(temp_data)
        # self.data_head.progress_apply(temp_apply, axis=1)
        
    def release(self) -> None:
        self.collection = None
        self.data = None
        self.data_head = None
        self.data_entry = None
        self.data_run = None
        self.connection.close()