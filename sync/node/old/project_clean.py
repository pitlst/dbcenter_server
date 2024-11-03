import tqdm
import pandas as pd
from general.base import db_process_base

tqdm.tqdm.pandas()

class project_clean(db_process_base):
    def __init__(self,):
        super().__init__("项目数据清洗", ["人员数据清洗", "项目基础数据信息同步"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")

    def read(self):
        self.LOG.info("开始读取")
        self.data_shr = pd.read_sql_table("dwd_staff", self.connection)
        self.data = pd.read_sql_table("ods_project_basic", self.connection)

    def write(self):
        self.LOG.info("开始写入")
        self.data.to_sql(name="dwd_project_basic", con=self.connection,index=False, if_exists='replace', chunksize=1000)

    def process(self):
        self.LOG.info("开始处理")
        self.data = self.data[self.data["单据状态"] == "已审核"]

        temp_shr = self.data_shr[["员工编码", "一级组织名称"]]
        temp_shr.columns = ["创建人", "创建人组织"]
        temp_data = pd.merge(self.data, temp_shr, how="left", on="创建人")
        temp_shr.columns = ["更改人", "更改人组织"]
        temp_data = pd.merge(temp_data, temp_shr, how="left", on="更改人")
        temp_shr.columns = ["工艺经理", "工艺经理组织"]
        temp_data = pd.merge(temp_data, temp_shr, how="left", on="工艺经理")

        temp_data_create = temp_data[temp_data["创建人组织"] == "城轨事业部"]
        temp_data_modify = temp_data[temp_data["更改人组织"] == "城轨事业部"]
        temp_data_crasfts = temp_data[temp_data["工艺经理组织"] == "城轨事业部"]
        temp_data = pd.concat([temp_data_create, temp_data_modify, temp_data_crasfts])
        self.data = temp_data.drop_duplicates()

        # 规避SettingWithCopyWarning警告
        temp_data = self.data.copy()
        self.data["项目简称"] = temp_data["项目简称"].map(str.strip)
        
    def release(self) -> None:
        self.data_shr = None
        self.data = None
        self.connection.close()
