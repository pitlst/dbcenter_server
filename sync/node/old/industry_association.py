import tqdm
import pandas as pd
from general.base import db_process_base

tqdm.tqdm.pandas()

class industry_association_clean(db_process_base):
    def __init__(self) -> None:
        super().__init__("业联数据清洗", ["业联执行关闭同步", "人员数据清洗"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")
        
    def read(self) -> None:
        self.LOG.info("开始读取")
        self.data = pd.read_sql_table("ods_industry_association", self.connection)
        self.data_shr_staff = pd.read_sql_table("dwd_staff", self.connection)
    
    def write(self) -> None:
        self.LOG.info("开始写入")
        self.data.to_sql(name="dwd_industry_association", con=self.connection, index=False, if_exists='replace', chunksize=1000)
    
    def process(self) -> None:
        self.LOG.info("开始处理")
        self.data = self.data[self.data["部门名称"] == "城轨事业部"]
        temp_xiangying = self.data_shr_staff[["员工编码", "二级组织名称"]]
        self.data = pd.merge(self.data, temp_xiangying, how="left", left_on="创建人工号", right_on="员工编码")
        
        def temp_apply(row):
            if str(row["车号"]).isdigit():
                if int(row["车号"]) >= 0 and int(row["车号"]) < 10:
                    row["车号"] = "000" + row["车号"]
                elif int(row["车号"]) >= 10 and int(row["车号"]) < 100:
                    row["车号"] = "00" + row["车号"]
                elif int(row["车号"]) >= 100 and int(row["车号"]) < 1000:
                    row["车号"] = "0" + row["车号"]
            return row
        self.data = self.data.progress_apply(temp_apply, axis=1)
        
    def release(self) -> None:
        self.data = None
        self.data_shr_staff = None
        self.connection.close()
