import tqdm
import datetime
import pandas as pd
from general.base import db_process_base
from general.commont import change_character_code

tqdm.tqdm.pandas()

class attendance_clean(db_process_base):
    def __init__(self):
        super().__init__("考勤数据清洗", ["考勤类型同步", "考勤时间计算后同步", "人员数据清洗"])
        self.request_0 = ["实上班"]
        self.request_1 = ["平常加班", "休息日加班"]
        self.request_2 = ["病假","丧假","婚假","迟到","早退","旷工","事假"]
        self.request_3 = ["出差","年假","产假","疗养","参加会议","员工体检","工会活动","参加培训","因公出勤","客户接待"]
        
    def connect(self):
        self.LOG.info("开始连接")
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")

    def read(self):
        self.LOG.info("开始读取")
        self.data = pd.read_sql_table("ods_kq_time", self.connection)
        self.data_type = pd.read_sql_table("ods_kq_type", self.connection)
        self.data_staff = pd.read_sql_table("dwd_staff", self.connection)
        
        self.now = datetime.datetime.now()

    def write(self):
        self.LOG.info("开始写入")
        self.data_kq.to_sql(name="dwd_kq_time", con=self.connection, index=False, if_exists='replace', chunksize=1000)
        
    def process(self):
        self.LOG.info("开始处理")
        def temp_apply(row):
            row["类型名称"] = change_character_code(row["类型名称"])
            return row
        self.data_type = self.data_type.progress_apply(temp_apply, axis=1)
        self.data = pd.merge(self.data, self.data_type.loc[:,["id", "类型名称"]], how="left", left_on="考勤类型id", right_on="id")
        self.data = pd.merge(self.data, self.data_staff.loc[:,["考勤系统id", "员工编码", "员工姓名", "一级组织名称", "二级组织名称", "末级组织名称"]], how="left", left_on="员工考勤系统id", right_on="考勤系统id")
        self.data = self.data.loc[:, ['id', '调整时间', '时间长度(小时)', '时间长度(分钟)', '时间长度(百分比)', '类型名称', '员工编码', '员工姓名', '一级组织名称', '二级组织名称', '末级组织名称']]
        self.LOG.debug("处理各类型时长")
        self.data = self.data.loc[self.data["一级组织名称"] == "城轨事业部"]
        self.data = self.data.reindex(columns=self.data.columns.to_list() + ["聚合后分类"])
        def temp_apply(row):
            if row["类型名称"] in self.request_0:
                row["聚合后分类"] = "上班"
            elif row["类型名称"] in self.request_1:
                row["聚合后分类"] = "加班"                
            elif row["类型名称"] in self.request_2:
                row["聚合后分类"] = "因私出勤"
            elif row["类型名称"] in self.request_3:
                row["聚合后分类"] = "因公出勤"
            return row
        self.data = self.data.progress_apply(temp_apply, axis=1)
        self.data_kq = pd.pivot_table(self.data, index=["员工编码", "员工姓名", "一级组织名称", "二级组织名称", "末级组织名称", "调整时间"], columns=["聚合后分类"], values=["时间长度(分钟)"], aggfunc="sum").reset_index()
        self.data_kq.columns = ["工号", "姓名", "组织", "部门", "组室", "日期", "出勤时长", "加班时长", "因私出勤时长", "因公出勤时长"]
        self.data_kq = self.data_kq.fillna(value=0.0)
        
        self.data_kq = self.data_kq.reindex(columns=self.data_kq.columns.to_list() + ["生产时长"])
        def temp_apply(row):
            row["生产时长"] = (row["出勤时长"] if not pd.isna(row["出勤时长"]) else 0) + (row["加班时长"] if not pd.isna(row["加班时长"]) else 0) - (row["因公出勤时长"] if not pd.isna(row["因公出勤时长"]) else 0)
            row["出勤时长"] = (row["出勤时长"] if not pd.isna(row["出勤时长"]) else 0) + (row["加班时长"] if not pd.isna(row["加班时长"]) else 0)
            return row
        self.data_kq = self.data_kq.progress_apply(temp_apply, axis=1)
        
    def release(self) -> None:
        self.data = None
        self.data_type = None
        self.data_staff = None
        self.data_kq = None
        self.connection.close()