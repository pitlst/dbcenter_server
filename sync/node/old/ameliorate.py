import tqdm
import datetime
import pandas as pd
from general.base import db_process_base

tqdm.tqdm.pandas()

class ameliorate_process(db_process_base):
    def __init__(self) -> None:
        super().__init__("全员型改善数据处理", [
            "全员型改善同步", 
            "城轨各部门改善指标-各部门指标", 
            "城轨各部门改善指标-质量技术部组室指标", 
            "城轨各部门改善指标-综合管理部组室指标", 
            "城轨各部门改善指标-项目工程部组室指标", 
            "城轨各部门改善指标-交车车间班组指标", 
            "城轨各部门改善指标-总成车间班组指标"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")
        
    def read(self) -> None:
        self.LOG.info("开始读取")
        self.data_ameliorate = pd.read_sql_table("ods_ameliorate", self.connection)
        self.data_department = pd.read_sql_table("ods_ameliorate_department_index", self.connection)
        self.data_group_quality = pd.read_sql_table("ods_ameliorate_quality_group_index", self.connection)
        self.data_group_complex = pd.read_sql_table("ods_ameliorate_synthesis_group_index", self.connection)
        self.data_group_project = pd.read_sql_table("ods_ameliorate_item_group_index", self.connection)
        self.data_group_delivery = pd.read_sql_table("ods_ameliorate_delivery_group_index", self.connection)
        self.data_group_assembly = pd.read_sql_table("ods_ameliorate_assembly_group_index", self.connection)
        
        self.now = datetime.datetime.now()

    def write(self) -> None:
        self.LOG.info("开始写入")
        self.data_department.to_sql(name="dm_ameliorate_department", con=self.connection, index=False, if_exists='replace', chunksize=1000)
        self.data_group.to_sql(name="dm_ameliorate_group", con=self.connection, index=False, if_exists='replace', chunksize=1000)

    def process(self) -> None:
        self.LOG.info("开始处理")
        data_ameliorate_temp = self.data_ameliorate[
            (self.data_ameliorate["提案单位一级"] == "城轨事业部") &
            (self.data_ameliorate["提交日期"] >= datetime.datetime(self.now.year, self.now.month, 1))
            ]

        def temp_apply(row):
            for _index, _row in data_ameliorate_temp.iterrows():
                if row["部门"] == _row["提案单位二级"]:
                    row["已完成数"] += 1
            row["差额"] = row["指标"] - row["已完成数"]
            return row

        self.data_department = self.data_department.progress_apply(temp_apply, axis=1)
        self.data_department['差额'] = self.data_department['差额'].apply(lambda x: max(0, x))

        def temp_apply(row):
            for _index, _row in data_ameliorate_temp.iterrows():
                if row["组室"] == _row["班组"]:
                    row["已完成数"] += 1
            row["差额"] = row["指标"] - row["已完成数"]
            return row

        self.data_group_quality = self.data_group_quality.progress_apply(temp_apply, axis=1)
        self.data_group_quality['差额'] = self.data_group_quality['差额'].apply(lambda x: max(0, x))
        self.data_group_complex = self.data_group_complex.progress_apply(temp_apply, axis=1)
        self.data_group_complex['差额'] = self.data_group_complex['差额'].apply(lambda x: max(0, x))
        self.data_group_project = self.data_group_project.progress_apply(temp_apply, axis=1)
        self.data_group_project['差额'] = self.data_group_project['差额'].apply(lambda x: max(0, x))
        self.data_group_delivery = self.data_group_delivery.progress_apply(temp_apply, axis=1)
        self.data_group_delivery['差额'] = self.data_group_delivery['差额'].apply(lambda x: max(0, x))
        self.data_group_assembly = self.data_group_assembly.progress_apply(temp_apply, axis=1)
        self.data_group_assembly['差额'] = self.data_group_assembly['差额'].apply(lambda x: max(0, x))
        self.data_group = pd.concat([
            self.data_group_quality,
            self.data_group_complex,
            self.data_group_project,
            self.data_group_delivery,
            self.data_group_assembly
        ])
        
    def release(self) -> None:
        self.data_ameliorate = None
        self.data_department = None
        self.data_group_quality = None
        self.data_group_complex = None
        self.data_group_project = None
        self.data_group_delivery = None
        self.data_group_assembly = None
        self.data_group = None
        self.connection.close()