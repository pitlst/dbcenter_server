import tqdm
import datetime
import pandas as pd
from general.base import db_process_base

tqdm.tqdm.pandas()

class delivery_process(db_process_base):
    def __init__(self):
        super().__init__("交车看板数据处理", [
            "派工单同步",
            "异常数据清洗",
            "调试任务同步", 
            "交车轨道负责人同步", 
            "业联数据清洗", 
            "项目数据清洗", 
            "人员数据清洗"])
        
    def connect(self):
        self.LOG.info("开始连接")
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")

    def read(self):
        self.LOG.info("开始读取")
        self.data_dispatch_list = pd.read_sql_table("ods_dispatch_list", self.connection)
        self.data_abnormal = pd.read_sql_table("dwd_abnormal", self.connection)
        self.data_debug_tasks = pd.read_sql_table("ods_debug_tasks", self.connection)
        self.data_delivery_train = pd.read_sql_table("ods_delivery_train", self.connection)
        self.data_industry_association = pd.read_sql_table("dwd_industry_association", self.connection)
        self.data_project = pd.read_sql_table("dwd_project_basic", self.connection)
        self.data_shr_staff = pd.read_sql_table("dwd_staff", self.connection)
        
        self.now = datetime.datetime.now()

    def write(self):
        self.LOG.info("开始写入")
        self.data.to_sql(name="dm_delivery", con=self.connection, index=False, if_exists='replace', chunksize=1000)
        self.data_error.to_sql(name="dm_delivery_error", con=self.connection, index=False, if_exists='replace', chunksize=1000)
        self.data_process.to_sql(name="dm_delivery_process", con=self.connection, index=False, if_exists='replace', chunksize=1000)

    def process(self):
        self.LOG.info("开始处理")
        self.LOG.debug("为负责人添加班组")
        temp_fuze = self.data_shr_staff[["员工编码", "二级组织名称", "末级组织名称", "手机号码"]]
        temp_fuze.columns = ["负责人工号", "负责人部门", "负责人组室", "手机号码"]        
        self.data_delivery_train = pd.merge(self.data_delivery_train, temp_fuze, how="left", on="负责人工号")
        self.LOG.debug("添加列")
        extend_list = [
            "未关闭异常数",
            "未处理异常数", 
            "未响应异常数", 
            "异常总数", 
            "Q40时间", 
            "调试项点合格数", 
            "调试项点数", 
            "调试进度", 
            "业联未关闭数", 
            "项目简称", 
            "项目名称"]
        data_delivery_train_total = self.data_delivery_train.reindex(columns=self.data_delivery_train.columns.to_list() + extend_list)
        extend_list_error = [
            "未响应异常数", 
            "未处理异常数", 
            "未关闭异常数", 
            "异常总数",
            "业联未关闭数"]
        data_delivery_train_error = self.data_delivery_train.reindex(columns=self.data_delivery_train.columns.to_list() + extend_list_error)
        data_delivery_train_error["join"] = 1
        data_delivery_train_process = self.data_delivery_train.copy()

        
        self.LOG.debug("筛选派工单")
        self.data_dispatch_list = self.data_dispatch_list.loc[self.data_dispatch_list["车号"].isin(self.data_delivery_train["车号"].to_list())]
        self.data_dispatch_list = self.data_dispatch_list.loc[self.data_dispatch_list["班组"].isin(self.data_delivery_train["负责人组室"].to_list())]
        self.LOG.debug("筛选当前开工")
        temp_date_dispatch_list = self.data_dispatch_list.loc[self.data_dispatch_list["派工单状态"] == "1"]
        self.LOG.debug("筛选Q40")
        temp_q40_dispatch_list = self.data_dispatch_list.loc[(self.data_dispatch_list["工序编码"] == "GX032009703005") | (self.data_dispatch_list["工序编码"] == "1502001")]
        self.LOG.debug("筛选调试文件")
        self.data_debug_tasks = self.data_debug_tasks.loc[self.data_debug_tasks["车号"].isin(self.data_delivery_train["车号"].to_list())]
        self.data_debug_tasks = self.data_debug_tasks.loc[self.data_debug_tasks["班组名称"].isin(self.data_delivery_train["负责人组室"].to_list())]
        self.LOG.debug("筛选异常，确定相关异常数")
        self.data_abnormal = self.data_abnormal.loc[self.data_abnormal["车号"].isin(self.data_delivery_train["车号"].to_list())]
        self.data_abnormal = self.data_abnormal.loc[self.data_abnormal["发起人组室"].isin(self.data_delivery_train["负责人组室"].to_list())]
        self.LOG.debug("筛选业联关闭单，获取当前项目的未关闭业联数")
        new_dataframe = pd.DataFrame(columns=self.data_industry_association.columns)
        for _index, row in self.data_industry_association.iterrows():
            temp_name = str(row["项目号"]) + str(row["车号"])
            if temp_name in self.data_delivery_train["车号"].to_list() and int(row["是否需要质检"]) == 1 and row["单据状态"] == "B":
                new_dataframe.loc[len(new_dataframe.index)] = row

        self.LOG.debug("为交车轨道看板表填充数据")
        def temp_apply(row):
            for _index, _row in self.data_project.iterrows():
                temp_project = str(row["车号"])[:8]
                if temp_project == _row["项目号"]:
                    row["项目名称"] = _row["项目名称"]
                    row["项目简称"] = _row["项目简称"]
                    break
            for _index, _row in temp_q40_dispatch_list.iterrows():
                if row["车号"] == _row["车号"]:
                    row["Q40时间"] = _row["计划开始时间"]
                    break
            for _index, _row in self.data_abnormal.iterrows():
                if row["车号"] == _row["车号"] and row["负责人组室"] == _row["发起人组室"]:
                    row["异常总数"] = self.add_null_proof(row["异常总数"])
                    if _row["异常状态分类"] == "待响应":
                        row["未响应异常数"] = self.add_null_proof(row["未响应异常数"])
                    elif _row["异常状态分类"] == "待处理":
                        row["未处理异常数"] = self.add_null_proof(row["未处理异常数"])
                    elif _row["异常状态分类"] == "待关闭":
                        row["未关闭异常数"] = self.add_null_proof(row["未关闭异常数"])
            for _index, _row in self.data_debug_tasks.iterrows():
                if row["车号"] == _row["车号"]:
                    row["调试项点数"] = self.add_null_proof(row["调试项点数"])
                    if _row["调试项点状态"] == "合格":
                        row["调试项点合格数"] = self.add_null_proof(row["调试项点合格数"])
            row["调试进度"] = row["调试项点合格数"] / row["调试项点数"]
            for _index, _row in new_dataframe.iterrows():
                if row["车号"] == (str(_row["项目号"]) + str(_row["车号"])):
                    row["业联未关闭数"] = self.add_null_proof(row["业联未关闭数"])
            # 车号仅取最后两位
            row["车号"] = str(row["车号"])[-2:]
            return row
        self.data = data_delivery_train_total.progress_apply(temp_apply, axis=1)
        
        self.LOG.debug("为交车轨道看板异常分表填充数据")
        temp_dataframe = pd.DataFrame({
            "部门":["综合管理部", "质量技术部","项目工程部","城轨总成车间","城轨交车车间"],
            "join":[1,1,1,1,1]
        })
        self.data_error = pd.merge(data_delivery_train_error, temp_dataframe, how="left", on="join")
        def temp_apply(row):
            for _index, _row in self.data_abnormal.iterrows():
                if row["车号"] == _row["车号"] and row["负责人组室"] == _row["发起人组室"]:
                    if row["部门"] == _row["指定响应人部门"]:
                        row["异常总数"] = self.add_null_proof(row["异常总数"])
                    if row["部门"] == _row["指定响应人部门"] and _row["异常状态分类"] == "待响应":
                        row["未响应异常数"] = self.add_null_proof(row["未响应异常数"])
                    if row["部门"] == _row["响应人部门"] and _row["异常状态分类"] == "待处理":
                        row["未处理异常数"] = self.add_null_proof(row["未处理异常数"])
                    if row["部门"] == _row["处理人部门"] and _row["异常状态分类"] == "待关闭":
                        row["未关闭异常数"] = self.add_null_proof(row["未关闭异常数"])
            for _index, _row in new_dataframe.iterrows():
                if row["车号"] == (str(_row["项目号"]) + str(_row["车号"])) and row["部门"] == _row["二级组织名称"]:
                    row["业联未关闭数"] = self.add_null_proof(row["业联未关闭数"])
            return row
        self.data_error = self.data_error.progress_apply(temp_apply, axis=1)
        
        self.LOG.debug("为交车轨道看板工序分表填充数据")
        data_delivery_train_process = pd.merge(data_delivery_train_process, temp_date_dispatch_list.loc[:,["工序编码", "工序名称", "班组"]], how="left", left_on="负责人组室", right_on="班组")
        data_delivery_train_process = data_delivery_train_process.reindex(columns=data_delivery_train_process.columns.to_list() + ["工序进度", "工序项点数", "工序项点完成数"])
        # 用于处理FutureWarning: Setting an item of incompatible dtype is deprecated and will raise in a future error of pandas
        data_delivery_train_process["工序编码"] = data_delivery_train_process["工序编码"].astype(str)
        data_delivery_train_process["工序名称"] = data_delivery_train_process["工序名称"].astype(str)
        data_delivery_train_process["班组"] = data_delivery_train_process["班组"].astype(str)
        def temp_apply(row):
            if row["工序名称"] == "nan":
                row["工序名称"] = "无"
            for _index, _row in self.data_debug_tasks.iterrows():
                if row["工序编码"] == _row["工序编码"]:
                    row["工序项点数"] = self.add_null_proof(row["工序项点数"])
                    if _row["调试项点状态"] == "合格":
                        row["工序项点完成数"] = self.add_null_proof(row["工序项点完成数"])
            row["工序进度"] = row["工序项点完成数"] / row["工序项点数"]
            return row
        self.data_process: pd.DataFrame = data_delivery_train_process.progress_apply(temp_apply, axis=1)
        self.data_process.loc[:, ["工序项点数", "工序项点完成数", "工序进度"]] = self.data_process.loc[:, ["工序项点数", "工序项点完成数", "工序进度"]].fillna(value=0)
        self.data_process.loc[:, ["工序编码", "工序名称", "班组"]] = self.data_process.loc[:, ["工序编码", "工序名称", "班组"]].fillna(value="")
        
    @staticmethod
    def add_null_proof(input_row):
        if not pd.isna(input_row) or not pd.isnull(input_row):
            input_row += 1  
        else:
            input_row = 1
        return input_row
    
    def release(self) -> None:
        self.data_dispatch_list = None
        self.data_abnormal = None
        self.data_debug_tasks = None
        self.data_delivery_train = None
        self.data_industry_association = None
        self.data_project = None
        self.data_shr_staff = None
        self.data = None
        self.data_error = None
        self.data_process = None
        self.connection.close()