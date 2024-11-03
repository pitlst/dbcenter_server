import tqdm
import pandas as pd
import datetime
from general.base import db_process_base
from general.commont import change_character_code, year_month_days_num

tqdm.tqdm.pandas()
# 屏蔽警告
pd.options.mode.chained_assignment = None

class error_clean(db_process_base):
    def __init__(self) -> None:
        super().__init__("异常数据清洗", ["异常同步", "人员数据清洗", "项目数据清洗", "异常类型同步"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")
        
    def read(self) -> None:
        self.LOG.info("开始读取")
        self.data_abnormal = pd.read_sql_table("ods_abnormal", self.connection)
        self.data_abnormal_type = pd.read_sql_table("ods_abnormal_type", self.connection)
        self.data_staff = pd.read_sql_table("dwd_staff", self.connection)
        self.data_project = pd.read_sql_table("dwd_project_basic", self.connection)
    
    def write(self) -> None:
        self.LOG.info("开始写入")
        self.data_abnormal.to_sql(name="dwd_abnormal", con=self.connection, index=False, if_exists='replace', chunksize=1000)
    
    def process(self) -> None:
        self.LOG.info("开始处理")
        self.LOG.debug("拼接人员信息")
        temp_xiangying = self.data_staff[["员工编码", "员工姓名", "一级组织名称", "二级组织名称", "末级组织名称"]]
        temp_xiangying.columns = ["响应人", "响应人姓名", "响应人组织", "响应人部门", "响应人组室"]
        self.data_abnormal = pd.merge(self.data_abnormal, temp_xiangying, how="left", on="响应人")
        temp_xiangying.columns = ["指定响应人", "指定响应人姓名", "指定响应人组织", "指定响应人部门", "指定响应人组室"]
        self.data_abnormal = pd.merge(self.data_abnormal, temp_xiangying, how="left", on="指定响应人")
        temp_xiangying.columns = ["处理人", "处理人姓名", "处理人组织", "处理人部门", "处理人组室"]
        self.data_abnormal = pd.merge(self.data_abnormal, temp_xiangying, how="left", on="处理人")
        temp_xiangying.columns = ["发起人", "发起人姓名", "发起人组织", "发起人部门", "发起人组室"]
        self.data_abnormal = pd.merge(self.data_abnormal, temp_xiangying, how="left", on="发起人")

        self.LOG.debug("拼接异常类型")
        self.data_abnormal_type = self.data_abnormal_type.loc[:, ["异常类型编码", "类型属性名称", "类型属性编码"]]
        self.data_abnormal = pd.merge(self.data_abnormal, self.data_abnormal_type, how="left", on="异常类型编码")
        
        # 将项目相关的信息添加进来，注意项目基础数据不完全准确有重复，目前重复情况可以接受，控制在了10-100条左右
        self.LOG.debug("拼接项目信息")

        # 从车号中提取项目号
        def temp_apply(x):
            return str(x).strip()[:8]

        self.data_abnormal = pd.concat([self.data_abnormal, pd.DataFrame({"项目号": self.data_abnormal["车号"].apply(temp_apply)})], axis=1)
        self.data_project = self.data_project[self.data_project["使用状态"] == "可用"]
        self.data_project = self.data_project.drop_duplicates(subset=["项目号", "单据状态"], keep="first")
        self.data_project = self.data_project.loc[:, ["项目号", "项目名称", "项目简称", "项目启动年份", "节车号"]]
        self.data_project.columns = ["项目号", "项目名称", "项目简称", "项目启动年份", "项目所属节车号"]
        
        def temp_apply(row):
            if row["项目简称"] == "" or row["项目简称"] is None:
                if row["项目名称"] != "" or row["项目名称"] is not None:
                    row["项目简称"] = row["项目名称"]
                else:
                    row["项目简称"] = row["项目号"]
            return row

        self.data_project = self.data_project.progress_apply(temp_apply, axis=1)
        self.data_abnormal = pd.merge(self.data_abnormal, self.data_project, how="left", on="项目号")
        
    def release(self) -> None:
        self.data_abnormal = None
        self.data_abnormal_type = None
        self.data_staff = None
        self.data_project = None
        self.connection.close()
    

class error_process(db_process_base):
    def __init__(self):
        super().__init__("异常数据处理", ["异常数据清洗", "考勤节假日同步"])
        self.MAX_RESPONSE_TIME = 2 * 60 * 60  # 两小时
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")

    def read(self):
        self.LOG.info("开始读取")
        self.data_abnormal = pd.read_sql_table("dwd_abnormal", self.connection)
        self.data_holiday = pd.read_sql_table("ods_kq_scheduling_holiday", self.connection)

    def write(self):
        self.LOG.info("开始写入")
        self.deal_data.to_sql(name="dm_abnormal", con=self.connection, index=False, if_exists='replace', chunksize=1000)

    def process(self):
        self.LOG.info("开始处理")
        
        self.LOG.debug("删除校线异常")
        self.data_abnormal = self.data_abnormal[self.data_abnormal["类型属性名称"] != "校线异常"]
        
        self.LOG.debug("处理考勤系统乱码")
        def temp_apply(row):
            row["节假日名称"] = change_character_code(row["节假日名称"])
            return row
        self.data_holiday = self.data_holiday.progress_apply(temp_apply, axis=1)
        
        self.LOG.debug("计算响应时间")
        extend_list = ["响应状态", "响应时长", "是否及时响应", "是否处理"]
        self.data_abnormal = self.data_abnormal.reindex(columns=self.data_abnormal.columns.tolist() + extend_list)
        
        def temp_apply(row):
            start_date: datetime.datetime = row["发起日期"].to_pydatetime()
            if row["响应人"] is None:
                response_date = datetime.datetime.now()
            else:
                response_date = row["响应时间"].to_pydatetime()
            row["响应时长"] = self.compute_duration(start_date, response_date)
            
            if row["响应人部门"] == "质量技术部":
                temp_MAX_RESPONSE_TIME = self.MAX_RESPONSE_TIME * 2
            else:
                temp_MAX_RESPONSE_TIME = self.MAX_RESPONSE_TIME
            
            row["是否及时响应"] = 0
            if row["响应人"] is None and row["关闭人"] is not None:
                row["响应状态"] = "响应前关闭"
            else:
                if row["响应时长"] > temp_MAX_RESPONSE_TIME:
                    row["响应状态"] = "未及时响应"
                else:
                    if row["响应人"] is None:
                        row["响应状态"] = "未响应"
                    else:
                        row["响应状态"] = "及时响应"
                        row["是否及时响应"] = 1
                        
            if row["异常状态分类"] == "待处理":
                row["是否处理"] = 0
            else:
                row["是否处理"] = 1
            return row
        self.deal_data = self.data_abnormal.progress_apply(temp_apply, axis=1)
        
    def release(self) -> None:
        self.data_abnormal = None
        self.data_holiday = None
        self.deal_data = None
        self.connection.close()
        
    def compute_duration(self, start_date: datetime.datetime, end_date: datetime.datetime)-> int:
        # 计算两个时间内的响应时长，返回单位为秒
        # 不在同一年
        if start_date.year != end_date.year:
            temp_start = self.compute_duration(start_date, datetime.datetime(year=start_date.year, month=12, day=31, hour=23, minute=59, second=59))
            temp_end = self.compute_duration(datetime.datetime(year=end_date.year, month=1, day=1, hour=0, minute=0, second=1), end_date)
            if end_date.year - start_date.year > 1:
                temp_middle = self.compute_duration(datetime.datetime(year=start_date.year + 1, month=1, day=1, hour=0, minute=0, second=1), datetime.datetime(year=end_date.year-1, month=12, day=31, hour=23, minute=59, second=59))
            else:
                temp_middle = 0
            return temp_start + temp_end + temp_middle
        # 不在同一月
        elif start_date.month != end_date.month:
            temp_start = self.compute_duration(start_date, datetime.datetime(year=start_date.year, month=start_date.month, day=year_month_days_num(start_date.year, start_date.month), hour=23, minute=59, second=59))
            temp_end = self.compute_duration(datetime.datetime(year=end_date.year, month=end_date.month, day=1, hour=0, minute=0, second=1), end_date)
            if end_date.month - start_date.month > 1:
                temp_middle = self.compute_duration(datetime.datetime(year=start_date.year, month=start_date.month + 1, day=1, hour=0, minute=0, second=1), datetime.datetime(year=end_date.year, month=end_date.month - 1, day=year_month_days_num(end_date.year, end_date.month - 1), hour=23, minute=59, second=59))
            else:
                temp_middle = 0
            return temp_start + temp_end + temp_middle
        # 不在同一日
        elif start_date.day != end_date.day:
            temp_start = self.compute_ond_day_duration(start_date, datetime.datetime(year=start_date.year, month=start_date.month, day=start_date.day, hour=23, minute=59, second=59))
            temp_end = self.compute_ond_day_duration(datetime.datetime(year=end_date.year, month=end_date.month, day=end_date.day, hour=0, minute=0, second=1), end_date)
            if end_date.day - start_date.day > 1:
                temp_middle = self.compute_duration(datetime.datetime(year=start_date.year, month=start_date.month, day=start_date.day + 1, hour=0, minute=0, second=1), datetime.datetime(year=end_date.year, month=end_date.month, day=end_date.day - 1, hour=23, minute=59, second=59))
            else:
                temp_middle = 0
            return temp_start + temp_end + temp_middle
        # 同年同月同日
        else:
            return self.compute_ond_day_duration(start_date, end_date)
            
    def compute_ond_day_duration(self, start_date: datetime.datetime, end_date: datetime.datetime)-> int:
        # 计算在一天内的响应时长，返回单位为秒
        if not start_date.year == end_date.year or not start_date.month == end_date.month or not start_date.day == end_date.day:
            self.LOG.error("compute_ond_day_duration: 输入时间的日期不同")
            raise Exception("输入时间的日期不同")
        start_weekday = start_date.weekday()
        change_label = True
        temp_duration = 0
        # 如果有节假日等更改
        for index, row in self.data_holiday.iterrows():
            temp_date = datetime.datetime.strptime(str(row["节假日日期"]), "%Y-%m-%d %H:%M:%S") 
            temp_rest = row["是否休息"]
            # 找到同一天
            if temp_date.year == start_date.year and temp_date.month == start_date.month and temp_date.day == start_date.day:
                change_label = False
                # 休息日工作
                if int(temp_rest) == 0:
                    temp_duration = self._compute(start_date, end_date)
                # 工作日休息
                elif int(temp_rest) == 1:
                    temp_duration = 0
                else:
                    self.LOG.error("compute_ond_day_duration: 输入数据超出设计")
                    raise Exception("输入数据超出设计")
        # 如果没有节假日等更改，按照正常周一到周五上班，周六周日休息
        if change_label:
            if start_weekday in [0,1,2,3,4]:
                temp_duration = self._compute(start_date, end_date)
            elif start_weekday in [5,6]:
                temp_duration = 0
            else:
                self.LOG.error("compute_ond_day_duration: 输入数据超出设计")
                raise Exception("输入数据超出设计")
        return temp_duration
    
    @staticmethod
    def _compute(start_date: datetime.datetime, end_date: datetime.datetime) -> int:
        # 计算排除了排班表影响之后的一天内的响应时长，返回单位为秒
        # 早8点半到中午12点，下午1点半到晚上5点半
        up_start = datetime.datetime(year=1900, month=1, day=1, hour=8, minute=30)
        up_end = datetime.datetime(year=1900, month=1, day=1, hour=12)
        down_start = datetime.datetime(year=1900, month=1, day=1, hour=13, minute=30)
        down_end = datetime.datetime(year=1900, month=1, day=1, hour=17, minute=30)
        start_time = datetime.datetime(year=1900, month=1, day=1, hour=start_date.hour, minute=start_date.minute, second=start_date.second)
        end_time = datetime.datetime(year=1900, month=1, day=1, hour=end_date.hour, minute=end_date.minute, second=end_date.second)
        temp_duration: datetime.timedelta = None
        # 处理上午
        if start_time < up_start:
            temp_start = up_start
        elif start_time >= up_start and start_time < up_end:
            temp_start = start_time
        else:
            temp_start = up_end
        if end_time < up_start:
            temp_end = up_start
        elif end_time >= up_start and end_time < up_end:
            temp_end = end_time
        else:
            temp_end = up_end
        temp_duration = temp_end - temp_start
        # 处理下午
        if start_time < down_start:
            temp_start = down_start
        elif start_time >= down_start and start_time < down_end:
            temp_start = start_time
        else:
            temp_start = down_end
        if end_time < down_start:
            temp_end = down_start
        elif end_time >= down_start and end_time < down_end:
            temp_end = end_time
        else:
            temp_end = down_end
        temp_duration += temp_end - temp_start
        return temp_duration.seconds
    

class error_export_process(db_process_base):
    def __init__(self):
        super().__init__("异常月报导出", ["异常数据处理", "项目数据清洗"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")
        
    def read(self):
        self.LOG.info("开始读取")
        self.data = pd.read_sql_table("dm_abnormal", self.connection)
        self.data_project_base = pd.read_sql_table("dwd_project_basic", self.connection)
        
        self.now = datetime.datetime.now()
        self.data_project = pd.DataFrame(columns=["项目号", "及时响应数量", "未及时响应数量", "总计", "响应及时率"])
        self.data_department = pd.DataFrame(columns=["组室名称", "及时响应数量", "未及时响应数量", "总计", "响应及时率", "改善目标", "未及时响应责任人/数量", "原因分析", "整改措施", "备注"])

    def write(self):
        self.LOG.info("开始写入")
        self.data_project.to_sql(name="dm_abnormal_zl_project", con=self.connection, index=False, if_exists='replace', chunksize=1000)
        self.data_department.to_sql(name="dm_abnormal_zl_department", con=self.connection, index=False, if_exists='replace', chunksize=1000)

    def process(self):
        self.LOG.info("开始处理")
        
        department_list = ["电气工程设计组", "电气组", "调试组", "过程质量组", "机械组", "交付质量组", "内装组", "粘接组", "质量保证组", "总体技术组", "检查班"]
        
        self.data_project["项目号"] = self.data["项目号"].unique()
        self.data_project = self.data_project.fillna(value=0.0)
        def temp_apply(row):
            for _index, _row in self.data.iterrows():
                if _row["发起日期"] >= datetime.datetime(self.now.year, self.now.month-1, 1) and _row["发起日期"] < datetime.datetime(self.now.year, self.now.month, 1) and _row["响应人组室"] in department_list:
                    if row["项目号"] == _row["项目号"]:
                        if _row["响应状态"] == "及时响应":
                            row["及时响应数量"] += 1
                        elif _row["响应状态"] == "未及时响应":
                            row["未及时响应数量"] += 1
            row["总计"] = row["及时响应数量"] + row["未及时响应数量"]
            if row["总计"] != 0:
                row["响应及时率"] =  row["及时响应数量"] / row["总计"]
            else:
                row["响应及时率"] = 1.0
            return row
        self.data_project = self.data_project.progress_apply(temp_apply, axis=1)
        self.data_project = pd.merge(self.data_project, self.data_project_base.loc[:, ["项目号", "项目名称", "项目简称"]], how='left', on="项目号")
        
        department_list.extend(["总计"])
        self.data_department["组室名称"] = department_list
        self.data_department = self.data_department.fillna(value=0.0)
        def temp_apply(row):
            for _index, _row in self.data.iterrows():
                if _row["发起日期"] >= datetime.datetime(self.now.year, self.now.month-1, 1) and _row["发起日期"] < datetime.datetime(self.now.year, self.now.month, 1):
                    if row["组室名称"] == _row["响应人组室"]:
                        if _row["响应状态"] == "及时响应":
                            row["及时响应数量"] += 1
                        elif _row["响应状态"] == "未及时响应":
                            row["未及时响应数量"] += 1
            row["总计"] = row["及时响应数量"] + row["未及时响应数量"]
            if row["总计"] != 0:
                row["响应及时率"] =  row["及时响应数量"] / row["总计"]
            else:
                row["响应及时率"] = 1.0
            return row
        self.data_department = self.data_department.progress_apply(temp_apply, axis=1)
        
        self.data_department.loc[len(self.data_department.index)-1, "及时响应数量"] = self.data_department["及时响应数量"].sum()
        self.data_department.loc[len(self.data_department.index)-1, "未及时响应数量"] = self.data_department["未及时响应数量"].sum()
        self.data_department.loc[len(self.data_department.index)-1, "总计"] = self.data_department["总计"].sum()
        if self.data_department.loc[len(self.data_department.index)-1, "总计"] != 0:
            self.data_department.loc[len(self.data_department.index)-1, "响应及时率"] = self.data_department.loc[len(self.data_department.index)-1, "及时响应数量"] / self.data_department.loc[len(self.data_department.index)-1, "总计"]
        else:
            self.data_department.loc[len(self.data_department.index)-1, "响应及时率"] = 1.0
            
    def release(self) -> None:
        self.data = None
        self.data_project = None
        self.data_department = None
        self.data_project_base = None
        self.connection.close()