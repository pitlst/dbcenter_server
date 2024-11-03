import tqdm
import datetime
import pandas as pd
from general.base import db_process_base
from general.commont import year_month_days_num

tqdm.tqdm.pandas()


class human_efficiency_person_process(db_process_base):
    def __init__(self):
        super().__init__("人员效能处理-考勤部分", ["考勤数据清洗", "考勤节假日同步"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")
        
    def read(self):
        self.LOG.info("开始读取")
        self.data_kq = pd.read_sql_table("dwd_kq_time", self.connection)
        self.data_holiday = pd.read_sql_table("ods_kq_scheduling_holiday", self.connection)
        
        self.now = datetime.datetime.now()

    def write(self):
        self.LOG.info("开始写入")
        self.data_kq.to_sql(name="dm_kq_time", con=self.connection, index=False, if_exists='replace', chunksize=1000)

    def process(self):
        self.LOG.info("开始处理")
        
        self.LOG.debug("按需求进行聚合处理")
        self.data_kq = self.data_kq.loc[:, ["部门", "组室", "工号", "日期", "出勤时长", "加班时长", "生产时长", "因私出勤时长", "因公出勤时长"]]
        # 聚合同天多个考勤
        self.data_kq = pd.pivot_table(self.data_kq, index=["部门", "组室", "工号", "日期"], values=["出勤时长", "生产时长", "加班时长", "因私出勤时长", "因公出勤时长"], aggfunc="sum").reset_index()
        # 聚合月份
        def temp_apply(row):
            row["日期"] = datetime.datetime.strptime(str(row["日期"]), "%Y-%m-%d").strftime("%Y-%m")
            return row
        self.data_kq = self.data_kq.progress_apply(temp_apply, axis=1)
        self.data_kq = pd.pivot_table(self.data_kq, index=["部门", "组室", "工号", "日期"], values=["出勤时长", "生产时长", "加班时长", "因私出勤时长", "因公出勤时长"], aggfunc="sum").reset_index()
        # 聚合班组
        self.data_kq["总人数"] = 1
        self.data_kq = pd.pivot_table(self.data_kq, index=["部门", "组室", "日期"], values=["总人数", "出勤时长", "生产时长", "加班时长", "因私出勤时长", "因公出勤时长"], aggfunc="sum").reset_index()
        self.data_kq = self.data_kq.reindex(columns=self.data_kq.columns.to_list() + ["当月计划工作时长", "出勤饱和度"])
        self.data_kq["平均出勤"] = self.data_kq["出勤时长"] / self.data_kq["总人数"]

        self.LOG.debug("计算出勤饱和度")
        def temp_apply(row):
            row["当月计划工作时长"] = 0
            temp_date_0 = datetime.datetime.strptime(str(row["日期"]), "%Y-%m")
            # 先计算属于该月特殊节假日
            for _index, _row in self.data_holiday.iterrows():
                temp_date_1 = datetime.datetime.strptime(str(_row["节假日日期"]), '%Y-%m-%d %H:%M:%S')
                if temp_date_0.year == temp_date_1.year and temp_date_0.month == temp_date_1.month:
                    if int(_row["是否休息"]) == 0 and temp_date_1.weekday() in [5, 6]:
                        row["当月计划工作时长"] += 450
                    elif int(_row["是否休息"]) == 1 and temp_date_1.weekday() in [0,1,2,3,4]:
                        row["当月计划工作时长"] -= 450
            # 再计算理论上班时长
            year_month_days = year_month_days_num(temp_date_0.year, temp_date_0.month)
            end_date = datetime.datetime(year=temp_date_0.year, month=temp_date_0.month, day=year_month_days)
            start_date = end_date - datetime.timedelta(days=year_month_days)
            date_range = pd.date_range(start=start_date, end=end_date).tolist()
            for ch in date_range:
                if ch.weekday() in [0,1,2,3,4]:
                    row["当月计划工作时长"] += 450
            row["当月计划工作时长"] = row["当月计划工作时长"] * row["总人数"]
            # 计算饱和度
            row["出勤饱和度"] = 0.0
            if row["当月计划工作时长"] != 0:
                row["出勤饱和度"] = row["出勤时长"] / row["当月计划工作时长"]
            return row
        self.data_kq = self.data_kq.progress_apply(temp_apply, axis=1)
        
    def release(self) -> None:
        self.data_kq = None
        self.data_holiday = None
        self.connection.close()
        
        
class full_month_travel_calculation(db_process_base):
    def __init__(self):
        super().__init__("全月出差人员计算", ["差旅数据清洗"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.connection = self.temp_db.get(self.LOG,"数据仓库缓存_新")
        
    def read(self):
        self.LOG.info("开始读取")
        self.data_travel = pd.read_sql_table("dwd_travel", self.connection)
        
        self.data = pd.DataFrame(columns=["工号", "年月"])
        
    def write(self):
        self.LOG.info("开始写入")
        self.data.to_sql(name="dwd_full_month_travel", con=self.connection, index=False, if_exists='replace', chunksize=1000)
        
    def process(self):
        self.LOG.info("开始处理")
        def temp_apply(row):
            temp_range = pd.date_range(row["出发时间"], row["结束时间"], freq='MS').strftime("%Y-%m").tolist()
            if len(temp_range) > 2:
                temp_range = temp_range[1:len(temp_range)-1]
                for ch in temp_range:
                    self.data.loc[len(self.data.index)] = [row["员工编码"], ch]
        self.data_travel.progress_apply(temp_apply, axis=1)
        
    def release(self) -> None:
        self.data = None
        self.data_travel = None
        self.connection.close()
        
        
class human_efficiency_work_clean(db_process_base):
    def __init__(self):
        super().__init__("人员效能处理-工时清洗", ["考勤数据清洗", "人员数据清洗", "全月出差人员计算", "工时同步", "工时人员筛选同步", "定额外工时同步", "线下工时同步"])
        # 要求的不计算工时限制
        self.request_work_time = 20
        self.request_group = [
            "车门工位", 
            "电工一工位", 
            "电工二工位", 
            "电工三工位", 
            "电工四工位", 
            "电工五工位", 
            "管钳一工位", 
            "管钳二工位", 
            "管钳三工位", 
            "内装一工位", 
            "内装二工位", 
            "内装三工位", 
            "内装四工位",
            "设备工位", 
            "粘接一工位",
            "粘接二工位",
            "落车班",
            "调车班",
            "校线一班",
            "校线二班",
            "调试一班",
            "调试二班",
            "调试三班",
            "调试四班"]
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")

    def read(self):
        self.LOG.info("开始读取")
        self.data_kq_time = pd.read_sql_table("dwd_kq_time", self.connection)
        self.data_staff = pd.read_sql_table("dwd_staff", self.connection)
        self.data_travel_fillter = pd.read_sql_table("dwd_full_month_travel", self.connection)
        self.data_work_time = pd.read_sql_table("ods_work_time", self.connection)
        self.data_work_time_fillter = pd.read_sql_table("ods_work_time_fillter", self.connection)
        self.data_work_time_temp = pd.read_sql_table("ods_work_time_temp", self.connection)
        self.data_work_time_offline = pd.read_sql_table("ods_work_time_offline", self.connection)
        
    def write(self):
        self.LOG.info("开始写入")
        self.data.to_sql(name="dwd_work_time", con=self.connection, index=False, if_exists='replace', chunksize=1000)

    def process(self):
        self.LOG.info("开始处理")
        self.LOG.debug("处理定额外工时短工号问题")
        def temp_apply(row):
            if len(str(row["员工编码"])) <= 5:
                row["员工编码"] = "0102000" + str(row["员工编码"])
            return row
        self.data_work_time_temp = self.data_work_time_temp.progress_apply(temp_apply, axis=1)
        
        self.LOG.debug("拼接工时数据")
        self.data_work_time = self.data_work_time.fillna(value=0.0)
        self.data_work_time = self.data_work_time.loc[:, ["人员", "时间", "工时", "补报工时"]].rename(columns={'人员': '工号', '时间': '日期'})
        self.data_work_time_temp = self.data_work_time_temp.fillna(value=0.0)
        self.data_work_time_temp = self.data_work_time_temp.loc[:, ["员工编码", "作业日期", "批准工时"]].rename(columns={'员工编码': '工号', '作业日期': '日期', "批准工时": "定额外工时"})
        # 小时分钟转换
        self.data_work_time_temp["定额外工时"] = self.data_work_time_temp["定额外工时"] * 60
        self.data_work_time_offline = self.data_work_time_offline.fillna(value=0.0)
        self.data_work_time_offline = self.data_work_time_offline.loc[:, ["工号", "日期", "线下工时"]]
        self.data_kq_time = self.data_kq_time.loc[:, ["工号","日期", "生产时长"]]
        def temp_apply(row):
            # 去掉日的时分秒，便于聚合
            temp_date = datetime.datetime.strptime(str(row["日期"]), '%Y-%m-%d %H:%M:%S')
            row["日期"] = datetime.datetime(year=temp_date.year, month=temp_date.month, day=temp_date.day)
            return row
        self.data_work_time = self.data_work_time.progress_apply(temp_apply, axis=1)
        self.data_work_time_temp = self.data_work_time_temp.progress_apply(temp_apply, axis=1)
        self.data_work_time_offline = self.data_work_time_offline.progress_apply(temp_apply, axis=1)
        def temp_apply(row):
            # 处理数据类型，便于聚合
            row["日期"] = datetime.datetime.strptime(str(row["日期"]), '%Y-%m-%d')
            return row
        self.data_kq_time = self.data_kq_time.progress_apply(temp_apply, axis=1)
        
        self.data_work_time = pd.pivot_table(self.data_work_time, index=["工号", "日期"], values=["工时", "补报工时"], aggfunc="sum").reset_index()
        self.data_work_time_temp = pd.pivot_table(self.data_work_time_temp, index=["工号", "日期"], values=["定额外工时"], aggfunc="sum").reset_index()
        self.data = pd.merge(self.data_work_time, self.data_work_time_temp, how="outer", on=["工号", "日期"])
        self.data_work_time_offline = pd.pivot_table(self.data_work_time_offline, index=["工号", "日期"], values=["线下工时"], aggfunc="sum").reset_index()
        self.data = pd.merge(self.data, self.data_work_time_offline, how="outer", on=["工号", "日期"])
        self.data_kq_time = pd.pivot_table(self.data_kq_time, index=["工号", "日期"], values=["生产时长"], aggfunc="sum").reset_index()
        self.data = pd.merge(self.data, self.data_kq_time, how="left", on=["工号", "日期"])
        self.data = self.data.fillna(value=0.0)
        
        self.LOG.debug("聚合工时")
        def temp_apply(row):
            row["工时"] = float(row["工时"]) + float(row["补报工时"]) + float(row["定额外工时"]) + float(row["线下工时"])
            return row
        self.data = self.data.progress_apply(temp_apply, axis=1)
        self.data = self.data.loc[:, ["工号", "日期", "工时", "生产时长"]]
        
        self.LOG.debug("拼接人员数据")
        temp_xiangying = self.data_staff[["员工编码", "员工姓名", "一级组织名称", "二级组织名称", "末级组织名称"]]
        temp_xiangying.columns = ["工号", "姓名", "组织", "部门", "组室"]
        self.data = pd.merge(self.data, temp_xiangying, how="left", on=["工号"])
                
        self.LOG.debug("保留设定特殊人员")
        temp_retain = self.data_work_time_fillter.loc[self.data_work_time_fillter["是否加入计算"] == '1']
        temp_retain_data = pd.DataFrame(columns=self.data.columns.to_list())
        def temp_apply(row):
            for _index, _row in temp_retain.iterrows():
                if row["工号"] == _row["工号"] and row["日期"] == _row["日期"]:
                    temp_retain_data.loc[len(temp_retain_data.index)] = row
                    break
        self.data.progress_apply(temp_apply, axis=1)
        
        self.LOG.debug("按条件筛选")
        self.data = self.data.loc[(self.data["组织"] == "城轨事业部") & (self.data["组室"].isin(self.request_group))]
        self.data = self.data.loc[self.data["工时"] >= self.request_work_time]
        self.data = self.data.loc[self.data["生产时长"] != 0]
        
        self.LOG.debug("排除全月出差人员")
        def temp_apply(row):
            label = True
            temp_date = row["日期"].strftime("%Y-%m")
            for _index, _row in self.data_travel_fillter.iterrows():
                if row["工号"] == _row["工号"] and temp_date == _row["年月"]:
                    label = False
                    break
            return label
        self.data = self.data.loc[lambda x: x.progress_apply(temp_apply, axis=1), :].reset_index(drop=True)

        self.LOG.debug("排除设定特殊人员")
        temp_retain = self.data_work_time_fillter.loc[self.data_work_time_fillter["是否加入计算"] == '0']
        def temp_apply(row):
            label = True
            for _index, _row in temp_retain.iterrows():
                if row["工号"] == _row["工号"] and row["日期"] == _row["日期"]:
                    label = False
                    break
            return label
        self.data = self.data.loc[lambda x: x.progress_apply(temp_apply, axis=1), :].reset_index(drop=True)
        self.data = pd.concat([self.data, temp_retain_data], ignore_index=True)
        
    def release(self) -> None:
        self.data = None
        self.data_staff = None
        self.data_work_time = None
        self.data_work_time_fillter = None
        self.data_work_time_temp = None
        self.data_work_time_offline = None
        self.data_kq_time = None
        self.connection.close()
        
        
class human_efficiency_work_process(db_process_base):
    def __init__(self):
        super().__init__("人员效能处理-工时处理", ["人员效能处理-工时清洗"])
        # 均衡率达标要求
        self.request_balance = 0.8
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")

    def read(self):
        self.LOG.info("开始读取")
        self.data = pd.read_sql_table("dwd_work_time", self.connection)
        
    def write(self):
        self.LOG.info("开始写入")
        self.data.to_sql(name="dm_work_time", con=self.connection, index=False, if_exists='replace', chunksize=1000)
        self.data_group.to_sql(name="dm_work_time_group", con=self.connection, index=False, if_exists='replace', chunksize=1000)
        self.data_department.to_sql(name="dm_work_time_department", con=self.connection, index=False, if_exists='replace', chunksize=1000)

    def process(self):
        self.LOG.info("开始处理")
        
        def temp_apply(row):
            # 处理数据类型，便于聚合
            row["日期"] = row["日期"].strftime("%Y-%m")
            return row
        data_temp = self.data.progress_apply(temp_apply, axis=1)
        data_temp["总人数"] = 1
        
        self.LOG.debug("计算班组工时聚合指标")
        self.data_group = pd.pivot_table(data_temp, index=["部门", "组室", "日期"], values=["总人数",  "工时", "生产时长"], aggfunc="sum").reset_index()
        self.data_group = self.data_group.rename(columns={"日期": "年月", "部门":"车间", "组室":"班组", "工时":"总工时", "生产时长":"总生产"})
        self.data_group = self.data_group.reindex(columns = self.data_group.columns.tolist() + ["平均工时","平均生产",'复合完成率'])
        self.data_group["平均工时"] = self.data_group["总工时"] / self.data_group["总人数"]
        self.data_group["平均生产"] = self.data_group["总生产"] / self.data_group["总人数"]
        self.data_group["复合完成率"] = self.data_group["平均工时"] / self.data_group["平均生产"]
        
        self.LOG.debug("计算车间工时聚合指标")
        self.data_department = pd.pivot_table(data_temp, index=["部门", "日期"], values=["总人数",  "工时", "生产时长"], aggfunc="sum").reset_index()
        self.data_department = self.data_department.rename(columns={"日期": "年月", "部门":"车间", "工时":"总工时", "生产时长":"总生产"})
        self.data_department = self.data_department.reindex(columns = self.data_department.columns.tolist() + ["平均工时","平均生产",'复合完成率'])
        self.data_department["平均工时"] = self.data_department["总工时"] / self.data_department["总人数"]
        self.data_department["平均生产"] = self.data_department["总生产"] / self.data_department["总人数"]
        self.data_department["复合完成率"] = self.data_department["平均工时"] / self.data_department["平均生产"]
        
        self.LOG.debug("计算均衡率")
        self.data = self.data.reindex(columns = self.data.columns.tolist() + ["班组平均工时",'班组均衡率','班组均衡率是否达标',"车间平均工时",'车间均衡率','车间均衡率是否达标'])
        def temp_apply(row):
            temp_date = row["日期"].strftime("%Y-%m")
            for index_, row_ in self.data_group.iterrows():
                if row["部门"] == row_["车间"] and row["组室"] == row_["班组"] and temp_date == row_["年月"]:
                    row["班组平均工时"] = row_["平均工时"]
                    row["班组均衡率"] = row["工时"] / row["班组平均工时"]
                    if row["班组均衡率"] > self.request_balance:
                        row["班组均衡率是否达标"] = 1
                    else:
                        row["班组均衡率是否达标"] = 0
                    break
            for index_, row_ in self.data_department.iterrows():
                if row["部门"] == row_["车间"] and temp_date == row_["年月"]:
                    row["车间平均工时"] = row_["平均工时"]
                    row["车间均衡率"] = row["工时"] / row["车间平均工时"]
                    if row["车间均衡率"] > self.request_balance:
                        row["车间均衡率是否达标"] = 1
                    else:
                        row["车间均衡率是否达标"] = 0
                    break
            return row
        self.data = self.data.progress_apply(temp_apply, axis=1)
        
        self.LOG.debug("计算车间班组均衡人员占比")
        def temp_apply(row):
            # 处理数据类型，便于聚合
            row["日期"] = row["日期"].strftime("%Y-%m")
            return row
        data_temp = self.data.progress_apply(temp_apply, axis=1)
        self.data = pd.pivot_table(data_temp, index=[ "部门", "组室", "日期", "工号", "姓名"], values=["工时",  "生产时长"], aggfunc="sum").reset_index()
        
        data_temp["总人数"] = 1
        temp_group = pd.pivot_table(data_temp, index=["部门", "组室", "日期"], values=["总人数",  "班组均衡率是否达标"], aggfunc="sum").reset_index()
        temp_group = temp_group.rename(columns={"日期": "年月", "部门":"车间", "组室":"班组", "班组均衡率是否达标":"均衡率达标数"})
        temp_group = temp_group.reindex(columns = temp_group.columns.tolist() + ["均衡率达标占比"])
        temp_group["均衡率达标占比"] = temp_group["均衡率达标数"] / temp_group["总人数"]
        
        temp_department = pd.pivot_table(data_temp, index=["部门", "日期"], values=["总人数",  "车间均衡率是否达标"], aggfunc="sum").reset_index()
        temp_department = temp_department.rename(columns={"日期": "年月", "部门":"车间", "车间均衡率是否达标":"均衡率达标数"})
        temp_department = temp_department.reindex(columns = temp_department.columns.tolist() + ["均衡率达标占比"])
        temp_department["均衡率达标占比"] = temp_department["均衡率达标数"] / temp_department["总人数"]
        
        self.data_group = pd.merge(self.data_group, temp_group, how='left', on=["车间", "班组", "年月"])
        self.data_department = pd.merge(self.data_department, temp_department, how='left', on=["车间", "年月"])
        
        # 去掉一些无用的列
        self.data_group = self.data_group.loc[:, ["年月" ,"车间", "班组", "总工时", "总生产", "平均工时", "平均生产", "复合完成率", "均衡率达标占比"]]
        self.data_department = self.data_department.loc[:, ["年月" ,"车间", "总工时", "总生产", "平均工时", "平均生产", "复合完成率", "均衡率达标占比"]]
                
    def release(self) -> None:
        self.data = None
        self.data_group = None
        self.data_department = None
        self.connection.close()
        
        
class human_efficiency_labor_used_process(db_process_base):
    def __init__(self):
        super().__init__("人员效能处理-标杆产品用工量部分", ["标杆产品用工量同步"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")

    def read(self):
        self.LOG.info("开始读取")
        self.data = pd.read_sql_table("ods_product_labor", self.connection)
        
    def write(self):
        self.LOG.info("开始写入")
        self.data_month.to_sql(name="dm_product_labor_month", con=self.connection, index=False, if_exists='replace', chunksize=1000)
        self.data_quarter.to_sql(name="dm_product_labor_quarter", con=self.connection, index=False, if_exists='replace', chunksize=1000)
        self.data_year.to_sql(name="dm_product_labor_year", con=self.connection, index=False, if_exists='replace', chunksize=1000)

    def process(self):
        self.LOG.info("开始处理")
        temp_data = self.data.copy()
        temp_data["总成月度标杆产品用工量"] = temp_data["总成用工人数"] / temp_data["总成产量"]
        temp_data["交车月度标杆产品用工量"] = temp_data["交车用工人数"] / temp_data["交车产量"]
        self.data_month = temp_data.loc[:, ["年月", "总成月度标杆产品用工量", "交车月度标杆产品用工量", "总成车间目标值", "交车车间目标值"]]
        
        def temp_apply(row):
            temp_date = datetime.datetime.strptime(str(row["年月"]), '%Y-%m-%d %H:%M:%S')
            row["年月"] = str(temp_date.year) + '-' + str(int((int(temp_date.month) - 1) / 3) + 1)
            return row
        temp_data = self.data.progress_apply(temp_apply, axis=1)

        temp_data = pd.pivot_table(temp_data, index=["年月"], values=["总成用工人数", "交车用工人数", "总成产量", "交车产量"], aggfunc="sum").reset_index()
        temp_data["总成用工人数"] = temp_data["总成用工人数"] / 3
        temp_data["交车用工人数"] = temp_data["交车用工人数"] / 3
        temp_data["总成季度标杆产品用工量"] = temp_data["总成用工人数"] / temp_data["总成产量"]
        temp_data["交车季度标杆产品用工量"] = temp_data["交车用工人数"] / temp_data["交车产量"]
        self.data_quarter = temp_data.loc[:, ["年月", "总成季度标杆产品用工量", "交车季度标杆产品用工量"]]
        
        def temp_apply(row):
            row["年月"] = str(datetime.datetime.strptime(str(row["年月"]), '%Y-%m-%d %H:%M:%S').year)
            return row
        temp_data = self.data.progress_apply(temp_apply, axis=1)
        temp_data = pd.pivot_table(temp_data, index=["年月"], values=["总成用工人数",  "交车用工人数", "总成产量", "交车产量"], aggfunc="sum").reset_index()
        temp_data["总成用工人数"] = temp_data["总成用工人数"] / 12
        temp_data["交车用工人数"] = temp_data["交车用工人数"] / 12
        temp_data["总成年度标杆产品用工量"] = temp_data["总成用工人数"] / temp_data["总成产量"]
        temp_data["交车年度标杆产品用工量"] = temp_data["交车用工人数"] / temp_data["交车产量"]
        self.data_year = temp_data.loc[:, ["年月", "总成年度标杆产品用工量", "交车年度标杆产品用工量"]]
            
    def release(self) -> None:
        self.data = None
        self.data_month = None
        self.data_quarter = None
        self.data_year = None
        self.connection.close()