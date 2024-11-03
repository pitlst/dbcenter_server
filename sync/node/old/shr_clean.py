import tqdm
import pandas as pd
from general.base import db_process_base
from general.commont import change_character_code

tqdm.tqdm.pandas()


class shr_clean(db_process_base):
    def __init__(self):
        super().__init__("人员数据清洗", ["shr人员信息同步", "考勤人员信息同步", "mes在岗情况同步"])
        
    def connect(self) -> None:
        self.LOG.info("开始连接")
        self.connection = self.temp_db.get(self.LOG, "数据仓库缓存_新")

    def read(self):
        self.LOG.info("开始读取")
        self.shr_data = pd.read_sql_table("ods_shr_staff", self.connection)
        self.kq_data = pd.read_sql_table("ods_kq_staff", self.connection)
        self.mes_data = pd.read_sql_table("ods_mes_staff", self.connection)
        
    def write(self):
        self.LOG.info("开始写入")
        self.shr_data.to_sql(name="dwd_staff", con=self.connection, index=False, if_exists='replace', chunksize=1000)

    def process(self):
        self.LOG.info("开始处理")
        temp_set = set()
        new_data = pd.DataFrame(columns=self.shr_data.columns.to_list())

        def temp_apply(row):
            if row["员工编码"] not in temp_set:
                temp_set.add(row["员工编码"])
                new_data.loc[len(new_data.index)] = row
            else:
                if new_data.loc[new_data["员工编码"] == row["员工编码"], "fid"].iloc[-1] < row["fid"]:
                    # 正常仅能查出唯一一项
                    new_data[new_data["员工编码"] == row["员工编码"]] = row

        self.shr_data.progress_apply(temp_apply, axis=1)
        self.shr_data = new_data
        
        self.LOG.debug("拼接数据")
        def temp_apply(row):
            row["姓名"] = change_character_code(row["姓名"])
            row["末级组织名称"] = change_character_code(row["末级组织名称"])
            row["四级组织名称"] = change_character_code(row["四级组织名称"])
            row["三级组织名称"] = change_character_code(row["三级组织名称"])
            row["二级组织名称"] = change_character_code(row["二级组织名称"])
            row["一级组织名称"] = change_character_code(row["一级组织名称"])
            return row
        
        self.kq_data = self.kq_data.progress_apply(temp_apply, axis=1)
        self.shr_data = pd.merge(self.shr_data, self.kq_data.loc[:, ["工号", "id","雇佣开始日期","进入公司日期","雇佣结束日期","实习转正日期","结婚日期"]], how="outer", left_on="员工编码", right_on="工号")
        self.shr_data["考勤系统id"] = self.shr_data["id"]
        temp_list = self.shr_data.columns.to_list()
        temp_list.remove("id")
        self.shr_data = self.shr_data.loc[:, temp_list]
        self.shr_data = pd.merge(self.shr_data, self.mes_data.loc[:, ["工号", "gid","在岗情况"]], how="outer", left_on="员工编码", right_on="工号")
        self.shr_data["mes_id"] = self.shr_data["gid"]
        temp_list = self.shr_data.columns.to_list()
        temp_list.remove("gid")
        temp_list.remove("工号_x")
        temp_list.remove("工号_y")
        self.shr_data = self.shr_data.loc[:, temp_list]
        
        self.LOG.debug("填补并集空白")
        def temp_apply(row):
            if pd.isnull(row["fid"]) or pd.isna(row["fid"]):
                if pd.notnull(row["考勤系统id"]) and pd.notna(row["考勤系统id"]):
                    temp_data = self.kq_data[self.kq_data["id"] == row["考勤系统id"]].iloc[0]
                    row["员工编码"] = temp_data["工号"]
                    row["员工姓名"] = temp_data["姓名"]
                    row["一级组织名称"] = str(temp_data["三级组织名称"]) if pd.notnull(temp_data["三级组织名称"]) and pd.notna(temp_data["三级组织名称"]) and not temp_data["三级组织名称"] is None and str(temp_data["三级组织名称"]) != "None" else ""
                    row["二级组织名称"] = str(temp_data["四级组织名称"]) if pd.notnull(temp_data["四级组织名称"]) and pd.notna(temp_data["四级组织名称"]) and not temp_data["四级组织名称"] is None and str(temp_data["四级组织名称"]) != "None" else ""
                    row["末级组织名称"] = str(temp_data["末级组织名称"]) if pd.notnull(temp_data["末级组织名称"]) and pd.notna(temp_data["末级组织名称"]) and temp_data["末级组织名称"] is not None and str(temp_data["末级组织名称"]) != "None" else ""
                    row["完整组织名称"] = ((str(temp_data["一级组织名称"]) + "_") if pd.notnull(temp_data["一级组织名称"]) and pd.notna(temp_data["一级组织名称"]) and not temp_data["一级组织名称"] is None and str(temp_data["一级组织名称"]) != "None" else "") \
                        + ((str(temp_data["二级组织名称"]) + "_") if pd.notnull(temp_data["二级组织名称"]) and pd.notna(temp_data["二级组织名称"]) and not temp_data["二级组织名称"] is None and str(temp_data["二级组织名称"]) != "None" else "") \
                        + ((str(temp_data["三级组织名称"]) + "_") if pd.notnull(temp_data["三级组织名称"]) and pd.notna(temp_data["三级组织名称"]) and not temp_data["三级组织名称"] is None and str(temp_data["三级组织名称"]) != "None" else "") \
                        + ((str(temp_data["四级组织名称"]) + "_") if pd.notnull(temp_data["四级组织名称"]) and pd.notna(temp_data["四级组织名称"]) and not temp_data["四级组织名称"] is None and str(temp_data["四级组织名称"]) != "None" else "") \
                        + (str(temp_data["末级组织名称"]) if pd.notnull(temp_data["末级组织名称"]) and pd.notna(temp_data["末级组织名称"]) and temp_data["末级组织名称"] is not None and str(temp_data["末级组织名称"]) != "None" else "")
                elif pd.notnull(row["mes_id"]) and pd.notna(row["mes_id"]):
                    temp_data = self.mes_data.loc[self.mes_data["gid"] == row["mes_id"]].iloc[0]
                    row["员工编码"] = temp_data["工号"]
                    row["员工姓名"] = temp_data["姓名"]
                    row["末级组织名称"] = temp_data["班组名称"]
            return row
        self.shr_data = self.shr_data.progress_apply(temp_apply, axis=1)
    
    def release(self) -> None:
        self.shr_data = None
        self.kq_data = None
        self.mes_data = None
        self.connection.close()