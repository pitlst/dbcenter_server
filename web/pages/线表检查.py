import streamlit as st
import datetime
import os
import pandas as pd
from general.commont import get_file

def process(request_dataframe: pd.DataFrame, dataframe: pd.DataFrame) -> pd.DataFrame:
    no_have_dataframe = request_dataframe[request_dataframe["原理线束"]=="不选"].loc[:, ["起指称", "起点"]]
    have_dataframe = request_dataframe[request_dataframe["原理线束"]!="不选"].loc[:, ["起指称", "起点"]]

    # 处理不能有的列表
    no_have_list = []
    def temp_apply(row):
        temp_qidian = str(row["起点"]) if pd.notna(row["起点"]) and pd.notnull(row["起点"]) else ""
        temp_lianjiedian = str(row["起指称"]) if str(row["起指称"])[0] == "=" else "=" + str(row["起指称"])
        no_have_list.append(temp_qidian + temp_lianjiedian)
    no_have_dataframe.apply(temp_apply, axis=1)
    # 处理必须有的列表
    have_list_temp = []
    def temp_apply(row):
        temp_qidian = str(row["点位1"]) if pd.notna(row["点位1"]) and pd.notnull(row["点位1"]) else ""
        temp_lianjiedian = str(row["连接点1"]) if str(row["连接点1"])[0] == "=" else "=" + str(row["连接点1"])
        have_list_temp.append(temp_qidian + temp_lianjiedian)
        temp_qidian = str(row["点位2"]) if pd.notna(row["点位2"]) and pd.notnull(row["点位2"]) else ""
        temp_lianjiedian = str(row["连接点2"]) if str(row["连接点2"])[0] == "=" else "=" + str(row["连接点2"])
        have_list_temp.append(temp_qidian + temp_lianjiedian)
    dataframe.apply(temp_apply, axis=1)

    one_img_info = pd.DataFrame()

    for index, row in dataframe.iterrows():
        temp_qidian = str(row["点位1"]) if pd.notna(row["点位1"]) and pd.notnull(row["点位1"]) else ""
        temp_lianjiedian = str(row["连接点1"]) if str(row["连接点1"])[0] == "=" else "=" + str(row["连接点1"])
        temp_str = temp_qidian + temp_lianjiedian
        if temp_str in no_have_list:
            len_inedx = len(one_img_info.index)
            one_img_info.loc[len_inedx, "错误原因"] = "出现了不应当出现的点位"
            one_img_info.loc[len_inedx, "点位"] = "'" + temp_qidian
            one_img_info.loc[len_inedx, "连接点"] = "'" + temp_lianjiedian
            one_img_info.loc[len_inedx, "对应的总表行数"] = str(index + 1)
        temp_qidian = str(row["点位2"]) if pd.notna(row["点位2"]) and pd.notnull(row["点位2"]) else ""
        temp_lianjiedian = str(row["连接点2"]) if str(row["连接点2"])[0] == "=" else "=" + str(row["连接点2"])
        temp_str = temp_qidian + temp_lianjiedian
        if temp_str in no_have_list:
            len_inedx = len(one_img_info.index)
            one_img_info.loc[len_inedx, "错误原因"] = "出现了不应当出现的点位"
            one_img_info.loc[len_inedx, "点位"] = "'" + temp_qidian
            one_img_info.loc[len_inedx, "连接点"] = "'" + temp_lianjiedian
            one_img_info.loc[len_inedx, "对应的总表行数"] = str(index + 1)

    for index, row in have_dataframe.iterrows():
        if pd.isna(row["起指称"]) or pd.isnull(row["起指称"]):
            continue
        temp_qidian = str(row["起点"]) if pd.notna(row["起点"]) and pd.notnull(row["起点"]) else ""
        temp_lianjiedian = str(row["起指称"]) if str(row["起指称"])[0] == "=" else "=" + str(row["起指称"])
        temp_str = temp_qidian + temp_lianjiedian
        if temp_str not in have_list_temp:
            len_inedx = len(one_img_info.index)
            one_img_info.loc[len_inedx, "错误原因"] = "应当出现的点位没出现"
            one_img_info.loc[len_inedx, "点位"] = "'" + temp_qidian
            one_img_info.loc[len_inedx, "连接点"] = "'" + temp_lianjiedian
            one_img_info.loc[len_inedx, "对应的总表行数"] = "-1"
    one_img_info = one_img_info.astype("str")
    one_img_info = one_img_info.drop_duplicates()
    return one_img_info

st.title("线标检查")
st.markdown("这里上传线束模块的需求表")
uploaded_file_request = st.file_uploader("需求表",label_visibility="collapsed")
st.markdown("这里上传需要被检查的线表")
uploaded_file = st.file_uploader("线表",label_visibility="collapsed")

if st.button("开始检查"):
    with st.spinner('请稍等......正在处理......'):
        if not uploaded_file_request is None and not uploaded_file is None:
            df1=pd.read_excel(uploaded_file_request, sheet_name=0, skiprows=1, dtype=str)
            df2=pd.read_excel(uploaded_file, sheet_name=0, dtype=str)
            temp_dataframe = process(df1, df2)
            now_str = datetime.datetime.now().strftime("%Y-%d-%m--%H-%M-%S")
            path = os.path.join(r"C:\Users\Administrator\Documents\dbcenter_dag_process\dag\file\file\线标检查结果缓存", now_str+".xlsx")
            temp_dataframe.to_excel(path, sheet_name="sheet1",index=False)
            data = get_file(path).getvalue()
            st.download_button('下载检查出来的文件原因', data=data, file_name = now_str+".xlsx", mime="application/vnd.ms-excel")
        else:
            st.error("检测不到上传的两个文件，请刷新页面后重新上传")