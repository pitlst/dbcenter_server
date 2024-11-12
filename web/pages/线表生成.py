import pandas as pd
import warnings
import streamlit as st
import datetime
import os
from general import get_file

warnings.simplefilter(action='ignore', category=FutureWarning)

technology_copy_columns = [
    "备注", 
    "出口1", 
    "出口2", 
    "点位1", 
    "点位2",
    "接线头1/MVB剪桥",
    "接线头2/MVB剪桥",
    "类型",
    "连接点1",
    "连接点2",
    "路径11",
    "路径12",
    "路径13",
    "路径14",
    "路径21",
    "路径22",
    "路径23",
    "路径24",
    "起始位置",
    "数量",
    "说明1",
    "说明2",
    "物资编码",
    "线槽1",
    "线槽2",
    "线号",
    "线束号",
    "线长",
    "颜色",
    "预留1",
    "预留2",
    "原理图",
    "终止位置"]
technology_columns = [
    "备注", 
    "变更记录", 
    "补充备注", 
    "布线班组", 
    "起始彩色标签", 
    "终止彩色标签", 
    "车型", 
    "出口1", 
    "出口2", 
    "大/小", 
    "单/多", 
    "点位1", 
    "点位2",
    "电压等级",
    "接线班组1",
    "接线班组2",
    "接线头1/MVB剪桥",
    "接线头2/MVB剪桥",
    "类型",
    "连接点1",
    "连接点2",
    "路径11",
    "路径12",
    "路径13",
    "路径14",
    "路径21",
    "路径22",
    "路径23",
    "路径24",
    "起始位置",
    "数量",
    "说明1",
    "说明2",
    "物资编码",
    "下线线号",
    "下线线号型号",
    "线槽1",
    "线槽2",
    "线号",
    "线号大小",
    "线号类型",
    "线号下标1",
    "线号下标2",
    "线号颜色",
    "线径",
    "线束号",
    "线束号下标1",
    "线束号下标2",
    "线长",
    "序号",
    "序号1",
    "序号2",
    "压接档位/压模",
    "压线钳类型",
    "颜色",
    "预留1",
    "预留2",
    "原理图",
    "终止位置"]
color_index_list = ["粉色", "绿色", "蓝色" , "红色" ,"黄色", "湖绿色", "浅粉色", "荧黄色"]
placement_list_copy_columns_1 = [
    "接线班组1",
    "说明1",
    "起始位置",
    "连接点1",
    "点位1",
    "接线头1/MVB剪桥",
    "线号",
    "类型",
    "颜色",
    "线束号",
    "终止位置",
    "说明2",
    "连接点2",
    "点位2",
    "备注",
    "序号1"
]
placement_list_copy_columns_2 = [
    "接线班组2",
    "说明2",
    "终止位置",
    "连接点2",
    "点位2",
    "接线头2/MVB剪桥",
    "线号",
    "类型",
    "颜色",
    "线束号",
    "起始位置",
    "说明1",
    "连接点1",
    "点位1",
    "备注",
    "序号2"
]
placement_list_rename_colunms_1 = {
    "接线班组1":"接线班组",
    "接线头1/MVB剪桥":"接线头/MVB剪桥",
    "序号1":"序号"
}
placement_list_rename_colunms_2 = {
    "接线班组2":"接线班组",
    "说明2":"说明1",
    "终止位置":"起始位置",
    "连接点2":"连接点1",
    "接线班组2":"接线班组",
    "接线头2/MVB剪桥":"接线头/MVB剪桥",
    "起始位置":"终止位置",
    "说明1":"说明2",
    "连接点1":"连接点2",
    "序号2":"序号"
}
down_list_columns = [
    "备注", 
    "变更记录", 
    "补充备注", 
    "布线班组", 
    "起始彩色标签", 
    "终止彩色标签", 
    "车型", 
    "出口1", 
    "出口2", 
    "大/小", 
    "单/多", 
    "点位1", 
    "点位2",
    "电压等级",
    "接线班组1",
    "接线班组2",
    "接线头1/MVB剪桥",
    "接线头2/MVB剪桥",
    "类型",
    "连接点1",
    "连接点2",
    "路径11",
    "路径12",
    "路径13",
    "路径14",
    "路径21",
    "路径22",
    "路径23",
    "路径24",
    "起始位置",
    "数量",
    "说明1",
    "说明2",
    "物资编码",
    "下线线号",
    "下线线号型号",
    "线号",
    "线号大小",
    "线号类型",
    "线号下标1",
    "线号下标2",
    "线号颜色",
    "线径",
    "线束号",
    "线束号下标1",
    "线束号下标2",
    "线长",
    "序号",
    "序号1",
    "序号2",
    "颜色",
    "预留1",
    "预留2",
    "原理图",
    "终止位置"]
wire_list_columns = [
    "备注", 
    "布线班组", 
    "出口1", 
    "出口2",
    "连接点1",
    "连接点2",
    "路径11",
    "路径12",
    "路径13",
    "路径14",
    "路径21",
    "路径22",
    "路径23",
    "路径24",
    "起始位置",
    "数量",
    "线槽1",
    "线槽2",
    "线号",
    "线束号",
    "线长",
    "序号",
    "预留1",
    "预留2",
    "终止位置",
    "单/多"]
crimp_recording_colunms = [
    "备注",
    "操作人员/日期",
    "工具编号",
    "连接点1",
    "连接点2",
    "起始位置",
    "说明",
    "物资编码",
    "线径",
    "压接档位/压模",
    "压线钳类型",
    "自检情况"
]
crimp_recording_copy_colunms = [
    "连接点1",
    "连接点2",
    "起始位置",
    "物资编码",
    "线径",
    "压接档位/压模",
    "压线钳类型"
]


def process_position(position: float) -> str:
    '''单独处理位置号的加号'''
    return "+" + str(int(position))

def process_connect_point(connect_point: str) -> str:
    '''防止连接点被识别为公式被执行'''
    return "'" + connect_point

def process_nan(input_str) -> str:
    '''解决空值转字符串后出现的nan'''
    return str(input_str) if pd.notna(input_str) and pd.notnull(input_str) and str(input_str) != "nan" else ""

def get_wire_number(wire_gauge: str) -> str:
    '''根据线规获取线号'''
    wire_gauge_int = float(wire_gauge)
    if len(wire_gauge) > 3 and wire_gauge[:3] == "AWG":
        return "4.8"
    elif wire_gauge_int <= 2.5:
        return "4.8"
    elif wire_gauge_int >= 4 and wire_gauge_int <= 6:
        return "6.4"
    elif wire_gauge_int >= 10 and wire_gauge_int <= 16:
        return "9.5"
    elif wire_gauge_int >= 30 and wire_gauge_int <= 50:
        return "19.0"
    elif wire_gauge_int >= 70 and wire_gauge_int <= 120:
        return "25.4"
    elif wire_gauge_int == 240:
        return "38.1"
    else:
        return ""

def get_wiring_team(wire_raceway: str, start_position: str, end_position: str, wire_length: str, remark: str) -> str:
    '''获取布线班组'''
    assert len(start_position) != 0 and not start_position is None
    assert len(end_position) != 0 and not end_position is None

    if wire_raceway in ["RRA", "RRB", "RRC", "RLA", "RLB", "RLC"]:
        return "电五-PD0101"
    elif wire_raceway in ["UFH", "UFA", "UFLB", "UFLC"]:
        return "电五-PD0102"
    elif int(start_position[2:3]) < 2 or int(end_position[2:3]) < 2:
        return "电五-PE03"
    elif int(float(wire_length)) == 0 and remark.find("成品") == -1:
        return "/"
    elif int(start_position[2:3]) == 7 or int(end_position[2:3]) == 7:
        return "电五-P02"
    else:
        return "电五-P05"

def get_voltage_level(harness_number: str) -> str:
    '''获取电压等级'''  
    temp = ''.join([x for x in harness_number if x.isalpha()])
    return temp[0] if len(temp) > 0 else ""
    
def get_line_color(voltage_level: str) -> str:
    '''获取线号颜色'''
    if voltage_level == "H":
        return "红"
    elif voltage_level == "A":
        return "白"
    elif voltage_level == "B":
        return "黄"
    elif voltage_level == "C":
        return "黄"
    else:
        return ""

def get_connecting_team(start_position: str, end_position: str, explain_1: str, explain_2: str, ) -> dict[str, str]:
    '''获取接线班组'''
    start_position = float(start_position[1:])
    end_position = float(end_position[1:])
    temp = {}
    def get_built_in(explain: str, position: float) -> str|None:
        '''判断是否为内装工位拼接并生成'''
        label = False
        request_list = ["红橙灯", "检修灯", "座椅温度传感器"]
        for ch in request_list:
            if explain.find(ch) != -1:
                label = True
                break
        if label:
            if position >= 100 and position < 200:
                return "内装一工位"
            elif position >= 200 and position < 300:
                return "内装二工位"
            elif position >= 200 and position < 300:
                return "内装三工位"
            else:
                return None

    def get_connecting_team_end(position: float) -> str:
        '''获取一端的接线班组'''
        if (position >= 171 and position <= 179) or \
            (position >= 102 and position <= 104):
            return "电一-05"
        elif (position >= 121 and position <= 124) or \
            (position >= 131 and position <= 134) or \
            (position >= 141 and position <= 145) or \
            (position >= 151 and position <= 155) or \
            (position >= 161 and position <= 164) or \
            position in [188, 189]:
            return "电一-06上"
        elif position in [198, 199]:
            return "P06-下"
        elif (position >= 271 and position <= 279) or \
            (position >= 201 and position <= 204):
            return "电二-05"
        elif (position >= 221 and position <= 224) or \
            (position >= 231 and position <= 234) or \
            (position >= 241 and position <= 245) or \
            (position >= 251 and position <= 255) or \
            (position >= 261 and position <= 264) or \
            position in [281, 282, 288, 289]:
            return "电二-P06上"
        elif position in [296, 297, 298, 299]:
            return "电二-P06下"
        elif (position >= 371 and position <= 379) or \
            (position >= 301 and position <= 304):
            return "电三-P05"
        elif (position >= 321 and position <= 324) or \
            (position >= 331 and position <= 334) or \
            (position >= 341 and position <= 345) or \
            (position >= 351 and position <= 355) or \
            (position >= 361 and position <= 364) or \
            position in [388, 389, 381, 382]:
            return "电三-P06上"
        elif position in [396, 397, 398, 399]:
            return "电三-P06下"
        elif position in [101, 111, 112, 113, 114, 115, 116, 117, 118, 119]:
            return "电四"

    # 内装接线
    temp_group_1 = get_built_in(explain_1, start_position)
    if not temp_group_1 is None:
        temp["接线班组1"] = temp_group_1
    temp_group_2 = get_built_in(explain_2, end_position)
    if not temp_group_2 is None:
        temp["接线班组2"] = temp_group_2
    # 检查并提前退出
    if "接线班组1" in temp.keys() and "接线班组2" in temp.keys():
        return temp
    # 落车接线
    if (start_position >= 90 and start_position <= 95) or (end_position >= 90 and end_position <= 95):
        if "接线班组1" not in temp.keys():
            temp["接线班组1"] = "落车班"
        if "接线班组1" not in temp.keys():
            temp["接线班组2"] = "落车班"
    # 检查并提前退出
    if "接线班组1" in temp.keys() and "接线班组2" in temp.keys():
        return temp
    # 正常接线
    temp["接线班组1"] = get_connecting_team_end(start_position)
    temp["接线班组2"] = get_connecting_team_end(end_position)
    return temp

def get_upload_wire_number(monomany: str, wire_number: str, harness_number: str) -> str:
    '''获取下线线号'''
    if monomany == "单":
        return wire_number
    elif monomany == "多":
        return harness_number
    else:
        return ""

def get_upload_wire_set(monomany: str, wire_number_type: str) -> str:
    '''获取下线线号型号'''
    if monomany == "单":
        return wire_number_type
    elif monomany == "多":
        return "12.7黄"
    else:
        return ""

def process_table_generation(dataframe_design: pd.DataFrame, dataframe_line: pd.DataFrame, dataframe_plug: pd.DataFrame)-> dict[str, pd.DataFrame]:
    temp_excel = {}
    # ------------------------------ 处理工艺总表 ------------------------------
    index = 0
    def temp_apply(row: pd.Series) -> pd.Series:
        nonlocal index
        index += 1
        temp = pd.Series(index=technology_columns, dtype=str)
        # 处理单纯拷贝的列
        temp["序号1"] = str(index)
        temp["序号2"] = str(index) + ".1"
        for ch in technology_copy_columns:
            temp[ch] = process_nan(row[ch])
        # 特殊处理位置号
        temp["起始位置"] = process_position(float(temp["起始位置"]))
        temp["终止位置"] = process_position(float(temp["终止位置"]))
        # 处理在线缆数据中查询的列
        dataframe_line["物资编码"] = dataframe_line["物资编码"].astype(str)
        temp_line = dataframe_line[dataframe_line["物资编码"] == temp["物资编码"]]
        if len(temp_line) > 0:
            temp_line = temp_line.iloc[0]
            temp["大/小"] = temp_line["大/小"]
            temp["单/多"] = temp_line["单/多"]
            temp["线径"] = temp_line["线径"]
            temp["线号大小"] = get_wire_number(str(temp_line["线芯线规"]))
        # 处理在插芯数据中查询的列
        dataframe_plug["物资编码"] = dataframe_plug["物资编码"].astype(str)
        temp_plug = dataframe_plug[dataframe_plug["物资编码"] == temp["物资编码"]]
        if len(temp_plug) > 0:
            temp_plug = temp_plug.iloc[0]
            temp["压接档位/压模"] = temp_plug["压接档位/压模"]
            temp["压线钳类型"] = temp_plug["压线钳类型"]
        # 其他处理
        temp["布线班组"] = get_wiring_team(temp["线槽2"], temp["起始位置"], temp["终止位置"], temp["线长"], temp["备注"])
        temp["电压等级"] = get_voltage_level(temp["线束号"])
        temp["线号颜色"] = get_line_color(temp["电压等级"])
        temp["线号类型"] = process_nan(temp["线号大小"])  + str(temp["线号颜色"])
        temp["线号下标1"] = str(temp["连接点1"]) + "/" + str(temp["点位1"])
        temp["线号下标2"] = str(temp["连接点2"]) + "/" + str(temp["点位2"])
        temp["线束号下标1"] = str(temp["起始位置"]) + str(temp["连接点1"])
        temp["线束号下标2"] = str(temp["终止位置"]) + str(temp["连接点2"])
        temp_connect = get_connecting_team(temp["起始位置"], temp["终止位置"], temp["说明1"], temp["说明2"])
        temp["接线班组1"] = temp_connect["接线班组1"]
        temp["接线班组2"] = temp_connect["接线班组2"]
        temp["下线线号"] = get_upload_wire_number(temp["单/多"], temp["线号"], temp["线束号"])
        temp["下线线号型号"] = get_upload_wire_set(temp["单/多"], temp["线号类型"])
        # 解决字符串被认为是公式的原因
        temp["连接点1"] = process_connect_point(temp["连接点1"])
        temp["连接点2"] = process_connect_point(temp["连接点2"])
        temp["线号下标1"] = process_connect_point(temp["线号下标1"])
        temp["线号下标2"] = process_connect_point(temp["线号下标2"])
        temp["原理图"] = process_connect_point(temp["原理图"])
        return temp
    dataframe_technology: pd.DataFrame = dataframe_design.apply(temp_apply, axis=1)
    # 排序
    dataframe_technology.sort_values(by=["线槽1", "起始位置"],ascending=[True, True], inplace=True)
    # 生成线束序号
    index = 0
    def temp_apply(row: pd.Series) -> pd.Series:
        nonlocal index
        if row["数量"] != "":
            index += 1
            row["序号"] = str(index)
        else:
            row["序号"] = ""
        return row
    dataframe_technology = dataframe_technology.apply(temp_apply, axis=1)
    # 获取彩色标签
    temp_dict_start = {}
    temp_dict_end = {}
    def temp_apply(row: pd.Series) -> pd.Series:
        if str(row["连接点1"]).find("=97-X") != -1 or str(row["连接点1"]).find("=97-x") != -1:
            if row["起始位置"] not in temp_dict_start.keys():
                temp_dict_start[row["起始位置"]] = 0
                row["起始彩色标签"] = color_index_list[temp_dict_start[row["起始位置"]]]
            else:
                temp_dict_start[row["起始位置"]] += 1
                if temp_dict_start[row["起始位置"]] < len(color_index_list):
                    row["起始彩色标签"] = color_index_list[temp_dict_start[row["起始位置"]]]
                else:
                    row["起始彩色标签"] = ""
        if str(row["连接点2"]).find("=97-X") != -1 or str(row["连接点2"]).find("=97-x") != -1:
            if row["终止位置"] not in temp_dict_end.keys():
                temp_dict_end[row["终止位置"]] = 0
                row["终止彩色标签"] = color_index_list[temp_dict_end[row["终止位置"]]]
            else:
                temp_dict_end[row["终止位置"]] += 1
                if temp_dict_end[row["终止位置"]] < len(color_index_list):
                    row["终止彩色标签"] = color_index_list[temp_dict_end[row["终止位置"]]]
                else:
                    row["终止彩色标签"] = ""
        return row
    dataframe_technology = dataframe_technology.apply(temp_apply, axis=1)


    # ------------------------------ 处理下线表 ------------------------------
    def temp_apply(row: pd.Series) -> pd.Series:
        temp = pd.Series(index=down_list_columns, dtype=str)
        for ch in down_list_columns:
            temp[ch] = row[ch]
        return temp
    dataframe_down: pd.DataFrame = dataframe_technology.apply(temp_apply, axis=1)
    dataframe_down = dataframe_down[dataframe_down["线长"] != '0.0']
    dataframe_down = dataframe_down[~dataframe_down["备注"].str.contains('半成品|成品')]


    # ------------------------------ 处理布线表 ------------------------------
    def temp_apply(row: pd.Series) -> pd.Series:
        temp = pd.Series(index=wire_list_columns, dtype=str)
        for ch in wire_list_columns:
            temp[ch] = row[ch]
        return temp
    dataframe_wire: pd.DataFrame = dataframe_technology.apply(temp_apply, axis=1)
    def temp_apply(row: pd.Series) -> bool:
        if row["单/多"] == "单":
            return True
        elif row["单/多"] == "多" and (str(row["序号"]) != "" or str(row["序号"]) != "nan"):
            return True
        return False
    dataframe_wire = dataframe_wire[dataframe_wire.apply(temp_apply, axis=1)]
    dataframe_wire.sort_values(by=["布线班组", "线槽2"],ascending=[True, True], inplace=True)
    dataframe_wire = dataframe_wire.drop(columns="单/多")

    # ------------------------------ 处理接线表 ------------------------------
    temp_copy_list = placement_list_copy_columns_1
    def temp_apply(row: pd.Series) -> pd.Series:
        temp = pd.Series(index=temp_copy_list, dtype=str)
        for ch in temp_copy_list:
            temp[ch] = row[ch]
        return temp
    dataframe_placement_1: pd.DataFrame = dataframe_technology.apply(temp_apply, axis=1)
    dataframe_placement_1 = dataframe_placement_1.rename(columns=placement_list_rename_colunms_1)
    temp_copy_list = placement_list_copy_columns_2
    dataframe_placement_2: pd.DataFrame = dataframe_technology.apply(temp_apply, axis=1)
    dataframe_placement_2 = dataframe_placement_2.rename(columns=placement_list_rename_colunms_2)
    dataframe_placement = pd.concat([dataframe_placement_1, dataframe_placement_2], axis=0)


    # ------------------------------ 处理压接过程记录 ------------------------------
    def temp_apply(row: pd.Series) -> pd.Series:
        temp = pd.Series(index=crimp_recording_colunms, dtype=str)
        for ch in crimp_recording_copy_colunms:
            temp[ch] = row[ch]
        temp["说明"] = row["说明1"]
        temp["自检情况"] = "□完好"
        return temp
    dataframe_crimp_recording: pd.DataFrame = dataframe_technology.apply(temp_apply, axis=1)


    temp_excel["工艺总表"] = dataframe_technology
    temp_excel["下线表"] = dataframe_down
    temp_excel["布线表"] = dataframe_wire
    temp_excel["接线表"] = dataframe_placement
    temp_excel["压接过程记录表"] = dataframe_crimp_recording
    return temp_excel


path = r"C:\Users\Administrator\Documents\dbcenter_dag_process\dag\file\file\查询数据.xlsx"
@st.cache_data(ttl=86400)
def get_dataframe_request_line() -> list:
    dataframe_request_line = pd.read_excel(path, sheet_name="线缆数据")
    return dataframe_request_line

@st.cache_data(ttl=86400)
def get_dataframe_request_plug() -> list:
    dataframe_request_plug = pd.read_excel(path, sheet_name="插芯数据")
    return dataframe_request_plug

st.title("线表生成")
st.markdown("这里上传设计总表")
uploaded_file = st.file_uploader("需求表",label_visibility="collapsed")

if st.button("开始检查"):
    with st.spinner('请稍等......正在处理......'):
        if not uploaded_file is None:
            dataframe_input = pd.read_excel(uploaded_file, sheet_name=0, dtype=str)
            dataframe_request_line = get_dataframe_request_line()
            dataframe_request_plug = get_dataframe_request_plug()
            dataframe_output = process_table_generation(dataframe_input, dataframe_request_line, dataframe_request_plug)
            now_str = datetime.datetime.now().strftime("%Y-%d-%m--%H-%M-%S")
            temp_path = os.path.join(r"C:\Users\Administrator\Documents\dbcenter_dag_process\dag\file\file\线表生成结果缓存", now_str+".xlsx")
            with pd.ExcelWriter(temp_path) as writer:
                for ch in dataframe_output:
                    dataframe_output[ch].to_excel(writer, ch, index=False)
            data = get_file(temp_path).getvalue()
            st.download_button('下载生成的线表文件', data=data, file_name = now_str+".xlsx", mime="application/vnd.ms-excel")
        else:
            st.error("检测上传的文件错误，请刷新页面后重新上传")