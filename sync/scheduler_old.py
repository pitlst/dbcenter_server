import os
import numpy as np
import traceback
import socket
import json
import time
import datetime
from general.node import node_base
from general.node import \
    sql_to_table, \
    table_to_table, \
    sql_to_nosql, \
    table_to_nosql, \
    excel_to_table, \
    excel_to_nosql, \
    table_to_excel, \
    json_to_nosql, \
    nosql_to_json, \
    nosql_to_nosql, \
    nosql_to_table
from general.config import SYNC_CONFIG
from general.logger import node_logger
from concurrent.futures import ThreadPoolExecutor, wait

SOCKET_IP = SYNC_CONFIG["socket_ip"]
SOCKET_PORT = SYNC_CONFIG["socket_port"]
MIN_SYNC_INTERVAL = SYNC_CONFIG["min_sync_interval"]
WAIT_SYNC_INTERVAL = SYNC_CONFIG["wait_sync_interval"]
NODE_SYNC_MIN_DATASIZE = SYNC_CONFIG["node_sync_min_datasize"]
MAX_NODE_FLOW_CAP = SYNC_CONFIG["max_node_flow_cap"]

TASKS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source", "config", "tasks.json")

class scheduler:
    '''用于执行数据ETL过程中的抽取，批量执行没有前向依赖的节点'''
    def __init__(self, task_apth: str = TASKS_PATH) -> None:
        self.LOG = node_logger("scheduler")
    
        # 启动socket连接
        try:
            self.soc = socket.socket()
            self.soc.connect((SOCKET_IP, SOCKET_PORT))
        except Exception as me:
            self.LOG.error(traceback.format_exc())
            raise RuntimeError("节点处理部分无法连接")
    
        # 读取节点配置
        with open(task_apth, "r", encoding="utf-8") as file:
            node_json = json.load(file)
        # 删除所有的process节点和对process节点的依赖    
        new_json = []
        for ch in node_json:
            if ch["type"] != "process":
                next_ch = []
                for _ch in ch["next_name"]:
                    for __ch in node_json:
                        if __ch["name"] == _ch:
                            if __ch["type"] != "process":
                                next_ch.append(_ch)
                            break
                ch["next_name"] = next_ch
                new_json.append(ch)

        # 检查节点是否存在重复
        name_set = set()
        for ch in new_json:
            if ch["name"] not in name_set:
                name_set.add(ch["name"])
            else:
                self.LOG.error("节点名称重复" + ch["name"])
                raise ValueError("节点名称重复" + ch["name"])
        
        # 初始化节点
        self.all_node: dict[str, node_base] = {}
        for ch in new_json:
            if ch["type"] in sql_to_table.allow_type:
                temp_node = sql_to_table(ch)
            elif ch["type"] in table_to_table.allow_type:
                temp_node = table_to_table(ch)
            elif ch["type"] in sql_to_nosql.allow_type:
                temp_node = sql_to_nosql(ch)
            elif ch["type"] in table_to_nosql.allow_type:
                temp_node = table_to_nosql(ch)
            elif ch["type"] in excel_to_table.allow_type:
                temp_node = excel_to_table(ch)
            elif ch["type"] in excel_to_nosql.allow_type:
                temp_node = excel_to_nosql(ch)
            elif ch["type"] in table_to_excel.allow_type:
                temp_node = table_to_excel(ch)
            elif ch["type"] in json_to_nosql.allow_type:
                temp_node = json_to_nosql(ch)
            elif ch["type"] in nosql_to_json.allow_type:
                temp_node = nosql_to_json(ch)
            elif ch["type"] in nosql_to_nosql.allow_type:
                temp_node = nosql_to_nosql(ch)
            elif ch["type"] in nosql_to_table.allow_type:
                temp_node = nosql_to_table(ch)
            else:
                raise ValueError("未知的节点类型")
            self.all_node[temp_node.name] = temp_node

        # 获取倒置的依赖
        # 内容含义：节点名称，节点的依赖，依赖他的节点
        node_deps: dict[str, tuple[list[str], list[str]]] = {}
        for node_ in self.all_node:
            if node_ not in node_deps.keys():
                node_deps[node_] = (self.all_node[node_].next_name, [])
            else:
                node_deps[node_][0] = self.all_node[node_].next_name
            for node__ in self.all_node[node_].next_name:
                node_deps[node__][1].append(node_)
                
        # 便利所有节点获取子图，时间触发与调度根据子图来进行
        '''
        dag_node 的结构
        列表
            列表
                字典
                    节点的名称
                    列表
                        节点的前向依赖
                        节点的后向依赖
                下次运行时间
                该子图是否正在运行
                子图的整体数据量
        '''
        self.dag_node: list[list[dict[str, list[list[str], list[str]]], datetime.datetime, bool]] = []
        for node_ in node_deps:
            # 既没有前向依赖也没有后向依赖，所以这个节点就是单独的子图
            if len(node_deps[node_][0]) == 0 and len(node_deps[node_][1]) == 0:
                self.dag_node.append([{node_: node_deps[node_]}, datetime.datetime.now(), False, 0])
            else:
                break_label = False
                # 遍历前向依赖
                for deps_name in node_deps[node_][0]:
                    # 遍历现有的子图
                    for index, dag_node_ in enumerate(self.dag_node):
                        # 如果前向依赖在子图的节点中
                        if deps_name in dag_node_[0].keys():
                            self.dag_node[index][0][node_] = node_deps[node_]
                            break_label = True
                            break
                    if break_label:
                        break
                # 遍历后向依赖
                if not break_label:
                    for deps_name in node_deps[node_][1]:
                        for index, dag_node_ in enumerate(self.dag_node):
                            if deps_name in dag_node_[0].keys():
                                self.dag_node[index][0][node_] = node_deps[node_]
                                break_label = True
                                break
                        if break_label:
                            break
                # 有前后项依赖但是不是图中任何一个，那就新开一个给他
                if not break_label:
                    self.dag_node.append([{node_: node_deps[node_]}, datetime.datetime.now(), False, 0])
            
    def run_node(self) -> None:
        '''真正执行节点的地方'''
        with ThreadPoolExecutor() as tpool:
            # 存储所有正在运行的任务
            total_tasks_name = set()
            total_tasks = []
            while True:
                run_node = self.get_node_run(total_tasks_name)
                if len(run_node) == 0:
                    self.LOG.debug("没有新的任务触发执行")
                else:
                    # 只有在本次获取的任务都执行完成后下一次执行才会开始
                    for ch in run_node:
                        total_tasks_name.add(ch.name)
                        total_tasks.append(tpool.submit(ch.run))
                    self.LOG.debug("触发以下任务执行:" + ",".join([ch.name for ch in run_node]))
                # 对于运行任务等待结果，未运行完就更新需要添加的任务
                if len(total_tasks) != 0:
                    is_done, _ = wait(total_tasks, timeout=MIN_SYNC_INTERVAL)
                    for future in is_done:
                        total_tasks_name = self.update_node(total_tasks_name, future.result())
                        total_tasks.remove(future)
                else:
                    self.LOG.debug("没有任务运行，等待" + str(WAIT_SYNC_INTERVAL) + "秒")
                    time.sleep(WAIT_SYNC_INTERVAL)
                
                
    def get_node_run(self, total_tasks_name: set[str]) -> list[node_base]:
        '''获取当前需要执行的节点'''
        run_node: set[node_base] = set()
        time_now = datetime.datetime.now()
        for index in range(len(self.dag_node)):
            # 如果子图正在运行
            if self.dag_node[index][2]:
                # 检查所有子图节点的need_run标志位，如果是false，证明该节点运行完
                not_need_run = True
                for node_ in self.dag_node[index][0]:
                    if self.all_node[node_].need_run:
                        not_need_run = False
                        # 其本身不在正在运行的节点中，并且本身也没有运行
                        if node_ not in total_tasks_name and self.all_node[node_].need_run:
                            # 如果这个节点的所有前向依赖全部是false，也就是运行过了
                            label = True
                            for node__ in self.dag_node[index][0][node_][0]:
                                if self.all_node[node__].need_run:
                                    label = False
                                    break
                            if label:
                                # 添加到需要运行的节点中
                                run_node.add(self.all_node[node_])
                # 如果该子图所有节点的need_run标志位都为false，认为该子图已经运行完成
                if not_need_run:
                    # 计算一下下次触发子图运行的时间
                    temp_second = np.arctan(np.sqrt(self.dag_node[index][3]) / 20480) / np.pi * 2 * 28600 + NODE_SYNC_MIN_DATASIZE
                    # temp_second = NODE_SYNC_MIN_DATASIZE + (28600 / (1 + np.exp(-self.dag_node[index][3] / 4000)))
                    self.dag_node[index][1] = time_now + datetime.timedelta(seconds=temp_second)
                    self.dag_node[index][2] = False
                    self.dag_node[index][3] = 0
                    # 遍历所有的节点重置need_run标志位
                    for node_ in self.dag_node[index][0]:
                        self.all_node[node_].need_run = True
            # 如果子图不在运行并且时间超过触发要求，开始执行
            elif not self.dag_node[index][2] and self.dag_node[index][1] <= time_now:
                # 将所有子图节点的前向无依赖节点运行
                for node_ in self.dag_node[index][0]:
                    if len(self.dag_node[index][0][node_][0]) == 0:
                        run_node.add(self.all_node[node_])
                self.dag_node[index][2] = True
        return run_node
    
    def update_node(self, total_tasks_name: set[str], node_result: tuple[str, int]) -> None:
        '''更新节点状态'''
        name, data_size = node_result
        total_tasks_name.remove(name)
        # 发送socket通信给process
        self.send_notice(name)
        # 更新子图的数据量
        for index in range(len(self.dag_node)):
            if name in self.dag_node[index][0].keys():
                self.dag_node[index][3] += data_size
                break
        return total_tasks_name
            
    def send_notice(self, msg):
        '''通知处理线程节点完成'''
        sebd_msg = msg + "节点执行完成"
        self.LOG.debug(sebd_msg)
        # 用换行符来分割不同次通知
        try:
            self.soc.send(bytes(sebd_msg, encoding='utf-8'))
        except Exception as me:
            self.LOG.error(traceback.format_exc())
            self.soc = socket.socket()
            self.soc.connect((SOCKET_IP, SOCKET_PORT))
            self.send_notice(msg)
            
            

if __name__ == "__main__":
    scheduler().run_node()