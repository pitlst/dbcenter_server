import os
import json
import time
import datetime
from sync.general.node import node_base, __total_node_num__
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

__all_node_type__: list[node_base] = [
    sql_to_table, 
    table_to_table, 
    sql_to_nosql, 
    table_to_nosql, 
    excel_to_table, 
    excel_to_nosql, 
    table_to_excel, 
    json_to_nosql, 
    nosql_to_json, 
    nosql_to_nosql, 
    nosql_to_table
]

'''
这里主要想实现的一个想法是，对于没有前后依赖的sql同步节点
在程序触发时根据日志检查上一次的同步数据量
并根据一个系数来计算这一次触发时该节点是否要执行
然后通过IPC来通知c++进行更新
对于有前后依赖的sql节点应当放到c++的处理中执行

实际计算同步时间的方法是在节点的定义里，可以重写

对于更新节点的情况建议直接重启数据同步服务
会有一个流量高峰，因为所有节点的状态和时间间隔会重新同步，所以建议在非业务时间重启
'''

SOCKET_IP = SYNC_CONFIG["socket_ip"]
SOCKET_PORT = SYNC_CONFIG["socket_port"]
MIN_SYNC_INTERVAL = SYNC_CONFIG["min_sync_interval"]
WAIT_SYNC_INTERVAL = SYNC_CONFIG["wait_sync_interval"]
POLLING_UPDATE_COUNT = SYNC_CONFIG["polling_update_count"]
TASKS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source", "config", "tasks.json")

class input_scheduler:
    '''用于执行数据ETL过程中的抽取，批量执行没有前向依赖的节点'''
    def __init__(self, task_apth: str = TASKS_PATH) -> None:
        self.nodep_node: dict[str, node_base] = {}
        self.havedep_node: dict[str, node_base] = {}
        self.nodep_node_last_time: dict[str, datetime.datetime] = {}
        self.task_apth = task_apth
        self.LOG = node_logger("scheduler")
        self.load_node()

    def run_node(self) -> None:
        '''真正执行节点的地方'''
        with ThreadPoolExecutor() as tpool:
            # 死循环，不退出
            total_tasks = []
            count = 0
            while True:
                count += 1
                run_node = self.get_node_run()
                if len(run_node) == 0:
                    # 没任务运行就等10秒，不要让cpu一直卡在检查
                    self.LOG.debug("没有新的任务触发执行")
                else:
                    # 只有在本次获取的任务都执行完成后下一次执行才会开始
                    for ch in run_node:
                        total_tasks.append(tpool.submit(ch.run))
                    self.LOG.debug("触发以下任务执行:" + ",".join([ch.name for ch in run_node]))
                # 对于运行任务等待结果，未运行完就更新需要添加的任务
                if len(total_tasks) != 0:
                    is_done, _ = wait(total_tasks, timeout=MIN_SYNC_INTERVAL)
                    for future in is_done:
                        node_result = future.result()
                        total_tasks.remove(future)
                        self.LOG.debug(str(node_result[0]) + "节点执行完成")
                        self.update_node(node_result)
                else:
                    self.LOG.debug("没有任务运行，等待" + str(WAIT_SYNC_INTERVAL) + "秒")
                    time.sleep(WAIT_SYNC_INTERVAL)
                # 更新节点配置，保证更新的后的task.json被使用
                if count > POLLING_UPDATE_COUNT:
                    count = 0
                    self.load_node()
                
    def get_node_run(self) -> list[node_base]:
        '''获取当前需要执行的节点'''
        temp_node = []
        # 检查无依赖节点的时间是否需要触发，是否应该运行
        temp_now = datetime.datetime.now()
        for _node in self.nodep_node_last_time:
            if self.nodep_node_last_time[_node] <= temp_now:
                temp_node.append(self.nodep_node[_node])
        # 检查有依赖的节点是否被通知需要运行
        for _node in self.havedep_node:
            if self.havedep_node[_node].need_run:
                temp_node.append(self.havedep_node[_node])
        return temp_node
    
    def update_node(self, node_result: tuple[str, int]) -> None:
        '''更新节点状态'''
        name, data_size = node_result
        # 没有依赖的节点计算下一次触发时间即可
        if name in self.nodep_node.keys():
            self.nodep_node_last_time[name] = datetime.datetime.now() + datetime.timedelta(seconds=data_size / (SYNC_CONFIG["max_node_flow_cap"] / __total_node_num__))
        # 通知所有依赖他的节点可以跑了
        for _node in self.havedep_node:
            if name in self.havedep_node[_node].next_name:
                self.havedep_node[_node].need_run = True

    def load_node(self)-> None:
        '''根据配置加载节点'''
        with open(self.task_apth, "r", encoding="utf-8") as file:
            node_json = json.load(file)
        self.nodep_node = []
        self.havedep_node = []
        for ch in node_json:
            for __type__ in __all_node_type__:
                if ch["type"] in __type__.allow_type:
                    if len(ch["next_name"]) == 0:
                        self.nodep_node[ch["name"]] = __type__(ch)
                        self.nodep_node_last_time[ch["name"]] = datetime.datetime.now()
                    else:
                        self.havedep_node[ch["name"]] = __type__(ch)
        self.check_node()
    
    def check_node(self):
        '''检查节点名称是否重复'''
        name_set = set()
        for ch in self.nodep_node:
            if ch not in name_set:
                name_set.add(ch)
            else:
                self.LOG.error("节点名称重复")
                raise ValueError("节点名称重复")
        for ch in self.havedep_node:
            if ch not in name_set:
                name_set.add(ch)
            else:
                self.LOG.error("节点名称重复")
                raise ValueError("节点名称重复")