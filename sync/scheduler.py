import os
import json
import time
import datetime
from node.base import node_base  # 用于类型标注
from node.sql import sql_node
from node.file import table_read_node, json_read_node
from node.api import heyform_node
from general.config import SYNC_CONFIG
from general.logger import node_logger
from concurrent.futures import ThreadPoolExecutor, wait

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
        self.node_list: list[node_base] = {}
        self.task_apth = task_apth
        self.LOG = node_logger("scheduler_input")
        self.update_node()

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
                        self.LOG.debug(str(future.result()) + "节点执行完成")
                        total_tasks.remove(future)
                else:
                    self.LOG.debug("没有任务运行，等待" + str(WAIT_SYNC_INTERVAL) + "秒")
                    time.sleep(WAIT_SYNC_INTERVAL)
                # 更新节点配置，保证更新的后的task.json被使用
                if count > POLLING_UPDATE_COUNT:
                    count = 0
                    self.update_node()
                
    def get_node_run(self) -> list[node_base]:
        '''获取当前需要执行的节点'''
        temp_node = []
        temp_now = datetime.datetime.now()
        for _node in self.node_list:
            if _node.last_time <= temp_now:
                temp_node.append(_node)
        return temp_node

    def update_node(self)-> None:
        '''更新节点'''
        with open(self.task_apth, "r", encoding="utf-8") as file:
            node_json = json.load(file)
        self.node_list = []
        for ch in node_json:
            if len(ch["next_name"]) == 0:
                if ch["type"] in sql_node.allow_type:
                    self.node_list.append(sql_node(ch))
                elif ch["type"] in table_read_node.allow_type:
                    self.node_list.append(table_read_node(ch))
                elif ch["type"] in json_read_node.allow_type:
                    self.node_list.append(json_read_node(ch))
                elif ch["type"] in heyform_node.allow_type:
                    self.node_list.append(heyform_node(ch))
        self.check_node()
    
    def check_node(self):
        '''检查节点名称是否重复'''
        temp = set()
        for ch in self.node_list:
            if ch.name not in temp:
                temp.add(ch.name)
            else:
                self.LOG.error("服务端已开始监听，正在等待客户端连接...")
                raise ValueError("节点名称重复")
            
            
class output_scheduler:
    '''用于执行数据ETL过程中的发布，批量执行发布到其他数据库的节点'''
    def __init__(self, task_apth: str = TASKS_PATH) -> None:
        self.node_list: list[node_base] = {}
        self.task_apth = task_apth
        self.LOG = node_logger("scheduler")
        self.update_node()