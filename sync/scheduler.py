import os
import json
import socket
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

SOCKET_DEBUG = SYNC_CONFIG["socket_debug"]
SOCKET_IP = SYNC_CONFIG["socket_ip"]
SOCKET_PORT = SYNC_CONFIG["socket_port"]
MIN_SYNC_INTERVAL = SYNC_CONFIG["min_sync_interval"]
WAIT_SYNC_INTERVAL = SYNC_CONFIG["wait_sync_interval"]
TASKS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source", "config", "task.json")

class scheduler:
    '''调度器，用于调度sql节点的执行'''
    def __init__(self, task_apth: str = TASKS_PATH) -> None:
        self.node_list: list[node_base] = {}
        self.LOG = node_logger("scheduler")
        self.update_node(task_apth)
        self.socket_server = socket.socket()
        self.socket_server.bind((SOCKET_IP, SOCKET_PORT))
        if SOCKET_DEBUG:
            self.LOG.info("服务端已开始监听，正在等待客户端连接...")
            try:
                self.conn, address = self.socket_server.accept()
            except Exception as me:
                self.LOG.error(me)
            self.LOG.info(f"接收到了客户端的连接，客户端的信息：{address}")

    def run_node(self) -> None:
        '''真正执行节点的地方'''
        with ThreadPoolExecutor() as tpool:
            # 死循环，不退出
            total_tasks = []
            while True:
                run_node = self.get_node_run()
                if len(run_node) == 0:
                    # 没任务运行就等10秒，不要让cpu一直卡在检查
                    self.LOG.debug("没有新的任务触发执行")
                else:
                    # 只有在本次获取的任务都执行完成后下一次执行才会开始
                    for ch in run_node:
                        total_tasks.append(tpool.submit(ch.run))
                # 对于运行任务等待结果，未运行完就更新需要添加的任务
                if len(total_tasks) != 0:
                    is_done, _ = wait(total_tasks, timeout=MIN_SYNC_INTERVAL)
                    for future in is_done:
                        name = future.result()
                        self.notify(name)
                        total_tasks.remove(future)
                else:
                    self.LOG.debug("没有任务运行，等待" + str(WAIT_SYNC_INTERVAL) + "秒")
                    time.sleep(WAIT_SYNC_INTERVAL)
                
    def get_node_run(self) -> list[node_base]:
        '''获取当前需要执行的节点'''
        temp_node = []
        temp_now = datetime.datetime.now()
        for _node in self.node_list:
            if _node.last_time <= temp_now:
                temp_node.append(_node)
        return temp_node

    def notify(self, name: str)-> None:
        '''
        在节点运行完成之后
        使用socket通知处理程序
        '''
        msg = "节点：" + name + "已经执行完成。"
        self.LOG.info(msg)
        if SOCKET_DEBUG:
            self.conn.send(msg.encode("UTF-8"))

    def update_node(self, task_apth: str = TASKS_PATH)-> None:
        '''更新节点'''
        with open(task_apth, "r+", encoding="utf-8") as file:
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