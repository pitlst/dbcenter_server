import socket
import datetime
import concurrent.futures    # 用于类型标注
from general.base import node_base  # 用于类型标注
from general.logger import node_logger
from concurrent.futures import ThreadPoolExecutor, as_completed


# 系统数据库同步速率上限定义，单位Mb/s
MAX_NODE_FLOW_CAP = 100
# socket的端口
SOCKET_PORT = 10089


class scheduler:
    '''
    调度器，用于调度sql节点的执行
    '''
    def __init__(self, node_list: list[node_base]):
        # 所有节点
        self.node_list: dict[str, node_base] = {}
        for _node in node_list:
            self.node_list[_node.name] = _node
        # 日志
        self.LOG = node_logger("scheduler")
        # 所有节点上一次运行的时间
        self.last_run_time: dict[str, datetime.datetime] = {}
        for _node in node_list:
            self.last_run_time[_node.name] = None
        # 所有节点的时间间隔
        self.time_interval: dict[str, datetime.timedelta] = {}
        for _node in node_list:
            self.time_interval[_node.name] = datetime.timedelta()

    
    def run_node(self) -> None:
        '''
        真正执行节点的地方
        '''
        

    def update_node(self):
        '''
        更新节点的信息
        '''
        temp_now = datetime.datetime.now()
        for _node in self.last_run_time:
            # 初始化时使用
            if self.last_run_time[_node] == None:
                self.node_list[_node].need_run = True
            if self.last_run_time[_node] + self.time_interval[_node] <= temp_now:
                self.node_list[_node].need_run = True

    def notify(self, name: str):
        '''
        在节点运行完成之后
        使用socket通知处理程序进行
        '''

if __name__ == "__main__":
    scheduler(node_list = [])