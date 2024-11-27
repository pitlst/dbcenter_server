import concurrent.futures
import os
import json
import time
import datetime
import numpy as np
import concurrent
from concurrent.futures import ThreadPoolExecutor, wait
from general.socket import l_socket
from general.config import SYNC_CONFIG
from general.logger import node_logger
from general.node import node_base
from general.node import \
    sql_to_table,   \
    table_to_table, \
    sql_to_nosql,   \
    table_to_nosql, \
    excel_to_table, \
    excel_to_nosql, \
    table_to_excel, \
    json_to_nosql,  \
    nosql_to_json,  \
    nosql_to_nosql, \
    nosql_to_table
    
TASKS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source", "config", "tasks.json")
CONTEXT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source", "config", "sync_context.json")

def process_tasks(task_apth: str = TASKS_PATH) -> list[dict]:
    '''检查并处理任务文件''' 
    with open(task_apth, "r", encoding="utf-8") as file:
        node_json = json.load(file)
    # 检查节点是否存在重复
    name_set = set()
    for ch in node_json:
        if ch["name"] not in name_set:
            name_set.add(ch["name"])
        else:
            raise ValueError("节点名称重复" + ch["name"])
    # 对于依赖是process的单独处理
    new_json = []
    process_json = []
    for ch in node_json:
        if ch["next_name"] != "process":
            new_json.append(ch)
        else:
            process_json.append(ch)
    return new_json, process_json


def make_node(new_json: list[dict])-> dict[str, node_base]:
    '''生成节点'''        
    all_node: dict[str, node_base] = {}
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
        all_node[temp_node.name] = temp_node
    return all_node


def make_node_deps(new_json: list[dict]) -> dict[str, list[str]]:
    '''生成节点的依赖'''
    node_deps: dict[str, list[str]] = {}
    for ch in new_json:
        node_deps[ch["name"]] = ch["next_name"]
    return node_deps


class scheduler:
    def __init__(self, sync_node: dict[str, node_base], process_node: dict[str, node_base], node_deps: dict[str, dict[str, list[str]]]) -> None:
        self.sync_node = sync_node
        self.process_node = process_node
        self.node_deps = node_deps
        self.total_tasks = []
        self.total_tasks_name = set()
        self.LOG = node_logger("sync_scheduler")
        # 调度上下文
        with open(CONTEXT_PATH, "r", encoding="utf-8") as file:
            self.context = json.load(file)
        l_socket.connect()
        
    def run(self) -> None:
        with ThreadPoolExecutor() as tpool:
            self.check_context()
            while True:
                run_node = self.get_run()
                if len(run_node) == 0:
                    self.LOG.debug("没有新的任务触发执行")
                else:
                    self.LOG.debug("触发以下任务执行:" + ",".join([ch.name for ch in run_node]))
                    for ch in run_node:
                        self.total_tasks.append(tpool.submit(ch.run))
                if len(self.total_tasks) != 0:
                    self.LOG.debug("等待任务运行" + str(SYNC_CONFIG["min_sync_interval"]) + "秒")
                    is_done, _ = wait(self.total_tasks, timeout=SYNC_CONFIG["min_sync_interval"])
                    for future in is_done:
                        self.update_context(future)
                    self.save_context()
                else:
                    self.LOG.debug("没有任务运行，等待" + str(SYNC_CONFIG["wait_sync_interval"]) + "秒")
                    time.sleep(SYNC_CONFIG["wait_sync_interval"])
        
    def get_run(self) -> list[node_base]:
        # process
        ipc_str = l_socket.recv()
        node_ipc = ipc_str.split(";")
        process_node: list[node_base] = []
        for ch in node_ipc:
            if ch in self.process_node.keys():
                process_node.append(self.process_node[ch])
        # sync
        sync_node: list[node_base] = []
        if not self.context["status"]["is_running"]:
            if datetime.datetime.strptime(self.context["status"]["last_run_time"], "%y-%d-%m %H:%M:%S") <= datetime.datetime.now():
                self.context["status"]["is_running"] = True
                for node_ in self.context["node_deps"]:
                    if len(self.context["node_deps"][node_]) == 0:
                        sync_node.append(self.sync_node[node_])
        else:
            all_run_label = True
            for node_ in self.context["node_deps"]:
                if not self.context["status"]["node"][node_]:
                    all_run_label = False
                    if not node_ in self.total_tasks_name:
                        run_label = True
                        for node__ in self.context["node_deps"][node_]:
                            if not self.context["status"]["node"][node__]:
                                run_label = False
                                break
                        if run_label:
                            sync_node.append(self.sync_node[node_])
            if all_run_label:
                temp_second = np.arctan(np.sqrt(self.context["status"]["datasize"]) / SYNC_CONFIG["node_sync_k"]) / np.pi * 2 * (SYNC_CONFIG["node_sync_max"] - SYNC_CONFIG["node_sync_min"]) + SYNC_CONFIG["node_sync_min"]
                self.context["status"]["last_run_time"] = (datetime.datetime.now() + datetime.timedelta(seconds=temp_second)).strftime("%y-%d-%m %H:%M:%S")
                self.context["status"]["datasize"] = 0
                self.context["status"]["is_running"] = False
                for node_ in self.context["status"]["node"]:
                    self.context["status"]["node"][node_] = False
        # 保存上下文
        self.save_context()
        need_run =  process_node + sync_node
        # 更新正在运行的节点
        for ch in need_run:
            self.total_tasks_name.add(ch.name)
        return need_run
        
    def update_context(self, node_future: concurrent.futures.Future) -> None:
        name, data_size = node_future.result()
        # 在当前运行的节点记录中移除
        self.total_tasks.remove(node_future)
        self.total_tasks_name.remove(name)
        if name in self.sync_node.keys():
            self.context["status"]["node"][name] = True
            self.context["status"]["datasize"] += data_size
            l_socket.send(name + ";")
        
    def check_context(self) -> None:
        # 比较老的上下文是否相同，不相同使用新的上下文
        if "node_deps" not in self.context or self.context["node_deps"] != self.node_deps:
            self.context["node_deps"] = self.node_deps
            self.context["status"] = {}
            self.context["status"]["last_run_time"] = datetime.datetime.now().strftime("%y-%d-%m %H:%M:%S")
            self.context["status"]["datasize"] = 0
            self.context["status"]["is_running"] = False
            self.context["status"]["node"] = {}
            for node_name in self.node_deps:
                self.context["status"]["node"][node_name] = False
            self.save_context()

    def save_context(self) -> None:
        # 保存新的上下文
        with open(CONTEXT_PATH, "w+", encoding="utf-8") as file:
            file.write(json.dumps(self.context, indent=4, sort_keys=True, ensure_ascii=False))
                    

if __name__ == "__main__":
    new_json, process_json = process_tasks()
    scheduler(make_node(new_json), make_node(process_json), make_node_deps(new_json)).run()