import os
import time
import json 
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, wait
from general.connect import db_engine
from general.logger import node_logger
from general.node import node_base
from general.node import \
    node_base, \
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
    node_list = []
    for ch in node_json:
        if ch["next_name"] != "process":
            node_list.append(ch)
    return node_list
    
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


mongo_client = db_engine.get_nosql("数据处理服务存储")["public"]

def get_run(all_node: dict[str, node_base], temp_coll: list) -> dict[str, node_base]:
    # 获取当前需要运行的节点
    temp_node: list[node_base] = {}
    for dag_node in temp_coll:
        # 如果这个子图在运行
        if dag_node["is_running"]:
            for node_ in dag_node["node"]:
                # 如果这个节点需要运行
                if node_["name"] in all_node.keys() and node_["status"] == "需要运行":
                    temp_node[node_["name"]] = all_node[node_["name"]]
    return temp_node

def update_context(node_future: concurrent.futures.Future, temp_coll: list) -> None:
    name, data_size = node_future.result()
    for dag_node in temp_coll:
        for node_ in dag_node["node"]:
            if node_["name"] == name:
                dag_node["data_size"] += data_size
                node_["status"] = "已经运行完成"
                mongo_client["context"].replace_one({"_id": dag_node["_id"]}, dag_node)
                return
            
                    
if __name__ == "__main__":
    total_tasks = []
    LOG = node_logger("sync")
    all_node = make_node(process_tasks())
    
    with ThreadPoolExecutor() as tpool:
        while True:
            temp_coll = mongo_client["context"].find().to_list()
            run_node = get_run(all_node, temp_coll)
            for ch in run_node:
                total_tasks.append(tpool.submit(run_node[ch]))
            if len(run_node) == 0:
                LOG.debug("没有新的任务接到触发信号执行")
            else:
                LOG.debug("接到信号触发以下任务执行:" + ",".join([ch for ch in run_node]))
                for ch in run_node:
                    total_tasks.append(tpool.submit(run_node[ch].run))
            if len(total_tasks) != 0:
                LOG.debug("等待任务运行10秒")
                is_done, _ = wait(total_tasks, timeout=10)
                for future in is_done:
                    total_tasks.remove(future)
                    update_context(future, temp_coll)
            else:
                LOG.debug("没有任务运行，等待任务运行10秒")
                time.sleep(10)