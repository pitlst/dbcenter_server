import os
import json
import datetime
from general.connect import MONGO_CLIENT

# ------------------------------------------获取节点配置------------------------------------------
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "config", "tasks.json"), "r", encoding="utf-8") as file:
    node_json = json.load(file)
name_set = set()
for ch in node_json:
    if ch["name"] not in name_set:
        name_set.add(ch["name"])
    else:
        raise ValueError("节点名称重复" + ch["name"])
    
    
# ------------------------------------------获取节点依赖------------------------------------------
NODE_DEPEND: dict[str, list[str]] = {}
for ch in node_json:
    NODE_DEPEND[ch["name"]] = ch["next_name"]
   
# ------------------------------------------获取节点子图------------------------------------------
def get_deps(node_name: str) -> set[str]:
    '''获取当前节点的所有前后依赖'''
    temp_set = set()
    temp_set.add(node_name)
    for ch in NODE_DEPEND[node_name]:
        temp_set.add(ch)
        temp_set = temp_set.union(get_deps(ch))
    return temp_set

NODE_DAG: list[set[str]] = []
for ch in NODE_DEPEND:
    not_have = True
    for ch_ in NODE_DAG:
        if ch in ch_:
            not_have = False
            break
    if not_have:
        temp_set = set()
        temp_set.add(ch)
        temp_set = temp_set.union(get_deps(ch))
        NODE_DAG.append(temp_set)