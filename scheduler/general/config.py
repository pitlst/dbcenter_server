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
        
# ------------------------------------------获取节点子图------------------------------------------

# ------------------------------------------获取或者更新调度上下文------------------------------------------
not_same = False
temp_coll = MONGO_CLIENT["public"]["context"].find({},{ "_id": 0, "node": 1}).to_list()
if len(temp_coll) == 0:
    not_same = True
else:
    coll_node_deps = []
    for ch in temp_coll:
        temp_set = set()
        for ch_ in ch["node"]:
            temp_set.add(ch_["name"])
        coll_node_deps.append(temp_set)
    not_same = coll_node_deps != NODE_DAG

if not_same:
    MONGO_CLIENT["public"]["context"].drop()
    all_doc = []
    for ch in NODE_DAG:
        temp_dict = {}
        temp_dict["is_running"] = False
        temp_dict["node"] = []
        for ch_ in ch:
            temp_dict_ = {}
            temp_dict_["name"] = ch_
            temp_dict_["deps"] = NODE_DEPEND[ch_]
            temp_dict_["status"] = "未运行"    # 可能有三种状态，未运行，需要运行，正在运行，已经运行完成
            temp_dict["node"].append(temp_dict_)
        all_doc.append(temp_dict)
    MONGO_CLIENT["public"]["context"].insert_many(all_doc)