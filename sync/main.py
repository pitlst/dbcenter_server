import os
import json
from node.sql import sql_node
from node.file import table_read_node, json_read_node
from node.api import heyform_node
from scheduler import scheduler

if __name__ == "__main__":
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source", "config", "task.json")
    with open(path, "r", encoding="utf-8") as file:
        node_json = json.load(file)
    node_list = []
    for ch in node_json:
        if len(ch["next_name"]) == 0:
            if ch["type"] in sql_node.allow_type:
                node_list.append(sql_node(ch))
            elif ch["type"] in table_read_node.allow_type:
                node_list.append(table_read_node(ch))
            elif ch["type"] in json_read_node.allow_type:
                node_list.append(json_read_node(ch))
            elif ch["type"] in heyform_node.allow_type:
                node_list.append(heyform_node(ch))
    dispatcher = scheduler(node_list)
    dispatcher.run_node()