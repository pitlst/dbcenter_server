import toml

from node.sql import sql_node
from node.file import table_read_node, table_write_node, json_read_node, json_write_node
from scheduler import scheduler

if __name__ == "__main__":
    node_list = []
    
    dispatcher = scheduler(node_list)
    dispatcher.run_node()