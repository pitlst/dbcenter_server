import os
import json
import datetime
import numpy as np
from collections import deque

from general.connect import MONGO_CLIENT
from general.logger import LOG
from general.config import NODE_DEPEND, NODE_DAG

# 节点两次同步变化的系数
NODE_SYNC_K = 204800
# 节点两次同步的最小间隔，该参数用在计算节点同步数据量的使用
NODE_SYNC_MIN = 600
# 节点两次同步的最大间隔，该参数用在计算节点同步数据量的使用
NODE_SYNC_MAX = 3600

if __name__ == "__main__":
    
    while True:

                            
        