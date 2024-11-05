from scheduler import node_run

# 兼容用
from node.old.shr_clean import shr_clean
from node.old.project_clean import project_clean
from node.old.ameliorate import ameliorate_process
from node.old.error import error_clean, error_export_process, error_process
from node.old.industry_association import industry_association_clean
from node.old.delivery import delivery_process
from node.old.attendance_clean import attendance_clean
from node.old.human_efficiency import \
    human_efficiency_person_process, \
    full_month_travel_calculation, \
    human_efficiency_work_clean, \
    human_efficiency_work_process, \
    human_efficiency_labor_used_process
from node.old.travel import travel_clean
from node.old.business_connection.bs_splice import shop_exection_splice

'''
这里主要想实现的一个想法是，对于没有前向依赖的sql同步节点
在程序触发时根据日志检查上一次的同步数据量
并根据一个系数来计算这一次触发时该节点是否要执行

'''





if __name__ == "__main__":
    
    
    ...