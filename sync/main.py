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


if __name__ == "__main__":
    ...