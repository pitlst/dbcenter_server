#include "business_connection/process.hpp"

using namespace dbs;

void task_bc_process::main_logic()
{
    LOGGER.info(this->node_name, "读取业联数据");
    auto ods_bc_ = this->get_coll_data("dwd", "业联-车间执行单");
    auto ods_bc_class_group_entry = this->get_coll_data("dwd", "业联-工艺流程");
    // auto ods_bc_class_group_entry = this->get_coll_data("dwd", "业联-设计变更");
    // auto ods_bc_class_group_entry = this->get_coll_data("dwd", "业联-设计变更执行");
    // auto ods_bc_class_group_entry = this->get_coll_data("dwd", "业联-业联执行关闭");
    // auto ods_bc_class_group_entry = this->get_coll_data("dwd", "业联-业务联系书");
}