#include "business_connection/process.hpp"

using namespace dbs;

void task_bc_process::main_logic()
{
    try
    {
        auto client = MONGO.init_client();
        // ----------从数据库读取数据----------
        LOGGER.info(this->node_name, "读取数据");
        auto dwd_bc_design_change = MONGO.get_coll_data(client, "dwd", "业联-设计变更");
        auto dwd_bc_technological_process = MONGO.get_coll_data(client, "dwd", "业联-工艺流程");
        auto dwd_bc_design_change_execution = MONGO.get_coll_data(client, "dwd", "业联-设计变更执行");
        auto dwd_bc_close = MONGO.get_coll_data(client, "ods", "bc_business_connection_close");

        
    }
    catch (const std::exception &e)
    {
        LOGGER.error(this->node_name, e.what());
    }
}