#include "person/person.hpp"

using namespace dbs;

void increment_person::main_logic()
{
    // ----------从数据库读取数据----------
    LOGGER.info(this->node_name, "读取数据");
    auto dwd_results = this->get_coll_data("ods", "shr_");

    
}

void task_person::main_logic()
{
    // ----------从数据库读取数据----------
    LOGGER.info(this->node_name, "读取数据");
    auto dwd_results = this->get_coll_data("ods", "shr_");

    
}
