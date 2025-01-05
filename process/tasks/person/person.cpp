#include "person/person.hpp"

using namespace dbs;

void task_person::main_logic()
{
    LOGGER.info(this->node_name, "读取数据");
    auto client = MONGO.init_client();
    // ----------从数据库读取数据----------
    auto ods_results = MONGO.get_coll_data(client, "ods", "shr_staff");

    
}
