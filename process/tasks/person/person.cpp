#include "person/person.hpp"

using namespace dbs;

void task_person::main_logic()
{
    try
    {
        // ----------从数据库读取数据----------
    }
    catch (const std::exception &e)
    {
        LOGGER.error(this->node_name, e.what());
    }
}
