#include "logger.hpp"

using namespace dbs;

int main()
{
    logger::instance().create_time_collection("测试");
    logger::instance().debug("测试", "测试1");
    logger::instance().debug("测试", "测试2");
    logger::instance().debug("测试", "测试3");
    logger::instance().debug("测试", "测试4");
    logger::instance().debug("测试", "测试5");
    return 0;
}