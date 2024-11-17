#include "logger.hpp"

using namespace dbs;

int main()
{
    LOGGER.create_time_collection("测试");
    LOGGER.debug("测试", "测试1");
    LOGGER.debug("测试", "测试2");
    LOGGER.debug("测试", "测试3");
    LOGGER.debug("测试", "测试4");
    LOGGER.debug("测试", "测试5");
    return 0;
}