#include "logger.hpp"

using namespace dbs;

int main()
{
    logger::instance().debug("测试", "测试");
    return 0;
}