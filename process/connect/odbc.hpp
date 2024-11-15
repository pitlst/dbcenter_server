#ifndef DBS_CONNECT_INCLUDE
#define DBS_CONNECT_INCLUDE
#include <map>
#include <string>

#include "toml.hpp"

#include "data.hpp"

namespace dbs
{

    // 全局单例的连接类，用于组织对数据库的连接
    struct mysql_connect
    {

        mysql_connect(const toml::value &input_config);
    };

    class pgsql_connect
    {
    };

}

#endif