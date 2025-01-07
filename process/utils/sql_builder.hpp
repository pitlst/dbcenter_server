#ifndef DBS_SQL_BUILD_INCLUDE
#define DBS_SQL_BUILD_INCLUDE

#include <iostream>
#include <string>
#include <vector>
#include <sstream>

namespace dbs
{
    class sql_builder
    {
    public:
        // 初始化查询（例如 SELECT * FROM table）
        sql_builder &init(const std::string &query);
        // 添加筛选条件（支持 AND 或 OR 连接）
        sql_builder &add(const std::string &condition, const std::string &logic = "AND");
        // 构建最终的 SQL 查询字符串
        std::string build();

    private:
        std::string m_sql;                // 存储基础 SQL 查询
        std::vector<std::string> conditions; // 存储筛选条件
    };
}

#endif