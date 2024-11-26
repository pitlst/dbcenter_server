#ifndef DBS_NODE_VISITOR_INCLUDE
#define DBS_NODE_VISITOR_INCLUDE

#include <map>

#include "node.hpp"
#include "data.hpp"

namespace dbs
{
    // 处理访客系统的数据结构
    inline void process_visitor()
    {
        // 连接数据库获取集合
        auto m_client_ptr = std::make_unique<mongocxx::pool::entry>(MONGO.m_pool_ptr->acquire());
        auto m_database_ptr = std::make_unique<mongocxx::database>((*m_client_ptr)["ods"]);
        auto collection = (*m_database_ptr)["submissionmodels"];

        for (size_t i = 0; i < count; i++)
        {
            /* code */
        }
        
    }
}

#endif