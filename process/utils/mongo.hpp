#ifndef DBS_MONGO_INCLUDE
#define DBS_MONGO_INCLUDE

#include <string>
#include <memory>

#include "mongocxx/client.hpp"
#include "mongocxx/instance.hpp"
#include "mongocxx/pool.hpp"
#include "mongocxx/uri.hpp"

#include "toml.hpp"

namespace dbs
{

    // 全局单例的连接类，用于组织对数据库的连接
    class mongo_connect
    {
    public:
        // 获取单实例对象
        static mongo_connect &instance();
        // 数据库连接池单例
        std::unique_ptr<mongocxx::pool> m_pool_ptr = nullptr;

    private:
        // 禁止外部构造与析构
        mongo_connect();
        ~mongo_connect();

        // 数据库驱动单例
        mongocxx::instance m_instance;
    };
}

// mongo全局单例连接的简写
#define MONGO mongo_connect::instance()

#endif