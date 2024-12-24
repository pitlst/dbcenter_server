#ifndef DBS_MONGO_INCLUDE
#define DBS_MONGO_INCLUDE

#include <string>
#include <memory>
#include <map>
#include <thread>

#include "mongocxx/client.hpp"
#include "mongocxx/instance.hpp"
#include "mongocxx/pool.hpp"
#include "mongocxx/uri.hpp"

#include "toml.hpp"
#include "json.hpp"

#include "general.hpp"

namespace dbs
{

    // 全局单例的连接类，用于组织对数据库的连接
    class mongo_connect
    {
    public:
        // 获取单实例对象
        static mongo_connect &instance();
        // 在对应线程初始化以获取连接
        mongocxx::v_noabi::pool::entry inithread_client();

        // 获取集合
        mongocxx::collection get_coll(mongocxx::v_noabi::pool::entry & client_, const std::string &db_name, const std::string &coll_name) const;
        // 获取集合的数据
        std::vector<nlohmann::json> get_coll_data(mongocxx::v_noabi::pool::entry & client_, const std::string &db_name, const std::string &coll_name) const;

    private:
        // 禁止外部构造与析构
        mongo_connect();
        ~mongo_connect();

        // 数据库驱动
        mongocxx::instance m_instance;
        // 数据库连接池
        std::unique_ptr<mongocxx::pool> m_pool_ptr = nullptr;
    };
}

// mongo全局单例连接的简写
#define MONGO dbs::mongo_connect::instance()

#endif