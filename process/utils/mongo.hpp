#ifndef DBS_MONGO_INCLUDE
#define DBS_MONGO_INCLUDE

#include <string>
#include <memory>
#include <map>

#include "mongocxx/client.hpp"
#include "mongocxx/instance.hpp"
#include "mongocxx/pool.hpp"
#include "mongocxx/uri.hpp"

#include "json.hpp"
#include "toml.hpp"

namespace dbs
{

    // 全局单例的连接类，用于组织对数据库的连接
    class mongo_connect
    {
    public:
        // 获取单实例对象
        static mongo_connect &instance();
        // 获取数据库
        mongocxx::database &get_db(const std::string &db_name);
        // 获取集合
        mongocxx::collection get_coll(const std::string &db_name, const std::string &coll_name);
        // 获取集合的值
        std::vector<nlohmann::json> get_coll_data(const std::string &db_name, const std::string &coll_name);

    private:
        // 禁止外部构造与析构
        mongo_connect();
        ~mongo_connect();

        // 数据库驱动
        mongocxx::instance m_instance;
        // 数据库连接
        std::unique_ptr<mongocxx::client> m_client_ptr = nullptr;
        // 所有获取过的数据库对象的保存，用于保证生命周期
        std::map<std::string, mongocxx::database> m_data;
    };
}

// mongo全局单例连接的简写
#define MONGO dbs::mongo_connect::instance()

#endif