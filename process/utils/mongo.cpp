#include "mongo.hpp"

#include "bsoncxx/builder/basic/array.hpp"
#include "bsoncxx/builder/basic/document.hpp"
#include "bsoncxx/builder/basic/kvp.hpp"
#include "bsoncxx/types.hpp"
#include "bsoncxx/json.hpp"

using namespace dbs;

mongo_connect &mongo_connect::instance()
{
    static mongo_connect m_self;
    return m_self;
}

mongo_connect::mongo_connect()
{
    // 因为c++程序不连接其他的数据库，所以这里写死即可
    auto data = toml::parse(std::string(PROJECT_PATH) + "../source/config/connect.toml")["数据处理服务存储"];
    auto ip = toml::get<std::string>(data["ip"]);
    auto port = std::to_string(toml::get<int>(data["port"]));
    mongocxx::uri url("mongodb://" + ip + ":" + port + "/");
    this->m_pool_ptr = std::make_unique<mongocxx::pool>(url);
    if (this->m_pool_ptr == nullptr)
    {
        std::string temp_err = "获取mongo数据库连接失败，连接池指针为空";
        std::cerr << temp_err << '\n';
        throw std::runtime_error(temp_err);
    }
}

mongo_connect::~mongo_connect()
{
    this->m_pool_ptr.release();
    this->m_pool_ptr = nullptr;
}

mongocxx::v_noabi::pool::entry mongo_connect::inithread_client()
{
    return m_pool_ptr->acquire();
}

mongocxx::collection mongo_connect::get_coll(mongocxx::v_noabi::pool::entry &client_, const std::string &db_name, const std::string &coll_name) const
{
    return client_[db_name][coll_name];
}

std::vector<nlohmann::json> mongo_connect::get_coll_data(mongocxx::v_noabi::pool::entry &client_, const std::string &db_name, const std::string &coll_name) const
{
    auto results_cursor = this->get_coll(client_, db_name, coll_name).find({});
    std::vector<nlohmann::json> results;
    for (auto &&ch : results_cursor)
    {
        nlohmann::json m_json = nlohmann::json::parse(bsoncxx::to_json(ch));
        results.emplace_back(m_json);
    }
    return results;
}
