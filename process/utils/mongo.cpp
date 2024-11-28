#include "mongocxx/uri.hpp"

#include "mongo.hpp"

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
    m_pool_ptr = std::make_unique<mongocxx::pool>(url);
    if(m_pool_ptr == nullptr)
    {
        std::string temp_err = "获取mongo数据库连接失败，连接池指针为空";
        std::cerr << temp_err << '\n';
        throw std::runtime_error(temp_err);
    }
}

mongo_connect::~mongo_connect()
{
    m_pool_ptr.release();
    m_pool_ptr = nullptr;
}