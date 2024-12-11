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
    m_client_ptr = std::make_unique<mongocxx::client>(url);
    if(m_client_ptr == nullptr)
    {
        std::string temp_err = "获取mongo数据库连接失败，连接池指针为空";
        std::cerr << temp_err << '\n';
        throw std::runtime_error(temp_err);
    }
}

mongo_connect::~mongo_connect()
{
    m_data.clear();
    m_client_ptr.release();
    m_client_ptr = nullptr;    
}

mongocxx::database &mongo_connect::get_db(const std::string & db_name)
{
    if (m_data.find(db_name) == m_data.end())
    {
        m_data.emplace(db_name, (*m_client_ptr)[db_name]);
    }
    return m_data[db_name];
}

mongocxx::collection mongo_connect::get_coll(const std::string &db_name, const std::string &coll_name)
{
    auto m_db = get_db(db_name);
    auto collections = m_db.list_collections();
    bool is_exist = false;
    for (const auto &coll : collections)
    {
        if (coll["name"].get_string().value == coll_name)
        {
            is_exist = true;
            break;
        }
    }
    if (!is_exist)
    {
        // 创建集合
        m_db.create_collection(coll_name);
    }
    return m_db[coll_name];
}