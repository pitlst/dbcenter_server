#include <chrono>

#include "pipeline.hpp"

using namespace dbs;

pipeline::pipeline(const std::string & node_name): m_name(node_name)
{
    // 连接数据库
    m_client_ptr = std::make_unique<mongocxx::pool::entry>(MONGO.m_pool_ptr->acquire());
    auto m_database = (*m_client_ptr)["public"];
    // 检查集合是否创建
    bool recv_is_exist = false;
    bool send_is_exist = false;
    for (const auto &coll : m_database.list_collections())
    {
        recv_is_exist = coll["name"].get_string().value == "mq_recv";
        send_is_exist = coll["name"].get_string().value == "mq_send";
        if (recv_is_exist && send_is_exist)
        {
            break;
        }
    }
    if (!recv_is_exist || !send_is_exist)
    {
        throw std::logic_error("消息队列对应的数据库未创建");
    }
    // 获取集合
    m_send_coll_ptr = std::make_unique<mongocxx::collection>(m_database["mq_send"]);
    m_recv_coll_ptr = std::make_unique<mongocxx::collection>(m_database["mq_recv"]);
}

bool pipeline::recv()
{

}

void pipeline::send()
{
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;
    auto now = std::chrono::system_clock::now();
    m_recv_coll_ptr->insert_one(
        make_document(
            kvp("timestamp", bsoncxx::types::b_date{now}),
            kvp("node_name", m_name),
            kvp("is_process", false)
        )
    );
}