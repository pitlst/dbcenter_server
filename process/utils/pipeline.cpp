#include <chrono>
#include "bsoncxx/json.hpp"
#include "bsoncxx/builder/basic/document.hpp"

#include "pipeline.hpp"

using namespace dbs;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

pipeline::pipeline(const std::string &node_name) : m_name(node_name)
{
    // 检查集合是否创建
    bool recv_is_exist = false;
    bool send_is_exist = false;
    for (const auto &coll : this->m_database.list_collections())
    {
        recv_is_exist = recv_is_exist || coll["name"].get_string().value == m_send_name;
        send_is_exist = send_is_exist || coll["name"].get_string().value == m_recv_name;
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
    this->m_send_coll_ptr = std::make_unique<mongocxx::collection>(this->m_database[m_send_name]);
    this->m_recv_coll_ptr = std::make_unique<mongocxx::collection>(this->m_database[m_recv_name]);
}

bool pipeline::recv()
{
    bool has_value = false;
    auto cursor = this->m_send_coll_ptr->find(
        make_document(
            kvp("node_name", m_name),
            kvp("is_process", false)
            )
        );
    has_value = cursor.begin() != cursor.end();   
    if (has_value)
    {
        this->m_send_coll_ptr->update_many(
            make_document(kvp("node_name", m_name)),
            make_document(kvp("$set", make_document(kvp("is_process", true))))
        );
    }
    return has_value;
}

void pipeline::send()
{
    auto now = std::chrono::system_clock::now();
    this->m_recv_coll_ptr->insert_one(
        make_document(
            kvp("timestamp", bsoncxx::types::b_date{now}),
            kvp("node_name", m_name),
            kvp("is_process", false)));
}