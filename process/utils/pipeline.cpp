#include <chrono>
#include "bsoncxx/json.hpp"
#include "bsoncxx/builder/basic/document.hpp"

#include "pipeline.hpp"

using namespace dbs;
using namespace std::chrono_literals;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

pipeline &pipeline::instance()
{
    static pipeline m_instance;
    return m_instance;
}

pipeline::pipeline()
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

pipeline::~pipeline()
{
    this->m_send_coll_ptr.release();
    this->m_recv_coll_ptr.release();
    this->m_send_coll_ptr = nullptr;
    this->m_recv_coll_ptr = nullptr;
}

bool pipeline::recv(const std::string &name)
{
    auto it = this->m_recv_set.find(name);
    bool has_value = it != this->m_recv_set.end();
    if (has_value)
    {
        // 通知pipeline的事件循环已经接收到消息
        this->m_recv_queue.push(name);
    }
    return has_value;
}

void pipeline::send(const std::string &name)
{
    // 通知pipeline的事件循环发送消息
    this->m_send_queue.push(name);
}

std::function<void()> pipeline::get_run_func()
{
    return [&]()
    {
        while (true)
        {
            // 处理发送消息
            std::string temp_name;
            if (this->m_send_queue.try_pop(temp_name))
            {
                auto now = std::chrono::system_clock::now();
                this->m_recv_coll_ptr->insert_one(
                    make_document(
                        kvp("timestamp", bsoncxx::types::b_date{now}),
                        kvp("node_name", temp_name),
                        kvp("is_process", false)));
            }
            // 接收消息队列消息并缓存
            auto cursor = this->m_send_coll_ptr->find(make_document(kvp("is_process", false)));
            for (const auto &ch : cursor)
            {
                this->m_recv_set.emplace(ch["node_name"].get_string().value);
            }
            // 处理接收消息的完成
            if (this->m_recv_queue.try_pop(temp_name))
            {
                this->m_send_coll_ptr->update_many(
                    make_document(kvp("node_name", temp_name)),
                    make_document(kvp("$set", make_document(kvp("is_process", true)))));
                this->m_recv_set.unsafe_erase(this->m_recv_set.find(temp_name));
            }
            // 暂停一段时间，防止过于占用cpu
            std::this_thread::sleep_for(50ms);
        }
    };
}