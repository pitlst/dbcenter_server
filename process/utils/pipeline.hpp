#ifndef DBS_PIPELINE_INCLUDE
#define DBS_PIPELINE_INCLUDE

#include <memory>
#include <string>

#include "mongo.hpp"

namespace dbs
{
    // 管道类，用于维护一个消息队列
    class pipeline
    {
    public:
        pipeline(const std::string & node_name);
        ~pipeline() = default;

        // 接收消息
        bool recv();
        // 发送消息
        void send();
    private:
        const std::string m_db_name = "public"; 
        const std::string m_send_name = "mq_send";
        const std::string m_recv_name = "mq_recv";
        // 需要监控的节点名称
        std::string m_name;
        // 数据库集合
        std::unique_ptr<mongocxx::pool::entry> m_client_ptr = nullptr;
        std::unique_ptr<mongocxx::collection> m_send_coll_ptr = nullptr;
        std::unique_ptr<mongocxx::collection> m_recv_coll_ptr = nullptr;
    };
}

#endif