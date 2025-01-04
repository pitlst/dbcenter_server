#ifndef DBS_PIPELINE_INCLUDE
#define DBS_PIPELINE_INCLUDE

#include <memory>
#include <string>

#include "mongo.hpp"

namespace dbs
{
    // 管道类，用于在消息队列中接收消息
    class pipeline
    {
    public:
        // 获取单实例对象
        static pipeline &instance();

        // 接收消息
        bool recv(const std::string & name);
        // 发送消息
        void send(const std::string & name);
        // 获取事件循环
        std::function<void()> get_run_func();

    private:
        // 禁止外部构造与析构
        pipeline();
        ~pipeline();

        const std::string m_db_name = "public"; 
        const std::string m_send_name = "mq_send";
        const std::string m_recv_name = "mq_recv";
        // 发送队列
        tbb::concurrent_queue<std::string> m_send_queue;
        // 接收队列
        tbb::concurrent_queue<std::string> m_recv_queue;
        // 接收消息的缓存
        tbb::concurrent_set<std::string> m_recv_set;
        // 数据库客户端
        mongocxx::pool::entry m_client = MONGO.init_client();
        mongocxx::database m_database = m_client[m_db_name];
        std::unique_ptr<mongocxx::collection> m_send_coll_ptr = nullptr;
        std::unique_ptr<mongocxx::collection> m_recv_coll_ptr = nullptr;

        // 这里有个小问题，就是数据库的指针是反着的，这与调度器中的定义对应
    };
}

// 管道的全局引用简写
#define PIPELINE dbs::pipeline::instance()

#endif