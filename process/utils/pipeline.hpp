#ifndef DBS_LOGGER_INCLUDE
#define DBS_LOGGER_INCLUDE

#include <memory>
#include <string>

#include "mongo.hpp"

namespace dbs
{
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
        std::string m_name;
        std::unique_ptr<mongocxx::pool::entry> m_client_ptr;
        std::unique_ptr<mongocxx::collection> m_send_coll_ptr;
        std::unique_ptr<mongocxx::collection> m_recv_coll_ptr;
    };
}

#endif