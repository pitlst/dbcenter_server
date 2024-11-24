#ifndef DBS_SOCKET_INCLUDE
#define DBS_SOCKET_INCLUDE

#include <string>
#include <memory>

#include "asio.hpp"
#include "asio/placeholders.hpp"
#include "toml.hpp"

namespace dbs
{
    class mysocket
    {
    public:
        // 获取单实例对象
        static mysocket &instance();
        // 获取socket通信传输的字符串
        std::string get();
        // 连接socket，阻塞直到联通
        void connect();
        // 断开socket，并清空资源
        void close();

    private:
        mysocket();
        ~mysocket();

        // 默认链接的ip
        std::string ip;
        // 默认连接的端口号
        int port;
        // socket接收的缓冲区大小
        int buffer_size;

        // winsocket需要的变量
        asio::io_service m_io;
        std::shared_ptr<asio::ip::tcp::socket> m_socket_ptr;
    };
}

// log的全局引用简写
#define MYSOCKET dbs::mysocket::instance()


#endif