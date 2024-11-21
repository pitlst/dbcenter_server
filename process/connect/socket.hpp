#ifndef DBS_SOCKET_INCLUDE
#define DBS_SOCKET_INCLUDE

#include <string>

#include <winsock2.h>
#include <ws2tcpip.h>

#include "toml.hpp"

namespace dbs
{
    class socket
    {
    public:
        // 获取单实例对象
        static socket &instance();
        // 获取socket通信传输的字符串
        std::string get();
        // 连接socket，阻塞直到联通
        void connect(int input_port = -1);
        // 断开socket，并清空资源
        void close();

        // 用于表示是否处于连接状态的标志位
        bool is_connect = false;
    private:
        socket();
        ~socket();

        // 抛出异常
        void throw_error(const std::string & func_name);
        void throw_error_l(const std::string & func_name);

        // winsokcet的操作状态返回
        int result_lable;
        // 接受字符串的缓冲区
        char recv_buffer[1024];
        // 默认连接的端口号
        int port;

        // winsocket需要的变量
        WSADATA wsaData;
        SOCKET ListenSocket = INVALID_SOCKET;
        SOCKET ClientSocket = INVALID_SOCKET;
        struct addrinfo *result = NULL;
        struct addrinfo hints;
    };
}


#endif