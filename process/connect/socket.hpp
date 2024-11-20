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

    private:
        socket();
        ~socket();

        // winsokcet的操作状态返回
        int iResult;
        // 接受字符串的字节长度
        int recvbuflen;

        // winsocket需要的变量
        WSADATA wsaData;
        SOCKET ListenSocket = INVALID_SOCKET;
        SOCKET ClientSocket = INVALID_SOCKET;
        struct addrinfo *result = NULL;
        struct addrinfo hints;
    };
}


#endif