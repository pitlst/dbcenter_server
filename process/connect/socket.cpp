#include <exception>

#include "logger.hpp"
#include "socket.hpp"

using namespace dbs;

mysocket &mysocket::instance()
{
    static mysocket m_self;
    return m_self;
}

// 获取socket通信传输的字符串
std::string mysocket::get()
{
    char recvbuf[1024];
    int recvbuflen = sizeof(recvbuf);

    // 接收数据
    iResult = ::recv(ClientSocket, recvbuf, recvbuflen, 0);
    if (iResult == SOCKET_ERROR)
    {
        int error = WSAGetLastError();
        if (error == WSAETIMEDOUT)
        {
            LOGGER.debug("process", "socket获取函数超时");
            return std::string(recvbuf);
        }
        else
        {
            LOGGER.error("process", "recv failed with error: " + std::to_string(error));
            throw std::logic_error("recv failed with error: " + std::to_string(error));
        }
    }
    else if (iResult == 0)
    {
        LOGGER.debug("process", "连接被客户端关闭");
        throw std::logic_error("连接被客户端关闭");
    }
    else
    {
        return std::string(recvbuf);
    }
}

void mysocket::send(std::string msg)
{
    auto sendbuf = msg.c_str();
    iResult = ::send(ClientSocket, sendbuf, strlen(sendbuf), 0);
    if (iResult == SOCKET_ERROR)
    {
        close();
        LOGGER.error("process", "send failed" + std::to_string(WSAGetLastError()));
        throw std::logic_error("send failed" + std::to_string(WSAGetLastError()));
    }
}

void mysocket::connect()
{
    // 初始化 Winsock
    iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (iResult != 0)
    {
        LOGGER.error("process", "WSAStartup failed with error: " + std::to_string(iResult));
        throw std::logic_error("WSAStartup failed with error: " + std::to_string(iResult));
    }
    ZeroMemory(&hints, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags = AI_PASSIVE;

    // 解析服务器地址和端口
    iResult = getaddrinfo(ip.c_str(), std::to_string(port).c_str(), &hints, &result);
    if (iResult != 0)
    {
        LOGGER.error("process", "getaddrinfo failed with error: " + std::to_string(iResult));
        WSACleanup();
        throw std::logic_error("getaddrinfo failed with error: " + std::to_string(iResult));
    }

    // 创建套接字
    ListenSocket = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
    if (ListenSocket == INVALID_SOCKET)
    {
        std::string err_str = "socket failed with error: " + std::to_string(WSAGetLastError());
        freeaddrinfo(result);
        WSACleanup();
        LOGGER.error("process", err_str);
        throw std::logic_error(err_str);
    }

    // 绑定套接字
    iResult = bind(ListenSocket, result->ai_addr, (int)result->ai_addrlen);
    if (iResult == SOCKET_ERROR)
    {
        std::string err_str = "bind failed with error: " + std::to_string(WSAGetLastError());
        freeaddrinfo(result);
        closesocket(ListenSocket);
        WSACleanup();
        LOGGER.error("process", err_str);
        throw std::logic_error(err_str);
    }

    freeaddrinfo(result);

    // 监听
    iResult = listen(ListenSocket, SOMAXCONN);
    if (iResult == SOCKET_ERROR)
    {
        std::string err_str = "listen failed with error: " + std::to_string(WSAGetLastError());
        closesocket(ListenSocket);
        WSACleanup();
        LOGGER.error("process", err_str);
        throw std::logic_error(err_str);
    }

    // 接受客户端连接
    ClientSocket = accept(ListenSocket, NULL, NULL);
    if (ClientSocket == INVALID_SOCKET)
    {
        std::string err_str = "accept failed with error: " + std::to_string(WSAGetLastError());
        closesocket(ListenSocket);
        WSACleanup();
        LOGGER.error("process", err_str);
        throw std::logic_error(err_str);
    }

    // 设置接收超时
    setsockopt(ClientSocket, SOL_SOCKET, SO_RCVTIMEO, (char *)&time_out, sizeof(time_out));
}

void mysocket::close()
{
    closesocket(ClientSocket);
    WSACleanup();
}

mysocket::mysocket()
{
    auto data = toml::parse(std::string(PROJECT_PATH) + "../source/config/process_scheduler.toml");
    ip = toml::get<std::string>(data["socket_ip"]);
    port = toml::get<int>(data["socket_port"]);
    time_out = int(toml::get<float>(data["sokcet_time_out"]) * 1000);
    connect();
}

mysocket::~mysocket()
{
    close();
}
