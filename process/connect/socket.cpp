#include <exception>

#include "logger.hpp"
#include "socket.hpp"

// 因为头文件中定义过socket，所以不能使用using namespace

dbs::socket &dbs::socket::instance()
{
    static socket m_self;
    return m_self;
}

// 获取socket通信传输的字符串
std::string dbs::socket::get()
{
    if (!is_connect)
    {
        LOGGER.error("process", "sokcet未联通，正在尝试重新连接");
        connect();
    }
    
    result_lable = recv(ClientSocket, recv_buffer, 1024, 0);
    if (result_lable > 0)
    {
        return std::string(recv_buffer);
    }
    else if (result_lable == 0)
    {
        is_connect = false;
        LOGGER.info("process", "sokcet客户端断开连接");
        close();
        return "";
    }
    else
    {
        closesocket(ClientSocket);
        WSACleanup();
        throw_error_l("recv");
        // 为解决路径返回值警告，实际不会走到这，而是直接抛出异常终止
        return "";
    }
}

void dbs::socket::connect(int input_port)
{
    // 输入默认值使用初始化时读入的配置
    if (input_port == -1)
    {
        input_port = port;
    }

    // 初始化 Winsock
    result_lable = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (result_lable != 0)
    {
        throw_error("WSAStartup");
    }

    ZeroMemory(&hints, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags = AI_PASSIVE;

    // 解析服务器地址和端口
    result_lable = getaddrinfo(NULL, std::to_string(port).c_str(), &hints, &result);
    if (result_lable != 0)
    {
        WSACleanup();
        throw_error("getaddrinfo");
    }

    // 为服务器创建一个socket以监听客户端连接。
    // 这里需要指定一下命名空间，因为和类的构造函数重名了
    ListenSocket = ::socket(result->ai_family, result->ai_socktype, result->ai_protocol);
    if (ListenSocket == INVALID_SOCKET)
    {
        freeaddrinfo(result);
        WSACleanup();
        throw_error_l("socket");
    }

    // Setup the TCP listening socket
    result_lable = bind(ListenSocket, result->ai_addr, (int)result->ai_addrlen);
    if (result_lable == SOCKET_ERROR)
    {
        freeaddrinfo(result);
        closesocket(ListenSocket);
        WSACleanup();
        throw_error_l("bind");
    }

    freeaddrinfo(result);

    result_lable = listen(ListenSocket, SOMAXCONN);
    if (result_lable == SOCKET_ERROR)
    {
        closesocket(ListenSocket);
        WSACleanup();
        throw_error_l("listen");
    }

    // Accept a client socket
    ClientSocket = accept(ListenSocket, NULL, NULL);
    if (ClientSocket == INVALID_SOCKET)
    {
        closesocket(ListenSocket);
        WSACleanup();
        throw_error_l("accept");
    }

    // No longer need server socket
    closesocket(ListenSocket);

    is_connect = true;
}

void dbs::socket::close()
{
    is_connect = false;

    // shutdown the connection since we're done
    result_lable = shutdown(ClientSocket, SD_SEND);
    if (result_lable == SOCKET_ERROR)
    {
        closesocket(ClientSocket);
        WSACleanup();
        throw_error_l("shutdown");
    }

    // cleanup
    closesocket(ClientSocket);
    WSACleanup();
}

dbs::socket::socket()
{
    auto data = toml::parse(std::string(PROJECT_PATH) + "../source/config/sync_scheduler.toml");
    port = toml::get<int>(data["socket_port"]);
    connect(port);
}

dbs::socket::~socket()
{
    close();
}

void dbs::socket::throw_error(const std::string &func_name)
{
    std::string error_msg = func_name + "出现问题，错误码" + std::to_string(result_lable);
    LOGGER.error("process", error_msg);
    // throw std::runtime_error(error_msg);
}

void dbs::socket::throw_error_l(const std::string &func_name)
{
    std::string error_msg = func_name + "出现问题，错误码" + std::to_string(WSAGetLastError());
    LOGGER.error("process", error_msg);
    // throw std::runtime_error(error_msg);
}