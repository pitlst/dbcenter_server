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

}

dbs::socket::socket()
{
    auto data = toml::parse(std::string(PROJECT_PATH) + "../source/config/connect.toml")["数据处理服务存储"];
}

dbs::socket::~socket()
{

}