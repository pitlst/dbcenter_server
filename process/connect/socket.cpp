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
    
}

void mysocket::connect()
{
    asio::ip::tcp::endpoint ep(
        asio::ip::address::from_string(ip),
        port
    );
    m_socket_ptr = std::make_shared<asio::ip::tcp::socket>(m_io, ep);
    m_socket_ptr->connect(ep);
}

void mysocket::close()
{

}

mysocket::mysocket()
{
    auto data = toml::parse(std::string(PROJECT_PATH) + "../source/config/process_scheduler.toml");
    ip = toml::get<std::string>(data["socket_ip"]);
    port = toml::get<int>(data["socket_port"]);
    buffer_size = toml::get<int>(data["socket_buffer_size"]);
    connect();
}

mysocket::~mysocket()
{
    close();
}
