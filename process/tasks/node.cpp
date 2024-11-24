#include <chrono>

#include "node.hpp"
#include "process/person.hpp"

using namespace dbs;

node::node(const node & node_): name(node_.name), func(node_.func)
{
}

node::node(node && node_): name(node_.name), func(node_.func)
{

}

node& node::operator = (const node & node_)
{
    name = node_.name;
    func = node_.func;
    return *this;
}

node& node::operator = (node && node_)
{
    name = node_.name;
    func = node_.func;
    return *this;
}

void node::operator ()()
{
    LOGGER.info(name, "开始计算");
    auto beforeTime = std::chrono::steady_clock::now();
    func();
    auto afterTime = std::chrono::steady_clock::now();
    auto duration_second = std::to_string(std::chrono::duration<double>(afterTime - beforeTime).count());
    LOGGER.debug(name, "计算耗时" + duration_second);
    LOGGER.info(name, "结束计算");
    is_closed = true;
}

bool node::operator==(const node &node_) const
{
    return name == node_.name;
}

node_public_buffer &node_public_buffer::instance()
{
    static node_public_buffer m_self;
    return m_self;
}

node dbs::make_node_one(const std::string& node_name)
{
    node temp_node;
    // 这里维护一张表，实际上就是ORM(对象关系映射)
    // 就是json中定义的process节点和lambda函数的映射关系
    if (node_name == "人员处理")
    {
        temp_node = make_person_node(node_name);
    }
    return temp_node;
}

std::unordered_set<node> dbs::make_node(const std::unordered_set<std::string> &node_)
{
    std::unordered_set<node> total_node_;
    for (const auto & ch : node_)
    {
        total_node_.emplace(make_node_one(ch));
    }
    return total_node_;
}

void dbs::check_node_define(const std::unordered_set<node> &input_node)
{
}
