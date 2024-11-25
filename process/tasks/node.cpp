#include <chrono>

#include "node.hpp"
#include "process/person.hpp"

using namespace dbs;

node_public_buffer &node_public_buffer::instance()
{
    static node_public_buffer m_self;
    return m_self;
}

std::function<void(std::string)> dbs::get_func()
{
    return [](std::string name_)
    {
        NODE_NOTICE[name_] = false;
        LOGGER.info(name_, "开始计算");
        auto beforeTime = std::chrono::steady_clock::now();
        select_func(name_);
        auto afterTime = std::chrono::steady_clock::now();
        auto duration_second = std::to_string(std::chrono::duration<double>(afterTime - beforeTime).count());
        LOGGER.debug(name_, "计算耗时" + duration_second);
        LOGGER.info(name_, "结束计算");
        NODE_NOTICE[name_] = true;
    };
}

void dbs::select_func(const std::string &name)
{
    // 这里维护一张表，实际上就是ORM(对象关系映射)
    // 就是json中定义的process节点和lambda函数的映射关系
    // 放在这里的同时将对于执行函数的运行时判断变为多线程的减小耗时
    if (name == "人员处理")
    {
        process_person();
    } 
    else
    {
        LOGGER.warn("process", "没有对应的节点实现" + name);
    }
}