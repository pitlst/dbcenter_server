#ifndef DBS_NODE_INCLUDE
#define DBS_NODE_INCLUDE

#include <string>
#include <vector>
#include <map>
#include <unordered_set>
#include <functional>
#include <future>
#include <any>
#include <atomic>

#include "general.hpp"
#include "logger.hpp"

namespace dbs
{
    // 这里用于定义函数上下文传递的全局变量，使用单例模式维护线程安全，
    // 一定要做好人工检查，因为没有编译器检查了
    // 相当于手动维护的节点堆空间
    class node_public_buffer
    {
    public:
        // 获取单实例对象
        static node_public_buffer &instance();
        // 对应节点的通知，用于表示是否运行完成
        std::map<std::string, std::atomic<bool>> m_is_closed;
        // 变量的存储位置
        std::map<std::string, std::any> m_buffer;

    private:
        // 禁止外部构造与析构
        node_public_buffer() = default;
        ~node_public_buffer() = default;
    };

    // 单个节点的执行方法获取
    std::function<void(std::string)> get_func();
    // 选择当前实际运算的逻辑
    void dbs::select_func(const std::string &name);
}

// 节点是否完成的通知变量的全局引用简写
#define NODE_NOTICE dbs::node_public_buffer::instance().m_is_closed
// buffer的全局引用简写
#define NODE_BUFFER dbs::node_public_buffer::instance().m_buffer

#endif