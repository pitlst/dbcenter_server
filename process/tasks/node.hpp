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
    // 节点的基类定义
    struct node
    {
        node() = default;
        // 拷贝与移动构造的特殊定义
        node(const node &node_);
        node(node &&node_);
        node &operator=(const node &node_);
        node &operator=(node &&node_);

        // 节点的执行方法，节点通用的一些包装放在这里
        void operator()();
        // 为hash添加相等的对比
        bool operator==(const node &node_) const;

        // 节点名称
        std::string name;
        // 节点执行的逻辑
        // 在这里要求所有的节点都不能有函数上下文的输入输出值
        // 这些数据的传递要么通过mongo的中转数据库，要么通过全局共享的线程安全变量传递，通过将变量的生命周期扩展到全程序来保证变量的生命周期安全
        std::function<void()> func;
        // 标记节点是否完成的标志位
        std::atomic<bool> is_closed = false;
    };

    // 这里用于定义函数上下文传递的全局变量，使用单例模式维护线程安全，
    // 一定要做好人工检查，因为没有编译器检查了
    // 相当于手动维护的节点堆空间
    class node_public_buffer
    {
    public:
        // 获取单实例对象
        static node_public_buffer &instance();
        // 变量的存储位置
        std::map<std::string, std::any> m_buffer;

    private:
        // 禁止外部构造与析构
        node_public_buffer() = default;
        ~node_public_buffer() = default;
    };

    // 节点的工厂类
    node make_node_one(const std::string &node_name);
    std::unordered_set<node> make_node(const std::unordered_set<std::string> &node_);

    // 检查所有定义的节点是否符合要求，不符合则抛出异常
    void check_node_define(const std::unordered_set<node> &input_node);
}

// buffer的全局引用简写
#define NODE_BUFFER dbs::node_public_buffer::instance().m_buffer

namespace std
{
    template <>
    class hash<dbs::node>
    {
        // 偏特化（这里使用了标准库已经提供的hash偏特化类hash<string>，hash<int>()）
    public:
        size_t operator()(const dbs::node &p) const
        {
            return hash<std::string>()(p.name + dbs::random_gen(8));
        }
    };
}

#endif