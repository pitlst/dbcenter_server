#ifndef DBS_DAG_INCLUDE
#define DBS_DAG_INCLUDE

#include <vector>
#include <unordered_set>

#include "toml.hpp"

#include "node.hpp"

namespace dbs
{
    class dag_scheduler
    {
    public:
        dag_scheduler(const toml::value & config_define);
        ~dag_scheduler();

        // 获取所有节点的定义
        void set_node(const node & input_node);
        void set_node(const std::unordered_set<node> & input_nodes);
        // 真正运行并派发节点的地方
        void run();

    private:
        // 获取当前可以执行的方法
        std::vector<std::function<void()>> get_run_node();
        // 更新节点信息
        void update_node();
        // 接收并处理ipc通信
        void get_notice();

    private:
        // 在整理好后的所有节点定义
        std::unordered_map<std::string, std::pair<std::unordered_set<std::string>, std::function<void()>>> nodes;
    };
}

#endif