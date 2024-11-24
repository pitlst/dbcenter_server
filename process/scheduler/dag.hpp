#ifndef DBS_DAG_INCLUDE
#define DBS_DAG_INCLUDE

#include <vector>
#include <deque>
#include <unordered_set>

#include "json.hpp"
using json = nlohmann::json;
#include "toml.hpp"

#include "node.hpp"

namespace dbs
{
    class dag_scheduler
    {
    public:
        dag_scheduler(const json & total_tasks);
        ~dag_scheduler();
        // 真正运行并派发节点的地方
        void run();

    private:
        // 接收并处理ipc通信，获取对应需要启动的节点
        std::unordered_set<std::string> get_notice();
        // 获取当前已经运行完的节点的名称，并将其从节点队列中移除
        std::unordered_set<std::string> get_runned();
        // 根据已经运行完成的节点获取下一步需要触发的节点
        std::unordered_set<std::string> get_deps(const std::unordered_set<std::string> & runned_nodes);
        // 整理并保存节点相关的依赖信息
        void make_deps(const json & total_tasks);

        // 任务轮询也就是数据同步的最小间隔，单位秒
        std::size_t min_sync_interval;
        // 无任务时等待时间，单位秒
        std::size_t wait_sync_interval;
        // socket通信的未完全接收的节点信息缓存
        std::string socket_buffer;
        // 当前正在运行的所有节点
        std::deque<node> running_nodes;
        // 所有节点和他的被依赖关系，注意这里的依赖关系和tasks.json文件中定义的是相反的
        std::unordered_map<std::string, std::unordered_set<std::string>> node_deps;
    };
}

#endif