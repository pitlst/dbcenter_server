#include <algorithm>
#include <iostream>
#include <iterator>

#include "thread_pool.hpp"
#include "general.hpp"
#include "dag.hpp"
#include "socket.hpp"

using namespace dbs;

dag_scheduler::dag_scheduler(const json &total_tasks)
{
    auto data = toml::parse(std::string(PROJECT_PATH) + "../source/config/process_scheduler.toml");
    min_sync_interval = toml::get<std::size_t>(data["min_sync_interval"]);
    wait_sync_interval = toml::get<std::size_t>(data["wait_sync_interval"]);
    make_deps(total_tasks);
}

dag_scheduler::~dag_scheduler()
{
    // 等待线程池中所有的任务运行完成
    // thread_pool::instance().shutdown();
}

void dag_scheduler::run()
{
    while (true)
    {
        auto ipc_ndoe_names = get_notice();
        auto need_run_node_names = get_deps(get_runned());
        std::cout << need_run_node_names.size() << std::endl;
        std::set_union(ipc_ndoe_names.begin(), ipc_ndoe_names.end(), need_run_node_names.begin(), need_run_node_names.end(), std::insert_iterator(need_run_node_names, need_run_node_names.end()));
        for (const auto &node_name : need_run_node_names)
        {
            std::cout << "nnode_name" << node_name << std::endl;
            running_nodes.emplace_back(node_name);
            thread_pool::instance().submit(get_func(), node_name);
        }
    }
}

std::unordered_set<std::string> dag_scheduler::get_notice()
{
    socket_buffer = MYSOCKET.get();
    std::vector<std::string> split_str = split_string(socket_buffer, "节点执行完成");
    std::unordered_set<std::string> temp_ndoes;
    for (const auto &ch : split_str)
    {
        // 检查节点是否在注册的节点中，不要的全部丢弃
        if (nodes.find(ch) != nodes.end())
        {
            temp_ndoes.emplace(ch);
        }
    }
    return temp_ndoes;
}

std::unordered_set<std::string> dag_scheduler::get_runned()
{
    std::unordered_set<std::string> runned_nodes;
    auto it = running_nodes.begin();
    while (it != running_nodes.end())
    {
        if (NODE_NOTICE[*it] == true)
        {
            NODE_NOTICE[*it] = false;
            runned_nodes.emplace(*it);
            it = running_nodes.erase(it);
        }
        else
        {
            it++;
        }
    }
    return runned_nodes;
}

std::unordered_set<std::string> dag_scheduler::get_deps(const std::unordered_set<std::string> &runned_nodes)
{
    std::unordered_set<std::string> temp_name;
    for (const auto &ch : runned_nodes)
    {
        if (node_deps.find(ch) != node_deps.end() && !node_deps[ch].empty())
        {
            std::set_union(temp_name.begin(), temp_name.end(), node_deps[ch].begin(), node_deps[ch].end(), std::insert_iterator(temp_name, temp_name.end()));
        }
    }
    return temp_name;
}

void dag_scheduler::make_deps(const json &total_tasks)
{
    for (const auto &ch : total_tasks)
    {
        if (ch["type"] == "process")
        {
            nodes.emplace(ch["name"]);
            if (!ch["next_name"].empty())
            {
                for (const auto &ch_ : ch["next_name"])
                {
                    node_deps[ch_].emplace(ch["name"]);
                }
            }
        }
    }
    // 检查所有节点的日志集合是否创建
    for (const auto & ch : nodes)
    {
        LOGGER.create_time_collection(ch);
    }
}