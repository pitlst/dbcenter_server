#include <algorithm>
#include <iterator>

#include "thread_pool.hpp"
#include "general.hpp"
#include "dag.hpp"


using namespace dbs;

dag_scheduler::dag_scheduler(const toml::value &config_define, const json & total_tasks)
{
    auto data = toml::parse(std::string(PROJECT_PATH) + "../source/config/process_scheduler.toml");
    min_sync_interval = toml::get<std::size_t>(data["min_sync_interval"]);
    wait_sync_interval = toml::get<std::size_t>(data["wait_sync_interval"]);

    make_deps(total_tasks);
}

dag_scheduler::~dag_scheduler()
{

}

void dag_scheduler::run()
{
    auto ipc_ndoe_names = get_notice();
    auto need_run_node_names = get_deps(get_runned());
    std::set_union(ipc_ndoe_names.begin(),ipc_ndoe_names.end(),need_run_node_names.begin(),need_run_node_names.end(), std::insert_iterator(need_run_node_names, need_run_node_names.end()));
    auto need_run_nodes = make_node(need_run_node_names);
    // 这里实际上有一个拷贝，一定要先存到running_nodes中再根据running_nodes中的节点发送执行函数
    // 不然通知变量通知的不正确，检测不到执行完成
    for (const auto & ch : need_run_nodes)
    {
        auto temp_node = running_nodes.emplace_back(ch);
        // thread_pool::instance().submit_lambda(temp_node);
    }
}

std::unordered_set<std::string> dag_scheduler::get_notice()
{
    // socket_buffer += MYSOCKET.get();
    std::vector<std::string> split_str = split_string(socket_buffer, ';');
    socket_buffer = split_str[split_str.size() - 1];
    split_str.pop_back();

    std::unordered_set<std::string> temp_ndoes;
    for (const auto & ch : split_str)
    {
        temp_ndoes.emplace(ch);
    }
    return temp_ndoes;
}

std::unordered_set<std::string> dag_scheduler::get_runned()
{
    std::unordered_set<std::string> runned_nodes;
    auto it = running_nodes.begin();
    while (it != running_nodes.end())
    {
        if(it->is_closed == true)
        {
            runned_nodes.emplace(it->name);
            it = running_nodes.erase(it);
        }
        else
        {
            it++;
        }
    }
    return runned_nodes;
}

std::unordered_set<std::string> dag_scheduler::get_deps(const std::unordered_set<std::string> & runned_nodes)
{
    std::unordered_set<std::string> temp_name;
    for (const auto & ch : runned_nodes)
    {
        if (node_deps.find(ch) != node_deps.end())
        {
            std::set_union(temp_name.begin(),temp_name.end(),node_deps[ch].begin(),node_deps[ch].end(), std::insert_iterator(temp_name, temp_name.end()));
        }
    }
    return temp_name;
}

void dag_scheduler::make_deps(const json & total_tasks)
{
    for (const auto & ch : total_tasks)
    {
        if (ch["type"] == "process")
        {
            for (const auto & ch_ : ch["next_name"])
            {
                node_deps[ch_].emplace(ch["name"]);
            }
        }
    }
}