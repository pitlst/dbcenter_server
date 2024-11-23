#include "dag.hpp"
#include "socket.hpp"
#include "thread_pool.hpp"
#include "general.hpp"

using namespace dbs;

dag_scheduler::dag_scheduler(const toml::value &config_define)
{
    auto data = toml::parse(std::string(PROJECT_PATH) + "../source/config/process_scheduler.toml");
    min_sync_interval = toml::get<std::size_t>(data["min_sync_interval"]);
    wait_sync_interval = toml::get<std::size_t>(data["wait_sync_interval"]);
}

dag_scheduler::~dag_scheduler()
{

}

void dag_scheduler::set_node(node &input_node)
{
    nodes.emplace(input_node.name, input_node);
    for(const auto & ch : input_node.deps)
    {
        node_deps[ch].emplace(input_node.name);
    }
}

void dag_scheduler::set_node(std::unordered_set<node> &input_nodes)
{
    for (auto &ch : input_nodes)
    {
        nodes.emplace(ch.name, ch);
        for(const auto & ch_ : ch.deps)
        {
            node_deps[ch_].emplace(ch.name);
        }
    }
}

void dag_scheduler::run()
{
    while (true)
    {
        get_notice();
        auto need_run_nodes = get_run_node();
        for (const auto & node_name : need_run_nodes)
        {
            auto temp_packed = pack_func(nodes[node_name]);
            auto f = std::bind(&node::operator(), &(nodes[node_name]));
            all_node_result[node_name] = temp_packed.second;
            thread_pool::instance().submit(temp_packed.first);
        }
        update_after_run();
    }
    
}

std::vector<std::string> dag_scheduler::get_run_node()
{
    std::vector<std::string> temp_nodes;
    for (auto& [key , node_]: nodes)
    {
        if (node_.m_status == node::need_run)
        {
            temp_nodes.emplace_back(key);
        }
    }
    return temp_nodes;
}

void dag_scheduler::update_after_run()
{
    
    for (auto& [key , node_]: nodes)
    {
        // 对所有已经进行完的类变更状态
        if (node_.m_status == node::end_run)
        {
            node_.m_status = node::not_run;
            // 对依赖其的节点变更状态准备运行
            for (const auto & ch_ : node_deps[key])
            {
                auto &node = nodes[ch_];
                if (node.m_status == node::not_run)
                {
                    node.m_status = node::need_run;
                }
            }
        }
    }
}

void dag_scheduler::get_notice()
{
    socket_buffer += MYSOCKET.get();
    std::vector<std::string_view> split_str = split_string(socket_buffer, ';');
    socket_buffer = split_str[split_str.size() - 1];
    std::vector<std::string_view> node_str;
    std::vector<std::string_view> inter_node;
    for (auto& [key , _]: nodes)
    {
        node_str.emplace_back(key);
    }
    std::set_intersection(split_str.begin(), split_str.end(), node_str.begin(), node_str.end(), std::back_inserter(inter_node));
    for (const auto &ch : inter_node)
    {
        auto &node = nodes[std::string(ch)];
        if(node.m_status == node::not_run)
        {
            node.m_status = node::need_run;
        }
    }
}