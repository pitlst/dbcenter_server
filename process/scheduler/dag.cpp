#include "dag.hpp"
#include "socket.hpp"
#include "thread_pool.hpp"

using namespace dbs;

dag_scheduler::dag_scheduler(const toml::value & config_define)
{

}

dag_scheduler::~dag_scheduler()
{

}

void dag_scheduler::set_node(const node & input_node)
{
    nodes.emplace(input_node.name, std::make_pair(input_node.deps, input_node.func));
}

void dag_scheduler::set_node(const std::unordered_set<node> & input_nodes)
{
    for (const auto & ch : input_nodes)
    {
        set_node(ch);
    }
}

void dag_scheduler::run()
{

}

std::vector<std::function<void()>> dag_scheduler::get_run_node()
{

}

void dag_scheduler::update_node()
{

}

void dag_scheduler::get_notice()
{

}