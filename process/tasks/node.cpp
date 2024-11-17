#include "node.hpp"
#include "general.hpp"

using namespace dbs;

node_base::node_base(const json & node_define)
{
    source_config = node_define["source"];
    target_config = node_define["target"];
    name = node_define["name"];
    type = node_define["type"];
    auto temp_value = node_define["next_name"];
    for (size_t i = 0; i < temp_value.size(); i++)
    {
        next_name.emplace_back(temp_value[i]);
    }
    LOGGER.create_time_collection(name);
}

node_base::node_base(const std::string &name, const std::string &type, const std::vector<std::string> &next_name) : name(name), type(type), next_name(next_name) 
{
    LOGGER.create_time_collection(name);
}

node_base &node_base::emplace(std::function<void()> input_warpper)
{
    func_warpper = std::move(input_warpper);
    return *this;
}