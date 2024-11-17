#include "node.hpp"
#include "general.hpp"
#include "odbc.hpp"

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
}

node_base::node_base(const std::string &name, const std::string &type, const std::vector<std::string> &next_name) : name(name), type(type), next_name(next_name) {}

node_base &node_base::emplace(std::function<void()> input_warpper)
{
    func_warpper = std::move(input_warpper);
}

sql_node::sql_node(const json & node_define): node_base(node_define)
{
    
    m_sql = read_file(PROJECT_PATH + "/" + source_config["sql"]);
    auto sql_process  = [](){

    }
}

mongojs_node::mongojs_node(const json & node_define): node_base(node_define)
{
    std::filesystem::path path;
    path = path / PROJECT_PATH / source_config["js"];
    m_js = read_file(path);
};