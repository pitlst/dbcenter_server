#include "sql_builder.hpp"

using namespace dbs;

sql_builder &sql_builder::init(const std::string &query)
{
    this->m_sql = query;
    return *this;
}

sql_builder &sql_builder::add(const std::string &condition, const std::string &logic = "AND")
{
    if (!condition.empty())
    {
        if (this->conditions.empty())
        {
            this->conditions.push_back(condition); // 第一个条件
        }
        else
        {
            this->conditions.push_back(logic + " " + condition); // 添加逻辑连接符
        }
    }
    return *this;
}

std::string sql_builder::build()
{
    if (!conditions.empty())
    {
        std::ostringstream oss;
        oss << this->m_sql << " WHERE ";
        for (size_t i = 0; i < conditions.size(); ++i)
        {
            oss << conditions[i];
            if (i < conditions.size() - 1)
            {
                oss << " "; // 添加空格分隔条件
            }
        }
        this->m_sql = oss.str();
    }
    return this->m_sql + ";";
}