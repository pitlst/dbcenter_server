#include "sql_builder.hpp"

using namespace dbs;

sql_builder &sql_builder::init(const std::string &query)
{
    this->m_sql = query;
    return *this;
}

sql_builder &sql_builder::add(const std::string &condition, const std::string &logic)
{
    if (!condition.empty())
    {
        if (this->m_conditions.empty())
        {
            this->m_conditions.push_back(condition); // 第一个条件
        }
        else
        {
            this->m_conditions.push_back(logic + " " + condition); // 添加逻辑连接符
        }
    }
    return *this;
}

sql_builder &sql_builder::add(const std::vector<std::string> &conditions, const std::vector<std::string> &logics)
{
    if (logics.size() != conditions.size() - 1)
    {
        throw std::logic_error("生成sql时条件和逻辑关系符的个数关系不正确");
    }
    if (!conditions.empty())
    {
        for (size_t i = 0; i < conditions.size(); i++)
        {
            std::string temp_str; 
            if (i == 0)
            {
                temp_str += "(" + conditions[i];
            }
            else
            {
                temp_str += (logics[i-1] + " " + conditions[i]);
            }
            if (i == conditions.size() - 1)
            {
                temp_str += ")";
            }
            this->m_conditions.push_back(temp_str);
        }
    }
    return *this;
}

std::string sql_builder::build()
{
    if (!m_conditions.empty())
    {
        std::ostringstream oss;
        oss << this->m_sql << "\n" << "WHERE ";
        for (size_t i = 0; i < m_conditions.size(); ++i)
        {
            oss << m_conditions[i];
            if (i < m_conditions.size() - 1)
            {
                oss << " "; // 添加空格分隔条件
            }
        }
        this->m_sql = oss.str();
    }
    return this->m_sql;
}

void sql_builder::clear()
{
    this->m_sql.clear();
    this->m_conditions.clear();
}