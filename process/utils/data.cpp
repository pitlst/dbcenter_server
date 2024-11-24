#include "data.hpp"

#include <sstream>

using namespace dbs;

data::data()
{
}
data::data(bool input_value) : m_value(input_value)
{
}
data::data(int input_value) : m_value(input_value)
{
}
data::data(long input_value) : m_value(input_value)
{
}
data::data(double input_value) : m_value(input_value)
{
}
data::data(const char *input_value) : m_value(input_value)
{
}
data::data(const std::string &input_value) : m_value(input_value)
{
}
data::data(const data &input_value) : m_value(input_value.m_value)
{
}
data::data(data &&input_value) : m_value(input_value.m_value)
{
}

data &data::operator=(bool input_value)
{
    m_value = input_value;
    return *this;
}
data &data::operator=(int input_value)
{
    m_value = input_value;
    return *this;
}
data &data::operator=(long input_value)
{
    m_value = input_value;
    return *this;
}
data &data::operator=(double input_value)
{
    m_value = input_value;
    return *this;
}
data &data::operator=(const char *input_value)
{
    m_value = input_value;
    return *this;
}
data &data::operator=(const std::string &input_value)
{
    m_value = input_value;
    return *this;
}
data &data::operator=(const data &input_value)
{
    m_value = input_value.m_value;
    return *this;
}
data &data::operator=(data &&input_value)
{
    m_value = input_value.m_value;
    return *this;
}

data::operator bool() const
{
    if (std::holds_alternative<std::monostate>(m_value))
    {
        return false;
    }
    else if (std::holds_alternative<bool>(m_value))
    {
        return std::get<bool>(m_value);
    }
    else if (std::holds_alternative<int>(m_value))
    {
        return bool(std::get<int>(m_value));
    }
    else if (std::holds_alternative<double>(m_value))
    {
        return bool(std::get<double>(m_value));
    }
    else if (std::holds_alternative<std::string>(m_value))
    {
        return !(std::get<std::string>(m_value).empty());
    }
    else
    {
        throw std::logic_error("对data强制转换为bool时发生了未知错误");
    }
}
data::operator long() const
{
    if (std::holds_alternative<std::monostate>(m_value))
    {
        return 0;
    }
    else if (std::holds_alternative<bool>(m_value))
    {
        return int(std::get<bool>(m_value));
    }
    else if (std::holds_alternative<int>(m_value))
    {
        return std::get<int>(m_value);
    }
    else if (std::holds_alternative<double>(m_value))
    {
        return int(std::get<double>(m_value));
    }
    else if (std::holds_alternative<std::string>(m_value))
    {
        throw std::logic_error("对data强制转换为int时发生了错误:string不能转换为int");
    }
    else
    {
        throw std::logic_error("对data强制转换为int时发生了未知错误");
    }
}
data::operator float() const
{
    if (std::holds_alternative<std::monostate>(m_value))
    {
        return 0.;
    }
    else if (std::holds_alternative<bool>(m_value))
    {
        return float(std::get<bool>(m_value));
    }
    else if (std::holds_alternative<int>(m_value))
    {
        return float(std::get<int>(m_value));
    }
    else if (std::holds_alternative<double>(m_value))
    {
        return float(std::get<double>(m_value));
    }
    else if (std::holds_alternative<std::string>(m_value))
    {
        throw std::logic_error("对data强制转换为float时发生了错误:string不能转换为double");
    }
    else
    {
        throw std::logic_error("对data强制转换为float时发生了未知错误");
    }
}
data::operator double() const
{
    if (std::holds_alternative<std::monostate>(m_value))
    {
        return 0.;
    }
    else if (std::holds_alternative<bool>(m_value))
    {
        return double(std::get<bool>(m_value));
    }
    else if (std::holds_alternative<int>(m_value))
    {
        return double(std::get<int>(m_value));
    }
    else if (std::holds_alternative<double>(m_value))
    {
        return std::get<double>(m_value);
    }
    else if (std::holds_alternative<std::string>(m_value))
    {
        throw std::logic_error("对data强制转换为double时发生了错误:string不能转换为double");
    }
    else
    {
        throw std::logic_error("对data强制转换为double时发生了未知错误");
    }
}
data::operator std::string() const
{
    if (std::holds_alternative<std::monostate>(m_value))
    {
        return "null";
    }
    else if (std::holds_alternative<bool>(m_value))
    {
        auto temp = std::get<bool>(m_value);
        if (temp)
        {
            return "true";
        }
        else
        {
            return "false";
        }
    }
    else if (std::holds_alternative<int>(m_value))
    {
        return std::to_string(std::get<int>(m_value));
    }
    else if (std::holds_alternative<double>(m_value))
    {
        return std::to_string(std::get<double>(m_value));
    }
    else if (std::holds_alternative<std::string>(m_value))
    {
        return std::get<std::string>(m_value);
    }
    else
    {
        throw std::logic_error("对data强制转换为string时发生了未知错误");
    }
}

bool data::is_type(const std::string &input_type_name) const
{
    if (std::holds_alternative<std::monostate>(m_value))
    {
        return input_type_name == "null";
    }
    else if (std::holds_alternative<bool>(m_value))
    {
        return input_type_name == "bool";
    }
    else if (std::holds_alternative<int>(m_value))
    {
        return input_type_name == "int";
    }
    else if (std::holds_alternative<double>(m_value))
    {
        return input_type_name == "double" || input_type_name == "float";
    }
    else if (std::holds_alternative<std::string>(m_value))
    {
        return input_type_name == "string" || input_type_name == "str";
    }
    else
    {
        throw std::logic_error("is_type函数中:获取data内部类型时发生了未知错误");
    }
}
std::string data::get_type_name() const
{
    if (std::holds_alternative<std::monostate>(m_value))
    {
        return "null";
    }
    else if (std::holds_alternative<bool>(m_value))
    {
        return "bool";
    }
    else if (std::holds_alternative<int>(m_value))
    {
        return "int";
    }
    else if (std::holds_alternative<double>(m_value))
    {
        return "double";
    }
    else if (std::holds_alternative<std::string>(m_value))
    {
        return "string";
    }
    else
    {
        throw std::logic_error("get_type_name函数中:获取json内部类型时发生了未知错误");
    }
}

std::optional<std::string> data::to_str() const noexcept
{
    if (std::holds_alternative<std::monostate>(m_value))
    {
        return "null";
    }
    else if (std::holds_alternative<bool>(m_value))
    {
        auto temp = std::get<bool>(m_value);
        if (temp)
        {
            return "true";
        }
        else
        {
            return "false";
        }
    }
    else if (std::holds_alternative<int>(m_value))
    {
        return std::to_string(std::get<int>(m_value));
    }
    else if (std::holds_alternative<double>(m_value))
    {
        return std::to_string(std::get<double>(m_value));
    }
    else if (std::holds_alternative<std::string>(m_value))
    {
        return std::get<std::string>(m_value);
    }
    return std::nullopt;
}
std::optional<bool> data::to_bool() const noexcept
{
    if (std::holds_alternative<std::monostate>(m_value))
    {
        return false;
    }
    else if (std::holds_alternative<bool>(m_value))
    {
        return std::get<bool>(m_value);
    }
    else if (std::holds_alternative<int>(m_value))
    {
        return bool(std::get<int>(m_value));
    }
    else if (std::holds_alternative<double>(m_value))
    {
        return bool(std::get<double>(m_value));
    }
    else if (std::holds_alternative<std::string>(m_value))
    {
        return !(std::get<std::string>(m_value).empty());
    }
    return std::nullopt;
}
std::optional<long> data::to_int() const noexcept
{
    if (std::holds_alternative<std::monostate>(m_value))
    {
        return 0;
    }
    else if (std::holds_alternative<bool>(m_value))
    {
        return int(std::get<bool>(m_value));
    }
    else if (std::holds_alternative<int>(m_value))
    {
        return std::get<int>(m_value);
    }
    else if (std::holds_alternative<double>(m_value))
    {
        return int(std::get<double>(m_value));
    }
    return std::nullopt;
}
std::optional<double> data::to_double() const noexcept
{
    if (std::holds_alternative<std::monostate>(m_value))
    {
        return 0.;
    }
    else if (std::holds_alternative<bool>(m_value))
    {
        return double(std::get<bool>(m_value));
    }
    else if (std::holds_alternative<int>(m_value))
    {
        return double(std::get<int>(m_value));
    }
    else if (std::holds_alternative<double>(m_value))
    {
        return std::get<double>(m_value);
    }
    return std::nullopt;
}