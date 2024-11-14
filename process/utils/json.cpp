#include "utils/json.hpp"

#include <stdexcept>
#include <typeinfo>
#include <type_traits>
#include <iostream>

using namespace dbs;

json::json()
{
}
json::json(bool input_value) : m_value(input_value)
{
}
json::json(int input_value) : m_value(input_value)
{
}
json::json(double input_value) : m_value(input_value)
{
}
json::json(const char *input_value) : m_value(input_value)
{
}
json::json(const std::string &input_value) : m_value(input_value)
{
}
json::json(const std::vector<json> &input_value) : m_value(input_value)
{
}
json::json(const std::map<std::string, json> &input_value) : m_value(input_value)
{
}
json::json(const json &input_value) : m_value(input_value.m_value)
{
}
json::json(json &&input_value) : m_value(input_value.m_value)
{
}

json &json::operator=(bool input_value)
{
    m_value = input_value;
    return *this;
}
json &json::operator=(int input_value)
{
    m_value = input_value;
    return *this;
}
json &json::operator=(double input_value)
{
    m_value = input_value;
    return *this;
}
json &json::operator=(const json &input_value)
{
    m_value = input_value.m_value;
    return *this;
}
json &json::operator=(json &&input_value)
{
    m_value = input_value.m_value;
    return *this;
}
json &json::operator=(const char *input_value)
{
    m_value = std::string(input_value);
    return *this;
}
json &json::operator=(const std::string &input_value)
{
    m_value = input_value;
    return *this;
}
json &json::operator=(const std::vector<json> &input_value)
{
    m_value = input_value;
    return *this;
}
json &json::operator=(const std::map<std::string, json> &input_value)
{
    m_value = input_value;
    return *this;
}

json &json::operator[](size_t index)
{
    if (!std::holds_alternative<std::vector<json>>(m_value))
    {
        throw std::logic_error("当前json节点的类型不是数组，但是使用了数组才能使用的下标索引");
    }
    return std::get<std::vector<json>>(m_value)[index];
}
json &json::operator[](const char *input_value)
{   
    return (*this)[std::string(input_value)];
}
json &json::operator[](const std::string &key)
{
    if (!std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        throw std::logic_error("当前json节点的类型不是对象，但是使用了对象才能使用的键名索引");
    }
    return std::get<std::map<std::string, json>>(m_value)[key];
}

json::operator bool() const
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
    else if (std::holds_alternative<std::vector<json>>(m_value))
    {
        return !(std::get<std::vector<json>>(m_value).empty());
    }
    else if (std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        return !(std::get<std::map<std::string, json>>(m_value).empty());
    }
    else
    {
        throw std::logic_error("对json强制转换为bool时发生了未知错误");
    }
}
json::operator int() const
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
        throw std::logic_error("对json强制转换为int时发生了错误:string不能转换为int");
    }
    else if (std::holds_alternative<std::vector<json>>(m_value))
    {
        throw std::logic_error("对json强制转换为int时发生了错误:array不能转换为int");
    }
    else if (std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        throw std::logic_error("对json强制转换为int时发生了错误:object不能转换为int");
    }
    else
    {
        throw std::logic_error("对json强制转换为int时发生了未知错误");
    }
}
json::operator float() const
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
        throw std::logic_error("对json强制转换为double时发生了错误:string不能转换为double");
    }
    else if (std::holds_alternative<std::vector<json>>(m_value))
    {
        throw std::logic_error("对json强制转换为double时发生了错误:array不能转换为double");
    }
    else if (std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        throw std::logic_error("对json强制转换为double时发生了错误:object不能转换为double");
    }
    else
    {
        throw std::logic_error("对json强制转换为double时发生了未知错误");
    }
}
json::operator double() const
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
        throw std::logic_error("对json强制转换为double时发生了错误:string不能转换为double");
    }
    else if (std::holds_alternative<std::vector<json>>(m_value))
    {
        throw std::logic_error("对json强制转换为double时发生了错误:array不能转换为double");
    }
    else if (std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        throw std::logic_error("对json强制转换为double时发生了错误:object不能转换为double");
    }
    else
    {
        throw std::logic_error("对json强制转换为double时发生了未知错误");
    }
}
json::operator std::string() const
{
    std::string temp_value;
    if (std::holds_alternative<std::monostate>(m_value))
    {
        temp_value = "null";
    }
    else if (std::holds_alternative<bool>(m_value))
    {
        if (std::get<bool>(m_value))
        {
            temp_value = "true";
        }
        else
        {
            temp_value = "false";
        }
    }
    else if (std::holds_alternative<int>(m_value))
    {
        temp_value = std::to_string(std::get<int>(m_value));
    }
    else if (std::holds_alternative<double>(m_value))
    {
        temp_value = std::to_string(std::get<double>(m_value));
    }
    else if (std::holds_alternative<std::string>(m_value))
    {
        temp_value = "\"" + std::get<std::string>(m_value) + "\"";
    }
    else if (std::holds_alternative<std::vector<json>>(m_value))
    {
        temp_value = "[";
        bool first = false;
        for (const auto &ch : std::get<std::vector<json>>(m_value))
        {
            if (first)
            {
                temp_value += ",";
            }
            temp_value += std::string(ch);
            first = true;
        }
        temp_value += "]";
    }
    else if (std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        temp_value = "{";
        bool first = false;
        for (const auto &ch : std::get<std::map<std::string, json>>(m_value))
        {
            if (first)
            {
                temp_value += ",";
            }
            temp_value += ("\"" + ch.first + "\":" + std::string(ch.second));
            first = true;
        }
        temp_value += "}";
    }
    else
    {
        throw std::logic_error("对json强制转换为string时发生了未知错误");
    }
    return temp_value;
}

std::optional<bool> json::to_bool() const noexcept
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
    else if (std::holds_alternative<std::vector<json>>(m_value))
    {
        return !(std::get<std::vector<json>>(m_value).empty());
    }
    else if (std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        return !(std::get<std::map<std::string, json>>(m_value).empty());
    }
    return std::nullopt;
}
std::optional<int> json::to_int() const noexcept
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
std::optional<double> json::to_double() const noexcept
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
std::optional<std::string> json::to_str() const noexcept
{
    std::string temp_value;
    if (std::holds_alternative<std::monostate>(m_value))
    {
        temp_value = "null";
    }
    else if (std::holds_alternative<bool>(m_value))
    {
        if (std::get<bool>(m_value))
        {
            temp_value = "true";
        }
        else
        {
            temp_value = "false";
        }
    }
    else if (std::holds_alternative<int>(m_value))
    {
        temp_value = std::to_string(std::get<int>(m_value));
    }
    else if (std::holds_alternative<double>(m_value))
    {
        temp_value = std::to_string(std::get<double>(m_value));
    }
    else if (std::holds_alternative<std::string>(m_value))
    {
        temp_value = "\"" + std::get<std::string>(m_value) + "\"";
    }
    else if (std::holds_alternative<std::vector<json>>(m_value))
    {
        temp_value = "[";
        bool first = false;
        for (const auto &ch : std::get<std::vector<json>>(m_value))
        {
            if (first)
            {
                temp_value += ",";
            }
            auto value = ch.to_str();
            if (value.has_value())
            {
                temp_value += value.value();
            }
            else
            {
                temp_value += "null";
            }
            first = true;
        }
        temp_value += "]";
    }
    else if (std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        temp_value = "{";
        bool first = false;
        for (const auto &ch : std::get<std::map<std::string, json>>(m_value))
        {
            if (first)
            {
                temp_value += ",";
            }
            auto value = ch.second.to_str();
            if (value.has_value())
            {
                temp_value += ("\"" + ch.first + "\":" + ch.second.to_str().value());
            }
            else
            {
                temp_value += ("\"" + ch.first + "\":" + "null");
            }
            first = true;
        }
        temp_value += "}";
    }
    else
    {
        return temp_value;
    }
    return temp_value;
}

json json::array(const std::initializer_list<json> &input_value)
{
    return json(input_value);
}
json json::object(const std::map<std::string, json> &input_value)
{
    return json(input_value);
}

bool json::is_type(std::string_view input_type_name) const
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
    else if (std::holds_alternative<std::vector<json>>(m_value))
    {
        return input_type_name == "array";
    }
    else if (std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        return input_type_name == "object";
    }
    else
    {
        throw std::logic_error("is_type函数中:获取json内部类型时发生了未知错误");
    }
}

std::string_view json::get_type_name() const
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
    else if (std::holds_alternative<std::vector<json>>(m_value))
    {
        return "array";
    }
    else if (std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        return "object";
    }
    else
    {
        throw std::logic_error("get_type_name函数中:获取json内部类型时发生了未知错误");
    }
}

bool json::has(size_t index) const
{
    if (std::holds_alternative<std::vector<json>>(m_value))
    {
        return std::get<std::vector<json>>(m_value).size() >= index;
    }
    else
    {
        throw std::logic_error("使用has方法时发生错误，该json的类型不支持该方法");
    }
}

bool json::has(const std::string &key) const
{
    if (std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        return std::get<std::map<std::string, json>>(m_value).count(key) != 0;
    }
    else
    {
        throw std::logic_error("使用has方法时发生错误，该json的类型不支持该方法");
    }
}

bool json::empty() const
{
    if (std::holds_alternative<std::monostate>(m_value))
    {
        return false;
    }
    else if (std::holds_alternative<bool>(m_value) ||
             std::holds_alternative<int>(m_value) ||
             std::holds_alternative<double>(m_value))
    {
        return true;
    }
    else if (std::holds_alternative<std::string>(m_value))
    {
        return std::get<std::string>(m_value).empty();
    }
    else if (std::holds_alternative<std::vector<json>>(m_value))
    {
        return std::get<std::vector<json>>(m_value).empty();
    }
    else if (std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        return std::get<std::map<std::string, json>>(m_value).empty();
    }
    else
    {
        throw std::logic_error("empty函数中:获取json内部类型时发生了未知错误");
    }
}

size_t json::size() const
{
    if (std::holds_alternative<std::monostate>(m_value) ||
        std::holds_alternative<bool>(m_value) ||
        std::holds_alternative<int>(m_value) ||
        std::holds_alternative<double>(m_value))
    {
        return 0;
    }
    else if (std::holds_alternative<std::string>(m_value))
    {
        return std::get<std::string>(m_value).size();
    }
    else if (std::holds_alternative<std::vector<json>>(m_value))
    {
        return std::get<std::vector<json>>(m_value).size();
    }
    else if (std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        return std::get<std::map<std::string, json>>(m_value).size();
    }
    else
    {
        throw std::logic_error("size函数中:获取json内部类型时发生了未知错误");
    }
}

void json::append(const json &input_value)
{
    if (std::holds_alternative<std::monostate>(m_value) ||
        std::holds_alternative<bool>(m_value) ||
        std::holds_alternative<int>(m_value) ||
        std::holds_alternative<double>(m_value) ||
        std::holds_alternative<std::string>(m_value) ||
        std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        throw std::logic_error("使用append方法时发生错误，该json的类型不支持该方法");
    }
    else if (std::holds_alternative<std::vector<json>>(m_value))
    {
        std::get<std::vector<json>>(m_value).emplace_back(input_value);
    }
    else
    {
        throw std::logic_error("append函数中:获取json内部类型时发生了未知错误");
    }
}

void json::append(const std::string &input_name, const json &input_value)
{
    if (std::holds_alternative<std::monostate>(m_value) ||
        std::holds_alternative<bool>(m_value) ||
        std::holds_alternative<int>(m_value) ||
        std::holds_alternative<double>(m_value) ||
        std::holds_alternative<std::string>(m_value) ||
        std::holds_alternative<std::vector<json>>(m_value))
    {
        throw std::logic_error("使用append方法时发生错误，该json的类型不支持该方法");
    }
    else if (std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        std::get<std::map<std::string, json>>(m_value).emplace(input_name, input_value);
    }
    else
    {
        throw std::logic_error("append函数中:获取json内部类型时发生了未知错误");
    }
}

void json::remove(size_t index)
{
    if (std::holds_alternative<std::monostate>(m_value) ||
        std::holds_alternative<bool>(m_value) ||
        std::holds_alternative<int>(m_value) ||
        std::holds_alternative<double>(m_value) ||
        std::holds_alternative<std::string>(m_value) ||
        std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        throw std::logic_error("使用remove方法时发生错误，该json的类型不支持该方法");
    }
    else if (std::holds_alternative<std::vector<json>>(m_value))
    {
        auto &temp_value = std::get<std::vector<json>>(m_value);
        temp_value.erase(temp_value.begin() + index);
    }
    else
    {
        throw std::logic_error("remove函数中:获取json内部类型时发生了未知错误");
    }
}

void json::remove(const std::string &key)
{
    if (std::holds_alternative<std::monostate>(m_value) ||
        std::holds_alternative<bool>(m_value) ||
        std::holds_alternative<int>(m_value) ||
        std::holds_alternative<double>(m_value) ||
        std::holds_alternative<std::string>(m_value) ||
        std::holds_alternative<std::vector<json>>(m_value))
    {
        throw std::logic_error("使用remove方法时发生错误，该json的类型不支持该方法");
    }
    else if (std::holds_alternative<std::map<std::string, json>>(m_value))
    {
        auto &temp_value = std::get<std::map<std::string, json>>(m_value);
        auto it = temp_value.find(key);
        if (it != temp_value.end())
        {
            temp_value.erase(it);
        }
    }
    else
    {
        throw std::logic_error("remove函数中:获取json内部类型时发生了未知错误");
    }
}

std::string dbs::getLevelStr(int level)
{
    std::string levelStr = "";
    for (int i = 0; i < level; i++)
    {
        levelStr += "    "; // 这里可以\t换成你所需要缩进的空格数
    }
    return levelStr;
}

// json的格式化输出函数
std::string dbs::format_json(const std::string json)
{
    std::string result = "";
    int level = 0;
    for (std::string::size_type index = 0; index < json.size(); index++)
    {
        char c = json[index];

        if (level > 0 && '\n' == json[json.size() - 1])
        {
            result += getLevelStr(level);
        }

        switch (c)
        {
        case '{':
        case '[':
            result = result + c + "\n";
            level++;
            result += getLevelStr(level);
            break;
        case ',':
            result = result + c + "\n";
            result += getLevelStr(level);
            break;
        case '}':
        case ']':
            result += "\n";
            level--;
            result += getLevelStr(level);
            result += c;
            break;
        default:
            result += c;
            break;
        }
    }
    return result;
}

json_parse::json_parse(const std::string &input_str) : m_str(input_str)
{
}

char json_parse::get_next_token()
{
    if (!end_label)
    {
        while (m_str[m_idx] == ' ' || m_str[m_idx] == '\r' || m_str[m_idx] == '\n' || m_str[m_idx] == '\t')
        {
            m_idx++;
        }
        if (m_idx == m_str.size())
        {
            end_label = true;
            return m_str[m_str.size() - 1];
        }
        else
        {
            return m_str[m_idx++];
        }
    }
    else
    {
        return m_str[m_str.size() - 1];
    }
}

bool json_parse::in_range(int x, int lower, int upper)
{
    return (x >= lower && x <= upper);
}

void json_parse::load(const std::string &input_str)
{
    m_str = input_str;
}

json json_parse::parse()
{
    char ch = get_next_token();
    if (end_label)
    {
        return json();
    }
    else
    {
        switch (ch)
        {
        case 'n':
            m_idx--;
            return parse_null();
        case 't':
        case 'f':
            m_idx--;
            return json(parse_bool());
        case '-':
        case '+':
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            m_idx--;
            return json(parse_number());
        case '"':
            return json(parse_string());
        case '[':
            return parse_array();
        case '{':
            return parse_object();
        default:
            break;
        }
        throw std::logic_error("unexpected character in parse json");
    }
}

json json_parse::parse_null()
{
    if (m_str.compare(m_idx, 4, "null") != 0)
    {
        throw std::logic_error("parse null error");
    }
    m_idx += 4;
    return json();
}

bool json_parse::parse_bool()
{
    if (m_str.compare(m_idx, 4, "true") == 0)
    {
        m_idx += 4;
        return true;
    }
    else if (m_str.compare(m_idx, 5, "false") == 0)
    {
        m_idx += 5;
        return false;
    }
    else
    {
        throw std::logic_error("parse bool error");
    }
}

int json_parse::parse_number()
{
    size_t pos = m_idx;
    // 校验有没有负号
    if (m_str[m_idx] == '-')
        m_idx++;
    // 校验是否是数字
    if (m_str[m_idx] == '0')
    {
        m_idx++;
    }
    else if (in_range(m_str[m_idx], '1', '9'))
    {
        m_idx++;
        while (in_range(m_str[m_idx], '0', '9'))
        {
            m_idx++;
        }
    }
    else
    {
        throw std::logic_error("invalid character in number");
    }
    // 校验是不是整数
    if (m_str[m_idx] != '.')
    {
        // atoi在遇到非数字字符时自动停止转换
        return std::atoi((m_str.c_str() + pos));
    }
    else
    {
        // 如果是浮点数
        m_idx++;
        if (!in_range(m_str[m_idx], '0', '9'))
        {
            throw std::logic_error("at least one digit required in fractional part");
        }
        while (in_range(m_str[m_idx], '0', '9'))
        {
            m_idx++;
        }
        return int(std::atof(m_str.c_str() + pos));
    }
}

std::string json_parse::parse_string()
{
    size_t pos = m_idx;
    while (true)
    {
        if (m_idx == m_str.size())
        {
            throw std::logic_error("unexpected end of input in string");
        }

        char ch = m_str[m_idx++];
        if (ch == '"')
        {
            break;
        }

        // The usual case: non-escaped characters
        if (ch == '\\')
        {
            ch = m_str[m_idx++];
            switch (ch)
            {
            case 'b':
            case 't':
            case 'n':
            case 'f':
            case 'r':
            case '"':
            case '\\':
                break;
            case 'u':
                m_idx += 4;
                break;
            default:
                break;
            }
        }
    }
    return m_str.substr(pos, m_idx - pos - 1);
}

json json_parse::parse_array()
{
    json arr(std::vector<json>{});
    char ch = get_next_token();
    if (ch == ']')
    {
        return arr;
    }
    m_idx--;
    while (true)
    {
        arr.append(parse());
        ch = get_next_token();
        if (ch == ']')
        {
            break;
        }
        if (ch != ',')
        {
            throw std::logic_error("expected ',' in array");
        }
    }
    return arr;
}

json json_parse::parse_object()
{
    json obj(std::map<std::string, json>{});
    char ch = get_next_token();
    if (ch == '}')
    {
        return obj;
    }
    m_idx--;
    while (true)
    {
        ch = get_next_token();
        if (ch != '"')
        {
            throw std::logic_error("expected '\"' in object");
        }
        std::string key = parse_string();
        ch = get_next_token();
        if (ch != ':')
        {
            throw std::logic_error("expected ':' in object");
        }
        obj[key] = parse();
        ch = get_next_token();
        if (ch == '}')
        {
            break;
        }
        if (ch != ',')
        {
            throw std::logic_error("expected ',' in object");
        }
    }
    return obj;
}