#pragma once
#ifndef DBS_JSON_INCLUDE
#define DBS_JSON_INCLUDE

#include <variant>
#include <string>
#include <vector>
#include <initializer_list>
#include <map>
#include <optional>

namespace dbs
{
    class json
    {
    public:
        json();
        json(bool input_value);
        json(int input_value);
        json(double input_value);
        json(const json &input_value);
        json(json &&input_value);
        json(const char *input_value);
        json(const std::string & input_value);
        json(const std::vector<json> &input_value);
        json(const std::map<std::string, json> &input_value);
        ~json() = default;

        /*
        对于任何默认的或者为隐式的拷贝方式均为浅拷贝
        所有针对json中的值的修改均会同步到所有指向对应变量的json类
        对于深拷贝需要重新声明一个新的json变量或者使用下面定义的copy函数手动复制
        */
        json &operator=(bool input_value);
        json &operator=(int input_value);
        json &operator=(double input_value);
        json &operator=(const json &input_value);
        json &operator=(json &&input_value);
        json &operator=(const char *input_value);
        json &operator=(const std::string & input_value);
        json &operator=(const std::vector<json> &input_value);
        json &operator=(const std::map<std::string, json> &input_value);

        json &operator[](std::size_t index);
        json &operator[](const std::string &key);

        operator bool() const;
        operator int() const;
        operator float() const;
        operator double() const;
        operator std::string() const;

        static json array(const std::initializer_list<json> &input_value);
        static json object(const std::map<std::string, json> &input_value);

        std::optional<bool> to_bool() const;
        std::optional<int> to_int() const;
        std::optional<double> to_double() const;
        std::optional<std::string> to_str() const;

        bool is_type(std::string_view input_type_name) const;
        std::string_view get_type_name() const;

        bool has(size_t index) const;
        bool has(const std::string &key) const;
        bool empty() const;
        std::size_t size() const;
        json copy() const;
        void copy(const json &input_value);
        void append(const json &input_value);
        void append(const std::string & input_name, const json &input_value);
        void remove(std::size_t index);
        void remove(const std::string &key);

    private:
        using m_type = std::variant<
            std::monostate, 
            bool, 
            int, 
            double, 
            std::string,
            std::vector<json>,
            std::map<std::string, json>
            >;
        m_type m_value;
    };

    // json的格式化输出函数
    std::string getLevelStr(int level);
    std::string format_json(const std::string json);

    // json的工厂类
    class json_parse
    {
    public:
        json_parse() = default;
        ~json_parse() = default;
        json_parse(const std::string &input_str);

        void load(const std::string &input_str);
        json parse();

    private:
        char get_next_token();
        bool in_range(int x, int lower, int upper);

        json parse_null();
        bool parse_bool();
        int parse_number();
        std::string parse_string();
        json parse_array();
        json parse_object();

        std::string m_str = "";
        size_t m_idx = 0;
        bool end_label = false;
    };
}

#endif