#ifndef DBS_TABLE_INCLUDE
#define DBS_TABLE_INCLUDE

#include <string>
#include <vector>
#include <optional>
#include <variant>

#include "utils/general.hpp"

namespace dbs
{
    // 基于variant实现的常用数据多态
    struct data
    {
    public:
        data();
        data(bool input_value);
        data(int input_value);
        data(long input_value);
        data(double input_value);
        data(const char *input_value);
        data(const std::string &input_value);
        data(const data &input_value);
        data(data &&input_value);
        ~data() = default;

        data &operator=(bool input_value);
        data &operator=(int input_value);
        data &operator=(long input_value);
        data &operator=(double input_value);
        data &operator=(const char *input_value);
        data &operator=(const std::string &input_value);
        data &operator=(const data &input_value);
        data &operator=(data &&input_value);

        operator bool() const;
        operator long() const;
        operator float() const;
        operator double() const;
        operator std::string() const;

        bool is_type(const std::string &input_type_name) const;
        std::string get_type_name() const;

        std::optional<std::string> to_str() const noexcept;
        std::optional<bool> to_bool() const noexcept;
        std::optional<long> to_int() const noexcept;
        std::optional<double> to_double() const noexcept;

    private:
        using m_type = std::variant<
            std::monostate,
            bool,
            int,
            double,
            std::string>;
        m_type m_value;
    };
}

// 哈希支持，使value支持哈希，从而支持underorder_set一类的无序容器
namespace std
{
    template <> // function-template-specialization
    struct hash<dbs::data>
    {
    public:
        size_t operator()(const dbs::data &name) const
        {
            std::string temp;
            auto temp_str = name.to_str();
            if (temp_str.has_value())
            {
                temp = temp_str.value();
            }
            else
            {
                temp = dbs::random_gen(8);
            }
            return hash<std::string>()(temp);
        }
    };
};

#endif