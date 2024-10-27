#ifndef DBS_DATA_INCLUDE
#define DBS_DATA_INCLUDE

#include <string>
#include <variant>
#include <optional> 
#include <chrono>

namespace dbs
{

    // 时间类型缩写
    using clock = std::chrono::high_resolution_clock;
    using time_point = std::chrono::time_point<clock>;

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
        data(std::string_view input_value);
        data(const time_point &input_value);
        data(const data &input_value);
        data(data &&input_value);
        ~data() = default;

        data &operator=(bool input_value);
        data &operator=(int input_value);
        data &operator=(long input_value);
        data &operator=(double input_value);
        data &operator=(const char *input_value);
        data &operator=(std::string_view input_value);
        data &operator=(const time_point &input_value);
        data &operator=(const data &input_value);
        data &operator=(data &&input_value);

        operator bool() const;
        operator long() const;
        operator float() const;
        operator double() const;
        operator std::string() const;

        std::string get_type_name() const;

        std::optional<std::string> to_str() const;
        std::optional<bool> to_bool() const;
        std::optional<long> to_int() const;
        std::optional<double> to_double() const;
        std::optional<time_point> to_time() const;
        // hash支持
        std::string hash() const;

    private:
        using m_type = std::variant<
            std::monostate,
            bool,
            long,
            double,
            std::string,
            time_point>;
        m_type m_value;
    };
}

// 哈希支持，使value支持哈希
namespace std
{
    template <> // function-template-specialization
    struct hash<dbs::data>
    {
    public:
        size_t operator()(const dbs::data &name) const
        {
            return hash<std::string>()(name.hash());
        }
    };
};

#endif