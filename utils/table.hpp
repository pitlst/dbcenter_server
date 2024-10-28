#ifndef DBS_TABLE_INCLUDE
#define DBS_TABLE_INCLUDE

#include <string>
#include <map>
#include <unordered_map>
#include <vector>
#include <list>
#include <variant>
#include <chrono>

#include "utils/base.hpp"

namespace dbs
{

    // 时间类型缩写
    using clock = std::chrono::system_clock;
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
        data(const std::string &input_value);
        data(const time_point &input_value);
        data(const data &input_value);
        data(data &&input_value);
        ~data() = default;

        data &operator=(bool input_value);
        data &operator=(int input_value);
        data &operator=(long input_value);
        data &operator=(double input_value);
        data &operator=(const char *input_value);
        data &operator=(const std::string &input_value);
        data &operator=(const time_point &input_value);
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
        std::optional<time_point> to_time() const noexcept;

    private:
        using m_type = std::variant<
            std::monostate,
            bool,
            int,
            double,
            std::string,
            time_point>;
        m_type m_value;
    };

    // 数据列抽象，一维，返回常用数据的多态
    struct series
    {
    public:
        series(const series &input_value);
        series(series &&input_value);
        ~series() = default;

        size_t size() const;
        bool empty() const;

        // 数据行列索引，用于支持数字和字母的联合索引
        using index = std::variant<std::monostate, size_t, std::string>;

        void append(const index &input_name, const data &input_value);
        void append(const series &input_value);
        void remove(index index_label);
        void remove(const std::vector<index> &select_index);

        std::vector<index> colunms() const;
        void colunms(const std::vector<index> &input_name);
        void rename(const std::map<index, index> &input_name);

        data &operator[](std::size_t index);
        data &operator[](const std::string &key);

        std::vector<data>::iterator begin();
        std::vector<data>::iterator end();
        std::vector<data>::const_iterator cbegin() const;
        std::vector<data>::const_iterator cend() const;
        std::reverse_iterator<std::vector<data>::iterator> rbegin();
        std::reverse_iterator<std::vector<data>::iterator> rend();
        std::reverse_iterator<std::vector<data>::const_iterator> crbegin() const;
        std::reverse_iterator<std::vector<data>::const_iterator> crend() const;

    private:
        std::vector<data> m_value;
        std::vector<index> m_index;
    };

    // 数据表抽象，二维，返回数据列
    struct table
    {
    public:
        table(const table &input_value);
        table(table &&input_value);
        ~table() = default;

        std::pair<size_t, size_t> shape() const;
        bool empty() const;

        void append(const series &input_value);
        void append(const table &input_value);
        void remove(size_t index);
        void remove(size_t start_index, size_t end_index);
        void remove(const std::vector<size_t> &select_index);
        void remove(const std::string &key);
        void remove(const std::vector<std::string> &select_key);

        std::vector<std::string> colunms() const;
        void colunms(const std::vector<std::string> &input_name);
        void rename(const std::map<std::string, std::string> &input_name);

        series &operator[](std::size_t index);
        series &operator[](const std::string &key);

        std::vector<series>::iterator begin();
        std::vector<series>::iterator end();
        std::vector<series>::const_iterator cbegin() const;
        std::vector<series>::const_iterator cend() const;
        std::reverse_iterator<std::vector<series>::iterator> rbegin();
        std::reverse_iterator<std::vector<series>::iterator> rend();
        std::reverse_iterator<std::vector<series>::const_iterator> crbegin() const;
        std::reverse_iterator<std::vector<series>::const_iterator> crend() const;

    private:
        // 检查存储的行是否有不一样的index
        bool __check_index();
        // 数据行列索引，用于支持数字和字母的联合索引
        std::vector<series> m_value;
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