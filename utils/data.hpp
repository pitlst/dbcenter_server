#ifndef DBS_DATA_INCLUDE
#define DBS_DATA_INCLUDE

#include <string>
#include <map>
#include <unordered_map>
#include <vector>
#include <list>
#include <variant>
#include <chrono>

namespace dbs
{

    // 时间类型缩写
    using clock = std::chrono::high_resolution_clock;
    using time_point = std::chrono::time_point<clock>;

    class row;
    class column;
    class table;
    // 基于variant实现的常用单元数据运行时多态
    class value
    {
    public:
        value();
        value(bool input_value);
        value(int input_value);
        value(long input_value);
        value(double input_value);
        value(const char *input_value);
        value(std::string_view input_value);
        value(const time_point &input_value);
        value(const value &input_value);
        value(value &&input_value);
        ~value() = default;

        value &operator=(bool input_value);
        value &operator=(int input_value);
        value &operator=(long input_value);
        value &operator=(double input_value);
        value &operator=(const char *input_value);
        value &operator=(std::string_view input_value);
        value &operator=(const time_point &input_value);
        value &operator=(const value &input_value);
        value &operator=(value &&input_value);

        operator bool() const;
        operator long() const;
        operator float() const;
        operator double() const;
        operator std::string() const;

        bool is_null() const;
        bool is_bool() const;
        bool is_int() const;
        bool is_double() const;
        bool is_string() const;
        bool is_time() const;
        bool is_type(std::string_view input_type_name) const;

        std::string get_type_name() const;

        std::string to_str() const;
        bool to_bool() const;
        long to_int() const;
        double to_double() const;
        time_point to_time() const;

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

    // 数据库表行
    class row
    {
    public:
        row();
        row(const std::unordered_map<std::string, value> &input_value);
        row(const std::map<std::string, value> &input_value);
        row(const std::vector<value> &input_value);
        row(const std::list<value> &input_value);
        row(const row &input_value);
        row(row &&input_value);
        ~row() = default;

        value &operator[](const char *input_value);
        value &operator[](std::string_view input_value);

        row &operator=(const std::unordered_map<std::string, value> &input_value);
        row &operator=(const std::map<std::string, value> &input_value);
        row &operator=(const std::vector<value> &input_value);
        row &operator=(const std::list<value> &input_value);
        row &operator=(const row &input_value);
        row &operator=(row &&input_value);

        std::unordered_map<std::string, value>::iterator begin();
        std::unordered_map<std::string, value>::iterator end();
        std::unordered_map<std::string, dbs::value>::const_iterator cbegin() const;
        std::unordered_map<std::string, dbs::value>::const_iterator cend() const;

        column transpose() const;
        size_t size() const;
        std::vector<std::string> colnums() const;
        row &colnums(const std::vector<std::string> &input_value);
        row &append(std::string_view name, const value &input_value);
        std::unordered_map<std::string, value> data() const;
        row &data(const std::unordered_map<std::string, value> &input_value);
        row &data(const std::map<std::string, value> &input_value);
        row &data(const std::vector<value> &input_value);
        row &data(const std::list<value> &input_value);
        row &data(const row &input_value);
        row &data(row &&input_value);

    private:
        // 行数据与各列名称
        std::unordered_map<std::string, value> m_row;
    };

    // 数据库表列
    class column
    {
    public:
        column();
        column(const char *input_value);
        column(std::string_view input_value);
        column(const std::vector<value> &input_value);
        column(const std::list<value> &input_value);
        column(const column &input_value);
        column(column &&input_value);
        ~column() = default;

        value &operator[](size_t index);

        column &operator=(const std::vector<value> &input_value);
        column &operator=(const std::list<value> &input_value);
        column &operator=(const column &input_value);
        column &operator=(column &&input_value);

        std::vector<value>::iterator begin();
        std::vector<value>::iterator end();
        std::vector<value>::const_iterator cbegin() const;
        std::vector<value>::const_iterator cend() const;
        std::reverse_iterator<std::vector<value>::iterator> rbegin();
        std::reverse_iterator<std::vector<value>::iterator> rend();
        std::reverse_iterator<std::vector<value>::const_iterator> crbegin() const;
        std::reverse_iterator<std::vector<value>::const_iterator> crend() const;

        row transpose() const;
        size_t size() const;
        std::string name() const;
        column &name(const char *input_value);
        column &name(std::string_view input_value);
        column &append(const value &input_value);
        column slice(size_t first, size_t second) const;
        std::vector<value> data() const;
        column &data(const std::vector<value> &input_value);
        column &data(const std::list<value> &input_value);
        column &data(const column &input_value);
        column &data(column &&input_value);

    private:
        // 列名
        std::string m_column_name;
        // 列数据
        std::vector<value> m_column;
    };

    // 数据库表、视图
    class table
    {
    public:
        table();
        table(const char *name);
        table(std::string_view name);
        table(std::string_view name, const std::vector<column> &input_value);
        table(std::string_view name, const std::vector<row> &input_value);
        table(const std::vector<column> &input_value);
        table(const std::vector<row> &input_value);
        table(const std::map<std::string, std::vector<value>> &input_value);
        table(const std::unordered_map<std::string, std::vector<value>> &input_value);
        table(const table &input_value);
        table(table &&input_value);
        ~table() = default;

        row &operator[](size_t index);
        table &operator[](const std::pair<size_t, size_t> &index);
        column &operator[](const char *input_value);
        column &operator[](std::string_view input_value);
        table &operator[](const std::vector<std::string> &input_value);

        table &operator=(const std::vector<column> &input_value);
        table &operator=(const std::vector<row> &input_value);
        table &operator=(const std::map<std::string, std::vector<value>> &input_value);
        table &operator=(const std::unordered_map<std::string, std::vector<value>> &input_value);
        table &operator=(const table &input_value);
        table &operator=(table &&input_value);

        // 对表进行转置
        table transpose() const;
        // 获取表的形状
        std::pair<size_t, size_t> shape() const;
        // 获取表名
        std::string name() const;
        // 重命名表
        table &name(std::string_view input_value);
        // 获取所有列名
        std::vector<std::string> colnums() const;
        // 重命名列
        table &colnums(const std::vector<std::string> &input_value);
        // 重命名部分列
        table &colnums(const std::map<std::string, std::string> &input_value);
        // 添加列
        table &append(std::string_view name, const std::vector<value> &input_value);
        // 添加行
        table &append(const std::vector<value> &input_value);
        // 添加部分列的行，其他列默认为空
        table &append(const std::unordered_map<std::string, value> &input_value);
        // 切片
        table slice(size_t first, size_t second) const;
        table slice(const std::vector<std::string> &input_value) const;
        // 获取数据
        std::unordered_map<std::string, std::vector<value>> data() const;
        table &data(const std::vector<column> &input_value);
        table &data(const std::vector<row> &input_value);
        table &data(const std::map<std::string, std::vector<value>> &input_value);
        table &data(const std::unordered_map<std::string, std::vector<value>> &input_value);
        table &data(const table &input_value);
        table &data(table &&input_value);

    private:
        // 检查两个row是否有相同的列名
        static bool check_row_is_same(const row &ch1, const row &ch2);

    private:
        // 数据库表名
        std::string m_table_name;
        // 数据库表
        std::unordered_map<std::string, std::vector<value>> m_table;
        // 表行数
        size_t m_index;
    };
}

// 哈希支持，使value支持哈希
namespace std
{
    template <> // function-template-specialization
    class hash<dbs::value>
    {
    public:
        size_t operator()(const dbs::value &name) const
        {
            return hash<string>()(name.to_str());
        }
    };
};

#endif