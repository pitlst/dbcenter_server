#ifndef DBS_BIGINT_INCLUDE
#define DBS_BIGINT_INCLUDE

#include <iostream>
#include <string>
#include <utility>
#include <list>

namespace dbs
{
    class big_int
    {
    public:
        big_int();
        big_int(std::string_view input_value);
        big_int(long long input_value);
        ~big_int() = default;

        std::string to_str() const;
        size_t size() const;
        void negative();

        std::list<unsigned char>::iterator begin();
        std::list<unsigned char>::iterator end();
        std::reverse_iterator<std::list<unsigned char>::iterator> rbegin();
        std::reverse_iterator<std::list<unsigned char>::iterator> rend();
        std::list<unsigned char>::const_iterator cbegin() const;
        std::list<unsigned char>::const_iterator cend() const;
        std::reverse_iterator<std::list<unsigned char>::const_iterator> crbegin() const;
        std::reverse_iterator<std::list<unsigned char>::const_iterator> crend() const;

        explicit operator std::string() const;
        explicit operator bool() const;

        friend std::ostream &operator<<(std::ostream &output, const big_int &value);
        friend std::istream &operator>>(std::istream &input, big_int &value);

        friend big_int operator+(const big_int &first, const big_int &secend);
        friend big_int operator-(const big_int &first, const big_int &secend);
        friend big_int operator*(const big_int &first, const big_int &secend);
        friend std::pair<big_int, big_int> operator/(const big_int &first, const big_int &secend);
        friend big_int operator%(const big_int &first, const big_int &secend);

        big_int &operator=(const big_int &input_value);
        big_int &operator+=(const big_int &input_value);
        big_int &operator-=(const big_int &input_value);
        big_int &operator*=(const big_int &input_value);
        big_int &operator++();
        big_int &operator--();

        friend bool operator<(const big_int &first, const big_int &secend);
        friend bool operator>(const big_int &first, const big_int &secend);
        friend bool operator<=(const big_int &first, const big_int &secend);
        friend bool operator>=(const big_int &first, const big_int &secend);
        friend bool operator==(const big_int &first, const big_int &secend);

    private:
        void parse(std::string_view input_value);
        void parse(long long input_value);
        void clear();

        bool __negative;
        std::list<unsigned char> __nums;
    };
}

#endif