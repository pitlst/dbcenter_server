#include "utils/table.hpp"

using namespace dbs;

data::data()
{
}
data::data(bool input_value): m_value(input_value)
{
}
data::data(int input_value): m_value(input_value)
{
}
data::data(long input_value): m_value(input_value)
{
}
data::data(double input_value): m_value(input_value)
{
}
data::data(const char *input_value): m_value(input_value)
{
}
data::data(const std::string & input_value): m_value(input_value)
{
}
data::data(const time_point &input_value): m_value(input_value)
{
}
data::data(const data &input_value): m_value(input_value.m_value)
{
}
data::data(data &&input_value): m_value(input_value.m_value)
{
}

data &data::operator=(bool input_value)
{
    m_value = input_value;
    return *this;
}
data &data::operator=(int input_value){
    m_value = input_value;
    return *this;
}
data &data::operator=(long input_value){
    m_value = input_value;
    return *this;
}
data &data::operator=(double input_value){
    m_value = input_value;
    return *this;
}
data &data::operator=(const char *input_value){
    m_value = input_value;
    return *this;
}
data &data::operator=(const std::string & input_value){
    m_value = input_value;
    return *this;
}
data &data::operator=(const time_point &input_value){
    m_value = input_value;
    return *this;
}
data &data::operator=(const data &input_value){
    m_value = input_value.m_value;
    return *this;
}
data &data::operator=(data &&input_value){
    m_value = input_value.m_value;
    return *this;
}

data::operator bool() const
{

}
data::operator long() const
{

}
data::operator float() const
{

}
data::operator double() const
{

}
data::operator std::string() const
{
    
}