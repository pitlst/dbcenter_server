#ifndef DBS_GENERAL_INCLUDE
#define DBS_GENERAL_INCLUDE

#include <string>
#include <future>
#include <functional>
#include <utility>
#include <chrono>
#include <any>
#include <vector>

#include "json.hpp"

namespace dbs
{
    // 生成一个随机字符串
    std::string random_gen(int length);
    // 将文件读进字符串
    std::string read_file(const std::string &input_path);
    // 将gbk字符串转换为utf8
    std::string gbk_to_utf8(const std::string &input_str);
    // 切割字符串，并将最后一个分割好的字符串单独包装
    std::vector<std::string> split_string(const std::string &str, const std::string &delimiter);
    // 删除指定字串
    std::string remove_substring(const std::string &str, const std::string &toRemove);
    // 删除回车
    std::string remove_newline(const std::string &str);
}

#endif