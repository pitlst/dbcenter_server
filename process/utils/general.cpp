#include <random>
#include <sstream>
#include <iostream>
#include <fstream>

#include <windows.h>

#include "general.hpp"

using namespace dbs;

std::string dbs::random_gen(int length)
{
    char tmp;
    std::string buffer; // buffer: 保存返回值
    std::random_device rd;
    std::default_random_engine random(rd());
    for (int i = 0; i < length; i++)
    {
        tmp = random() % 36; // 随机一个小于 36 的整数，0-9、A-Z 共 36 种字符
        if (tmp < 10)
        { // 如果随机数小于 10，变换成一个阿拉伯数字的 ASCII
            tmp += '0';
        }
        else
        { // 否则，变换成一个大写字母的 ASCII
            tmp -= 10;
            tmp += 'A';
        }
        buffer += tmp;
    }
    return buffer;
}

std::string dbs::read_file(const std::string &input_path)
{
    std::ifstream file;
    file.open(input_path, std::ios::in | std::ios::out);
    std::stringstream temps;
    if (file.is_open())
    {
        std::string line;
        while (std::getline(file, line))
        {
            temps << line << "\n";
        }
    }
    else
    {
        // 如果文件无法打开，输出错误信息
        std::cout << "Failed to open the file." << std::endl;
    }
    // 关闭文件流
    file.close();
    return temps.str();
}

std::string dbs::gbk_to_utf8(const std::string &input_str)
{
    std::string GBK = "";
    // 1、GBK转unicode
    // 1.1 统计转换后的字节数
    auto data = input_str.c_str();
    int len = MultiByteToWideChar(CP_ACP, // 转换的格式
                                  0,      // 默认的转换方式
                                  data,   // 输入的字节
                                  -1,     // 输入的字符串大小 -1找\0结束  自己去算
                                  0,      // 输出（不输出，统计转换后的字节数）
                                  0       // 输出的空间大小
    );
    if (len <= 0)
    {
        return GBK;
    }
    std::wstring udata; // 用wstring存储的
    udata.resize(len);  // 分配大小
    // 开始写进去
    MultiByteToWideChar(CP_ACP, 0, data, -1, (wchar_t *)udata.data(), len);

    // 2 unicode 转 utf8
    len = WideCharToMultiByte(CP_UTF8, 0, (wchar_t *)udata.data(), -1, 0, 0,
                              0, // 失败替代默认字符
                              0  // 是否使用默认替代  0 false
    );
    if (len <= 0)
    {
        return GBK;
    }
    GBK.resize(len);
    WideCharToMultiByte(CP_UTF8, 0, (wchar_t *)udata.data(), -1, (char *)GBK.data(), len, 0, 0);

    return GBK;
}

std::vector<std::string> dbs::split_string(const std::string &str, const std::string &delimiter)
{
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);

    while (std::getline(tokenStream, token, delimiter[0]))
    {
        tokens.push_back(token);
    }

    return tokens;
}

std::string dbs::get_thread_id()
{
    std::stringstream ss;
    std::thread::id id = std::this_thread::get_id();
    ss << std::hash<std::thread::id>()(id);
    return ss.str();
}   