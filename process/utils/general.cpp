#include <random>
#include <sstream>
#include <iostream>
#include <fstream>

#include "general.hpp"

using namespace dbs;

std::string random_gen(int length)
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

std::string read_file(std::filesystem::path input_path)
{
	std::ifstream file;
	file.open("cp.txt", std::ios::in | std::ios::out);
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