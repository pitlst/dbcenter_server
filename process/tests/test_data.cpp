#include <iostream>
#include <string>

std::string removeSubstr(const std::string &str, const std::string &toRemove)
{
    std::string result;
    std::string temp_str = str;
    size_t pos = temp_str.find(toRemove);
    
    // 循环直到字符串末尾
    while (pos != std::string::npos)
    {
        // 将找到的子字符串之前的部分添加到结果中
        result.append(temp_str, 0, pos);
        // 移动到找到的子字符串之后的位置
        temp_str = temp_str.substr(pos + toRemove.length());
        // 重置pos，以便在新的子字符串中搜索
        pos = temp_str.find(toRemove);
    }
    // 添加剩余的部分
    result.append(temp_str);
    return result;
}

int main()
{
    std::string original = "Hello, world! Hello, C++.";
    std::string toRemove = "Hello, ";
    std::string modified = removeSubstr(original, toRemove);

    std::cout << "Original string: " << original << std::endl;
    std::cout << "Modified string: " << modified << std::endl;

    return 0;
}