#ifndef DBS_GENERAL_INCLUDE
#define DBS_GENERAL_INCLUDE

#include <string>
#include <future>
#include <functional>
#include <utility>
#include <any>
#include <vector>

namespace dbs
{ 
    // 生成一个随机字符串
    std::string random_gen(int length);
    // 将文件读进字符串
    std::string read_file(const std::string & input_path);
    // 将gbk字符串转换为utf8
    std::string gbk_to_utf8(const std::string & input_str);
    // 切割字符串，并将最后一个分割好的字符串单独包装
    std::vector<std::string> split_string(const std::string & input, char delimiter);


    // 将有参数输入的对象类型擦除，包装成无参数的可调用对象，并返回类型推导的未来结果
    template<typename F, typename... Args>
    auto pack_func(F&& f, Args&&... args)->std::pair<std::function<void()>, std::future<typename std::invoke_result<F, Args...>::type>>
    {
        // 连接函数和参数定义，特殊函数类型，避免左右值错误
        std::function<typename std::invoke_result<F, Args...>::type()> func = std::bind(std::forward<F>(f), std::forward<Args>(args)...); 
        auto task = std::make_shared< std::packaged_task<typename std::invoke_result<F, Args...>::type()> >(func);
        // 将任务封装为一个lambda表达式并放入任务队列
        // 该lambda表达式会调用std::packaged_task对象的operator()方法，从而执行任务
        std::function<void()> warpper_func = [task]() {(*task)(); };
        // 获取future也就是将来要返回的结果
        std::future<typename std::invoke_result<F,Args...>::type> res = task->get_future();
        return std::make_pair(std::move(warpper_func), res);
    }
}

#endif