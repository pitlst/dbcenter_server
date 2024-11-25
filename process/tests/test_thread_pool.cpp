#include <iostream>
#include <vector>

#include "thread_pool.hpp"

int main()
{
    // std::vector<std::future<int>> results;

    for (int i = 0; i < 8; i++)
    {
        // auto tp = [i]
        // {
        //     std::cout << "hello " << i << std::endl;
        //     std::this_thread::sleep_for(std::chrono::seconds(1));
        //     std::cout << "world " << i << std::endl;
        //     return i * i;
        // };
        auto tp = [i]()
        {
            std::cout << "hello " << i << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            std::cout << "world " << i << std::endl;
        };
        dbs::thread_pool::instance().submit(tp);
    }

    // for (auto &&result : results)
    //     std::cout << result.get() << ' ';
    // std::cout << std::endl;

    return 0;
}