#include "thread_pool.hpp"

using namespace dbs;

thread_pool &thread_pool::instance()
{
    static thread_pool m_self;
    return m_self;
}

thread_pool::thread_pool()
{
    for (std::size_t i = 0; i < thread_count; ++i)
    {
        // 创建线程
        auto thread = [this]
        {
            // 死循环，不通知不退出
            while (true)
            {
                std::function<void()> task;
                // 检查退出条件
                {
                    std::unique_lock<std::shared_mutex> lock(this->m_tasks_queue.mutex);
                    // 无任务时线程阻塞在这里，stop或许任务队列不为空时唤醒。
                    this->m_tasks_queue.cv.wait(lock, [this]{ return this->stop || this->froce_stop || !this->m_tasks_queue.queue.empty(); });
                    if (this->froce_stop)
                    {
                        return;
                    }
                    else if (this->stop && this->m_tasks_queue.queue.empty())
                    {
                        return;
                    }
                    // 获取任务
                    task = std::move(this->m_tasks_queue.queue.front());
                    this->m_tasks_queue.queue.pop();
                }
                // 执行任务
                task();
            }
        };
        // 添加进线程池
        worker_list.emplace_back(thread);
    }
}

thread_pool::~thread_pool()
{
    // 析构时终止线程池
    shutdown();
}

void thread_pool::shutdown(bool wait)
{
    if (wait)
    {
        stop = true;
    }
    else
    {
        froce_stop = true;
    }
    m_tasks_queue.cv.notify_all();
    for (auto &ch : worker_list)
    {
        ch.join();
    }
}