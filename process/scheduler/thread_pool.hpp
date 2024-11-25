#ifndef DBS_THREADPOOL_INCLUDE
#define DBS_THREADPOOL_INCLUDE

#include <functional>
#include <utility>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <list>
#include <cstdint>
#include <semaphore>
#include <shared_mutex>
#include <future>

namespace dbs
{
    // 线程池的任务对列
    struct tasks_queue
    {
        // 任务对列共享锁
        std::shared_mutex mutex;
        // 任务队列的条件变量
        std::condition_variable_any cv;
        // 任务队列，其中存储着待执行的任务
        std::queue<std::function<void()>> queue;      
    };

    // 单例模式线程池
    class thread_pool
    {
    public:
        // 获取单实例对象
        static thread_pool& instance();
        // 提交lambda函数执行
        template<class F, class... Args>
        auto submit(F&& f, Args&&... args)->std::future<typename std::invoke_result<F, Args...>::type>;
        // 终止线程池，通过变量确定是否等待任务执行完成
        // not_wait表示不等待剩余任务执行完成马上退出，但是已经运行的任务会运行完成后再退出
        void shutdown(bool wait = true);

    private:
        // 禁止外部构造与析构
        thread_pool();
        ~thread_pool();
    private:
        // 线程池中的线程数，默认按照逻辑处理器个数的两倍
        std::size_t thread_count = std::thread::hardware_concurrency()*2;
        // 线程池的任务对列
        tasks_queue m_tasks_queue;
        // 线程池所有的线程
        std::list<std::thread> worker_list;
        // 线程是否停止的标志位
        std::atomic<bool> stop = false;
        std::atomic<bool> froce_stop = false;
    };


    template<class F, class... Args>
    auto thread_pool::submit(F&& f, Args&&... args)->std::future<typename std::invoke_result<F, Args...>::type>
    {
        // 连接函数和参数定义，特殊函数类型，避免左右值错误
        std::function<typename std::invoke_result<F, Args...>::type()> func = std::bind(std::forward<F>(f), std::forward<Args>(args)...); 
        auto task = std::make_shared< std::packaged_task<typename std::invoke_result<F, Args...>::type()> >(func);
        // 将任务封装为一个lambda表达式并放入任务队列
        // 该lambda表达式会调用std::packaged_task对象的operator()方法，从而执行任务
        std::function<void()> warpper_func = [task]() {(*task)(); };
        // 获取future也就是将来要返回的结果
        std::future<typename std::invoke_result<F,Args...>::type> res = task->get_future();
        // 限定作用域控制加解锁的时间
        {
            std::unique_lock<std::shared_mutex> lock(m_tasks_queue.mutex);
            // 向对列添加任务
            m_tasks_queue.queue.emplace(std::move(warpper_func));
        }
        // 通知线程池中的一个线程
        m_tasks_queue.cv.notify_one();
        return res;
    }
}

#endif