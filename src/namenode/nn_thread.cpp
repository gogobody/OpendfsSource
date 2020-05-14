#include "nn_thread.h"
#include "dfs_memory.h"
#include "dfs_sys.h"
#include "nn_time.h"

extern _xvolatile rb_msec_t dfs_current_msec;
static pthread_key_t dfs_thread_key;

void thread_env_init()
{
    pthread_key_create(&dfs_thread_key, nullptr); //第二个参数是一个清理函数，用来在线程释放该线程存储的时候被调用。该函数指针可以设成 nullptr，这样系统将调用默认的清理函数。
}

dfs_thread_t *thread_new(pool_t *pool)
{
    if (pool) 
	{
        return (dfs_thread_t *)pool_calloc(pool, sizeof(dfs_thread_t));
    }
	
    return (dfs_thread_t *)memory_calloc(sizeof(dfs_thread_t));
}

// 为一个键设置线程私有数据
// 同一个线程中不同函数间共享数据
void thread_bind_key(dfs_thread_t *thread)
{
    pthread_setspecific(dfs_thread_key, thread);
}

dfs_thread_t *get_local_thread()
{
    return (dfs_thread_t *)pthread_getspecific(dfs_thread_key);
}

event_base_t  *thread_get_event_base() 
{
	// pthread_getpecific和pthread_setspecific提供了在同一个线程中不同函数间共享数据即线程存储的一种方法。
    dfs_thread_t *thread = (dfs_thread_t *)pthread_getspecific(dfs_thread_key);
	
    return thread != nullptr ? &thread->event_base : nullptr;
}

event_timer_t *thread_get_event_timer()
{
    dfs_thread_t *thread = (dfs_thread_t *)pthread_getspecific(dfs_thread_key);
	
    return thread != nullptr ? &thread->event_timer : nullptr;
}

conn_pool_t * thread_get_conn_pool()
{
    dfs_thread_t *thread = (dfs_thread_t *)pthread_getspecific(dfs_thread_key);
	
    return &thread->conn_pool;
}

int thread_create(void *args)
{
    pthread_attr_t  attr;
    int             ret;
    dfs_thread_t   *thread = (dfs_thread_t *)args;

    pthread_attr_init(&attr);

	// run->func = thread_ns_service_cycle
	// 线程运行函数thread->run_func ，运行参数 thread
    if ((ret = pthread_create(&thread->thread_id, &attr, 
		thread->run_func, thread)) != 0) 
    {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, 0,
            "thread_create err: %s", strerror(ret));
		
        return NGX_ERROR;
    }

    return NGX_OK;
}

void thread_clean(dfs_thread_t *thread)
{
}

// init epoll
int thread_event_init(dfs_thread_t *thread)
{
    if (epoll_init(&thread->event_base, dfs_cycle->error_log) == NGX_ERROR)
	{
        return NGX_ERROR;
    }

    event_timer_init(&thread->event_timer, time_curtime, dfs_cycle->error_log);

    return NGX_OK;
}

// 不同的线程 处理 epoll 事件

void thread_event_process(dfs_thread_t *thread)
{
    uint32_t      flags = 0;
    rb_msec_t     timer = 0;
    rb_msec_t     delta = 0;
    event_base_t *ev_base;
    
    ev_base = &thread->event_base;
    
    if (THREAD_DN == thread->type || THREAD_CLI == thread->type) 
	{
        flags = EVENT_POST_EVENTS | EVENT_UPDATE_TIME;
    }
    
    timer = event_find_timer(&thread->event_timer);

    if ((timer > 10) || (timer == EVENT_TIMER_INFINITE)) 
	{
        timer = 10;
    }
    
    delta = dfs_current_msec;

    // 添加事件到  THREAD_DN or THREAD_CLI->ep_base 的event queue , other thread执行对应event的 handler 函数
    // 把  THREAD_DN or THREAD_CLI 的 events 先缓存起来，顺序处理
    (void) epoll_process_events(ev_base, timer, flags);

    //  THREAD_DN or THREAD_CLI thread process accept events
    if ((THREAD_DN == thread->type || THREAD_CLI == thread->type) 
		&& !queue_empty(&ev_base->posted_accept_events)) 
    {
        event_process_posted(&ev_base->posted_accept_events, ev_base->log);
    }

    if ((THREAD_DN == thread->type || THREAD_CLI == thread->type) 
		&& !queue_empty(&ev_base->posted_events)) 
    {
        event_process_posted(&ev_base->posted_events, ev_base->log);
    }

    delta = dfs_current_msec - delta;
    if (delta) 
	{
        event_timers_expire(&thread->event_timer);
    }
}

