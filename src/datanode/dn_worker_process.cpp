#include "dn_worker_process.h"
#include "dfs_conf.h"
#include "dfs_channel.h"
#include "dfs_notice.h"
#include "dfs_conn.h"
#include "dfs_conn_listen.h"
#include "dn_module.h"
#include "dn_thread.h"
#include "dn_time.h"
#include "dn_conf.h"
#include "dn_process.h"
#include "dn_ns_service.h"
#include "dn_data_storage.h"

#define PATH_LEN  256

#define WORKER_TITLE "datanode: worker process"

extern uint32_t process_type;
extern uint32_t blk_scanner_running;

static int total_threads = 0;
static pthread_mutex_t init_lock;
static pthread_cond_t init_cond;
static int cur_runing_threads = 0;
static int cur_exited_threads = 0;

dfs_thread_t *woker_threads;
dfs_thread_t *last_task;
int           woker_num = 0;

dfs_thread_t *ns_service_threads;
int           ns_service_num = 0;

extern dfs_thread_t *main_thread;

extern faio_manager_t *faio_mgr;

static inline int hash_task_key(char* str, int len);
static void  thread_registration_init();
static void  threads_total_add(int n);
static int   thread_setup(dfs_thread_t *thread, int type);
static void *thread_worker_cycle(void *arg);
static void  thread_worker_exit(dfs_thread_t *thread);
static void  wait_for_thread_exit();
static void wait_for_thread_registration();
static int process_worker_exit(cycle_t *cycle);
static int create_worker_thread(cycle_t *cycle);
static void stop_worker_thread();
static int channel_add_event(int fd, int event,
    event_handler_pt handler, void *data);
static void channel_handler(event_t *ev);
static int create_ns_service_thread(cycle_t *cycle);
static int get_ns_srv_names(uchar_t *path, uchar_t names[][64]);
static void *thread_ns_service_cycle(void * args);
static void stop_ns_service_thread();
static void dio_event_handler(event_t * ev);
static int create_data_blk_scanner(cycle_t *cycle);

static int thread_setup(dfs_thread_t *thread, int type)
{
    conf_server_t *sconf = NULL;
	
    sconf = (conf_server_t *)dfs_cycle->sconf;
    thread->event_base.nevents = sconf->connection_n;
	thread->type = type;
    
    if (thread_event_init(thread) != DFS_OK) 
	{
        return DFS_ERROR;
    }

	thread->event_base.time_update = time_update;
    // 初始化线程连接池
	if (conn_pool_init(&thread->conn_pool, sconf->connection_n) != DFS_OK) 
	{
		return DFS_ERROR;
	}
    
    return DFS_OK;
}

static void thread_worker_exit(dfs_thread_t *thread)
{
    thread->state = THREAD_ST_EXIT;
    dfs_module_wokerthread_release(thread);
}

static int process_worker_exit(cycle_t *cycle)
{
    dfs_module_woker_release(cycle);
	
    exit(PROCESS_KILL_EXIT);
}

static void thread_registration_init()
{
    pthread_mutex_init(&init_lock, NULL);
    pthread_cond_init(&init_cond, NULL);
}

static void wait_for_thread_registration()
{
    pthread_mutex_lock(&init_lock);
	
    while (cur_runing_threads < total_threads) 
	{
        pthread_cond_wait(&init_cond, &init_lock);
    }
	
    pthread_mutex_unlock(&init_lock);
}

static void wait_for_thread_exit()
{
    pthread_mutex_lock(&init_lock);
	
    while (cur_exited_threads < total_threads) 
	{
        pthread_cond_wait(&init_cond, &init_lock);
    }
	
    pthread_mutex_unlock(&init_lock);
}

void register_thread_initialized(void)
{
    sched_yield();
	
    pthread_mutex_lock(&init_lock);
    cur_runing_threads++;
    pthread_cond_signal(&init_cond);
    pthread_mutex_unlock(&init_lock);
}

void register_thread_exit(void)
{
    pthread_mutex_lock(&init_lock);
    cur_exited_threads++;
    pthread_cond_signal(&init_cond);
    pthread_mutex_unlock(&init_lock);
}


// process_start_workers 中进来的入口函数
// 初始化 worker
void worker_processer(cycle_t *cycle, void *data)
{
    int            ret = 0;
    string_t       title;
    sigset_t       set;
    struct rlimit  rl;
    process_t     *process = NULL;

    ret = getrlimit(RLIMIT_NOFILE, &rl);
    if (ret == DFS_ERROR) 
	{
        exit(PROCESS_FATAL_EXIT);
    }

    process_type = PROCESS_WORKER;
    main_thread->event_base.nevents = 512;
    // 初始化 epoll 和 timer
    if (thread_event_init(main_thread) != DFS_OK) 
	{
		dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno, 
			"thread_event_init() failed");
		
        exit(PROCESS_FATAL_EXIT);
    }

    // start worker in dn_data_storage.c //dn_data_storage_worker_init
    // faio thread process queue task
    // init cache management
    // init report queue
    if (dfs_module_woker_init(cycle) != DFS_OK) 
	{
		dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno, 
			"dfs_module_woker_init() failed");
		
        exit(PROCESS_FATAL_EXIT);
    }

    sigemptyset(&set);//信号集置空
    if (sigprocmask(SIG_SETMASK, &set, NULL) == -1) // 就是不阻塞信号？
	{
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno, 
			"sigprocmask() failed");
    }

    // close this process's write fd
    process_close_other_channel();

    title.data = (uchar_t *)WORKER_TITLE;
    title.len = sizeof(WORKER_TITLE) - 1;
    process_set_title(&title);

    // 初始化线程锁
    thread_registration_init();

    // name node server ?
    // 获取 namespaceid 和 监控 report 上报
    // 发送心跳
    //
	if (create_ns_service_thread(cycle) != DFS_OK)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno,
            "create ns service thread failed");

        exit(PROCESS_FATAL_EXIT);
    }
    // 扫描 blk ，更新 hashtable 和 全局 reporter
	if (create_data_blk_scanner(cycle) != DFS_OK) 
	{
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno, 
            "create_data_blk_scanner failed");
		
        exit(PROCESS_FATAL_EXIT);
	}
    //创建worker线程
    //处理posted events？
    if (create_worker_thread(cycle) != DFS_OK)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno,
            "create worker thread failed");

        exit(PROCESS_FATAL_EXIT);
    }
//    sleep(1000);
    process = get_process(process_get_curslot());

    // channel 用于父子进程通信
    /* 根据全局变量ngx_channel开启一个通道,只用于处理读事件(ngx_channel_handler) */
    /*channel[0] 是用来发送信息的，channel[1]是用来接收信息的。那么对自己而言，它需要向其他进程发送信息，
     * 需要保留其它进程的channel[0], 关闭channel[1]; 对自己而言，则需要关闭channel[0]。
     * 最后把ngx_channel放到epoll中，从第一部分中的介绍我们可以知道，这个ngx_channel实际就是自己的 channel[1]。
     * 这样有信息进来的时候就可以通知到了。*/
    // 子进程读取通道消息
    if (channel_add_event(process->channel[1],
        EVENT_READ_EVENT, channel_handler, NULL) != DFS_OK) 
    {
        exit(PROCESS_FATAL_EXIT);
    }

    for ( ;; ) 
	{
        if (process_quit_check()) 
		{
            stop_worker_thread();
			stop_ns_service_thread();
			blk_scanner_running = DFS_FALSE;
			
            break;
        }
        // 先注释掉主线程的 thread 事件
        sleep(10000);

        thread_event_process(main_thread);
    }
	
    wait_for_thread_exit();
    process_worker_exit(cycle);
}

// 开启worker 线程

int create_worker_thread(cycle_t *cycle)
{
    conf_server_t *sconf = NULL;
    int            i = 0;
    
    sconf = (conf_server_t *)cycle->sconf;
    woker_num = sconf->worker_n;

    woker_threads = (dfs_thread_t *)pool_calloc(cycle->pool, 
		woker_num * sizeof(dfs_thread_t));
    if (!woker_threads) 
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "pool_calloc err");
		
        return DFS_ERROR;
    }

    for (i = 0; i < woker_num; i++) 
	{
        // 初始化epoll connection pool
        if (thread_setup(&woker_threads[i], THREAD_WORKER) == DFS_ERROR) 
		{
            dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, 
				"thread_setup err");
			
            return DFS_ERROR;
        }
    }
        
    for (i = 0; i < woker_num; i++) 
	{
        woker_threads[i].run_func = thread_worker_cycle; //
        woker_threads[i].running = DFS_TRUE;
        woker_threads[i].state = THREAD_ST_UNSTART;
		
        if (thread_create(&woker_threads[i]) != DFS_OK) 
		{
            dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, 
				"thread_create err");
			
            return DFS_ERROR;
        }
		
        threads_total_add(1);
    }

    wait_for_thread_registration();
    
    for (i = 0; i < woker_num; i++) 
	{
        if (woker_threads[i].state != THREAD_ST_OK) 
		{
           dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0,
                   "create_worker thread[%d] err", i);
		   
           return DFS_ERROR;
        }
    }
    
    return DFS_OK;
}

// worker 线程的 处理函数
// dn_data_storage_thread_init
static void * thread_worker_cycle(void *arg)
{
    dfs_thread_t *me = (dfs_thread_t *)arg;

    thread_bind_key(me);

	time_init();

	// dn_data_storage_thread_init
    // worker thread
    // init faio \ fio
    // init notifier eventfd
    // 初始化 io events 队列 posted events, posted bad events
    if (dfs_module_workethread_init(me) != DFS_OK) 
	{
        goto exit;
    }

    me->state = THREAD_ST_OK;

    register_thread_initialized();
   
    if (faio_mgr) 
	{
        // 添加 读事件
        // 监听的 fd me->faio_notify.nfd
        // eventfd 进程间通信
        // handle 处理 fio回调
        if (channel_add_event(me->faio_notify.nfd, EVENT_READ_EVENT, 
            dio_event_handler, (void *)me) == DFS_ERROR)
        {
            goto exit;
        }
    }

    while (me->running) 
	{
	    /*
	    if (process_doing & PROCESS_DOING_QUIT
            || process_doing & PROCESS_DOING_TERMINATE) 
        {
            if (thread_exit_check(me)) 
			{
                break;
            }
        }
        */
		
        thread_event_process(me);
    }

exit:
    thread_clean(me);
	register_thread_exit();
    thread_worker_exit(me);

    return NULL;
}

// epoll event handler in worker cycle
// notifier handler
// eventfd
static void dio_event_handler(event_t * ev)
{
    dfs_thread_t *thread = (dfs_thread_t *)((conn_t *)(ev->data))->conn_data;

    // 读取 eventfd
    // 设置 noticed FAIO_FALSE ??
    cfs_recv_event(&thread->faio_notify);
	cfs_ioevents_process_posted(&thread->io_events, &thread->fio_mgr);
}

// 根据 eventfd 初始化 connection
// epoll event 添加 读写事件
static int channel_add_event(int fd, int event, 
	event_handler_pt handler, void *data)
{
    event_t      *ev = NULL;
    event_t      *rev = NULL;
    event_t      *wev = NULL;
    conn_t       *c = NULL;
    event_base_t *base = NULL;

    // 根据 eventfd 初始化 connection
    c = conn_get_from_mem(fd);

    if (!c) 
	{
        return DFS_ERROR;
    }
    // worker thread  event base
    base = thread_get_event_base();

    c->pool = NULL;
    c->conn_data = data; // data 是 thread 本身

    rev = c->read;
    wev = c->write;

    ev = (event == EVENT_READ_EVENT) ? rev : wev;
    ev->handler = handler;

    // epoll add event
    // ev->data(conn)->fd
    if (epoll_add_event(base, ev, event, 0) == DFS_ERROR)
	{
        return DFS_ERROR;
    }

    return DFS_OK;
}

//ngx_channel_handler
static void channel_handler(event_t *ev)
{
    int            n = 0;
    conn_t        *c = NULL;
    channel_t      ch;
    event_base_t  *ev_base = NULL;
    process_t     *process = NULL;
    
    if (ev->timedout) 
	{
        ev->timedout = 0;
		
        return;
    }
    
    c = (conn_t *)ev->data;

    ev_base = thread_get_event_base();

    dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0, "channel handler");

    for ( ;; ) 
	{
        n = channel_read(c->fd, &ch, sizeof(channel_t), dfs_cycle->error_log);
		
        dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0, "channel: %i", n);
		
        if (n == DFS_ERROR) 
		{
            if (ev_base->event_flags & EVENT_USE_EPOLL_EVENT) 
			{
                event_del_conn(ev_base, c, 0);
            }

            conn_close(c);
            conn_free_mem(c);
            
            return;
        }
		
        if (ev_base->event_flags & EVENT_USE_EVENTPORT_EVENT) 
		{
            if (epoll_add_event(ev_base, ev, EVENT_READ_EVENT, 0) == DFS_ERROR)
			{
                return;
            }
        }
		
        if (n == DFS_AGAIN) 
		{
            return;
        }
		
        dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
            "channel command: %d", ch.command);
		
        switch (ch.command) 
		{
        case CHANNEL_CMD_QUIT:
            //process_doing |= PROCESS_DOING_QUIT;
            process_set_doing(PROCESS_DOING_QUIT);
            break;

        case CHANNEL_CMD_TERMINATE:
            process_set_doing(PROCESS_DOING_TERMINATE);
            break;

        case CHANNEL_CMD_OPEN:
            dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                "get channel s:%i pid:%P fd:%d",
                ch.slot, ch.pid, ch.fd);
			/* 收到其他进程的pid 和fd 信息 ，进程通信
			 * 就是在对应的位置上复制pid和fd,下次向往哪个进程发信息的时候，直接发到 ngx_process[目标进程].channel[0]*/
            process = get_process(ch.slot);
            process->pid = ch.pid;
            process->channel[0] = ch.fd;
            break;

        case CHANNEL_CMD_CLOSE:
            process = get_process(ch.slot);
            
            dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                "close channel s:%i pid:%P our:%P fd:%d",
                ch.slot, ch.pid, process->pid,
                process->channel[0]);
			
            if (close(process->channel[0]) == DFS_ERROR) {
                dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno,
                    "close() channel failed");
            }
			
            process->channel[0] = DFS_INVALID_FILE;
            process->pid = DFS_INVALID_PID;
            break;
			
        case CHANNEL_CMD_BACKUP:
            //findex_db_backup();
            break;
        }
    }
}

static void stop_worker_thread()
{
    for (int i = 0; i < woker_num; i++) 
	{
        woker_threads[i].running = DFS_FALSE;
    }
}

static void threads_total_add(int n)
{
    total_threads += n;
}

static inline int hash_task_key(char* str, int len)
{
    (void) len;
	
    return str[0];
}

// namenode 线程
static int create_ns_service_thread(cycle_t *cycle)
{
    conf_server_t *sconf = (conf_server_t *)cycle->sconf;

	int i = 0;
	uchar_t names[16][64]; // 127.0.0.1:8001
	
	ns_service_num = get_ns_srv_names(sconf->ns_srv.data, names);

	ns_service_threads = (dfs_thread_t *)pool_calloc(cycle->pool, 
		ns_service_num * sizeof(dfs_thread_t));
    if (!ns_service_threads) 
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "pool_calloc err");
		
        return DFS_ERROR;
    }

    for (i = 0; i < ns_service_num; i++)
    {
        int count = sscanf((const char *)names[i], "%[^':']:%d", 
			ns_service_threads[i].ns_info.ip, 
			&ns_service_threads[i].ns_info.port);
        if (count != 2)
        {
            return DFS_ERROR;
        }
        // namenode 的run func
		ns_service_threads[i].run_func = thread_ns_service_cycle;

        ns_service_threads[i].running = DFS_TRUE;
        ns_service_threads[i].state = THREAD_ST_UNSTART;
		// 线程创建之后运行 thread_ns_service_cycle，参数是thread 本身
        if (thread_create(&ns_service_threads[i]) != DFS_OK) 
		{
            dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, 
				"thread_create err");
			
            return DFS_ERROR;
        }
		
        threads_total_add(1);
	}

	wait_for_thread_registration();
    
    for (i = 0; i < ns_service_num; i++) 
	{
        if (ns_service_threads[i].state != THREAD_ST_OK) 
		{
           dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0,
                   "create ns service thread[%d] err", i);
		   
           return DFS_ERROR;
        }
    }
	
    return DFS_OK;
}

static int get_ns_srv_names(uchar_t *path, uchar_t names[][64])
{
    uchar_t *str = NULL;
    char    *saveptr = NULL;
    uchar_t *token = NULL;
    int      i = 0;

    for (str = path ; ; str = NULL, token = NULL, i++)
    {
        token = (uchar_t *)strtok_r((char *)str, ",", &saveptr);
        if (token == NULL)
        {
            break;
        }

        memset(names[i], 0x00, PATH_LEN);
		strcpy((char *)names[i], (const char *)token);
    }

    return i;
}

// name node server
// args is thread self
static void *thread_ns_service_cycle(void * args)
{
    dfs_thread_t *me = (dfs_thread_t *)args;

    thread_bind_key(me);

    me->state = THREAD_ST_OK;

    //
    register_thread_initialized();

    while (me->running) 
	{
        // 连接上 namenode 获取 namespaceid
        if (dn_register(me) != DFS_OK) 
		{
            sleep(1);

			continue;
		}
		// 检查 version namespace id ，创建子文件夹
		setup_ns_storage(me);
        // namenode 上报 receivedblock_report、 block_report
		offer_service(me);
    }

	register_thread_exit();
    me->state = THREAD_ST_EXIT;
	
    return NULL;
}

static void stop_ns_service_thread()
{
    for (int i = 0; i < ns_service_num; i++) 
	{
        ns_service_threads[i].running = DFS_FALSE;
    }
}

// 创建 scanner 线程
static int create_data_blk_scanner(cycle_t *cycle)
{
    pthread_t pid;

	if (pthread_create(&pid, NULL, &blk_scanner_start, NULL) != DFS_OK) 
    {
	    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno, 
			"create blk_scanner thread failed");

		return DFS_ERROR;
	}

    return DFS_OK;
}

