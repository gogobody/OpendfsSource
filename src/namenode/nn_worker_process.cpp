#include "nn_worker_process.h"
#include "dfs_conf.h"
#include "dfs_channel.h"
#include "dfs_notice.h"
#include "dfs_conn.h"
#include "dfs_conn_listen.h"
#include "nn_module.h"
#include "nn_thread.h"
#include "nn_task_queue.h"
#include "nn_time.h"
#include "nn_task_handler.h"
#include "nn_net_response_handler.h"
#include "nn_conf.h"
#include "nn_process.h"
#include "nn_paxos.h"

#define WORKER_TITLE "namenode: worker process"

extern uint32_t process_type;

static int total_threads = 0;
static pthread_mutex_t init_lock;
static pthread_cond_t init_cond;
static int cur_runing_threads = 0;
static int cur_exited_threads = 0;

dfs_thread_t *task_threads;
dfs_thread_t *last_task; // 用于 dispatch task
int           task_num = 0;

extern dfs_thread_t *main_thread;
dfs_thread_t        *dn_thread;
dfs_thread_t        *cli_thread;
dfs_thread_t        *paxos_thread;

static inline int hash_task_key(char* str, int len);
static void  thread_registration_init();
static void  threads_total_add(int n);
static int   thread_setup(dfs_thread_t *thread, int type);
static void *thread_task_cycle(void *arg);
static void  thread_task_exit(dfs_thread_t *thread);
static void  wait_for_thread_exit();
static void wait_for_thread_registration();
static int process_worker_exit(cycle_t *cycle);
static int create_task_thread(cycle_t *cycle);
static void stop_task_thread(cycle_t *cycle);
static int channel_add_event(int fd, int event,
    event_handler_pt handler, void *data);
static void channel_handler(event_t *ev);
static int create_dn_thread(cycle_t *cycle);
static void stop_dn_thread();
static void *thread_dn_cycle(void * args);
static int create_cli_thread(cycle_t *cycle);
static void stop_cli_thread();
static void *thread_cli_cycle(void * args);
static int create_paxos_thread(cycle_t *cycle);
static void *thread_paxos_cycle(void *arg);
static void stop_paxos_thread();

// init task queue
// set event_base and event_timer
// init conn_pool
static int thread_setup(dfs_thread_t *thread, int type)
{
    conf_server_t *sconf = nullptr;
	
    sconf = (conf_server_t *)dfs_cycle->sconf;
    task_queue_init(&thread->tq);
    thread->event_base.nevents = sconf->connection_n;


    if (thread_event_init(thread) != NGX_OK)
	{
        return NGX_ERROR;
    }
    
    thread->type = type;
    if (THREAD_TASK == thread->type) 
	{
        return NGX_OK;
    }

    thread->event_base.time_update = time_update;
        
	if (conn_pool_init(&thread->conn_pool, sconf->connection_n) != NGX_OK)
	{
		return NGX_ERROR;
	}
    
    return NGX_OK;
}

static void thread_task_exit(dfs_thread_t *thread)
{
    thread->state = THREAD_ST_EXIT;
    ngx_module_wokerthread_release(thread);
}

static int process_worker_exit(cycle_t *cycle)
{
    ngx_module_woker_release(cycle);
	
    exit(PROCESS_KILL_EXIT);
}

static void thread_registration_init()
{
    pthread_mutex_init(&init_lock, nullptr);
    pthread_cond_init(&init_cond, nullptr);
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

// dispatch task when recv it
// data is task_queue_node_t
// push task to last_task->tq
// notice_wake_up (&last_task->tq_notice
void dispatch_task(void *data)
{
    task_queue_node_t *node = nullptr;
    task_t            *t = nullptr;
    uint32_t           index = 0;

	node = (task_queue_node_t *)data;
    t = &node->tk;
		
    index = hash_task_key(t->key, 16); // index is key address
    last_task = &task_threads[index % task_num]; // 初始化 last_task = &task_threads[0];
    push_task(&last_task->tq, (task_queue_node_t *)data); //
    notice_wake_up(&last_task->tq_notice);
}


// child process
void ngx_worker_process_cycle(cycle_t *cycle, void *data)
{
    int            ret = 0;
    string_t       title;
    sigset_t       set;
    struct rlimit  rl;
    process_t     *process = nullptr;

    ret = getrlimit(RLIMIT_NOFILE, &rl);
    if (ret == NGX_ERROR)
	{
        exit(PROCESS_FATAL_EXIT);
    }

    process_type = PROCESS_WORKER;
    main_thread->event_base.nevents = 512;

    // epoll init： fd\ event_list\ timer
    if (thread_event_init(main_thread) != NGX_OK)
	{
		dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno, 
			"thread_event_init() failed");
		
        exit(PROCESS_FATAL_EXIT);
    }
    // worker init
    if (ngx_module_woker_init(cycle) != NGX_OK)
	{
		dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno, 
			"ngx_module_woker_init() failed");
		
        exit(PROCESS_FATAL_EXIT);
    }

    sigemptyset(&set);
    if (sigprocmask(SIG_SETMASK, &set, nullptr) == -1)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno, 
			"sigprocmask() failed");
    }

    process_close_other_channel();

    title.data = (uchar_t *)WORKER_TITLE;
    title.len = sizeof(WORKER_TITLE) - 1;
    process_set_title(&title);
    
    thread_registration_init();

	// load fsimage
	// set check point
	load_image();

	/*
	 * 每一个thread cycle都 有 notice_init ,当notice pipe被唤醒的时候执行对应的handler
	 */

	// THREAD_PAXOS
	// do_paxos_task
	if (create_paxos_thread(cycle) != NGX_OK)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno, 
            "create paxos thread failed");
		
        exit(PROCESS_FATAL_EXIT);
    }

	// THREAD_TASK
	// 创建 task_num/worker_n 个 task_thread
    // 处理 task 并把task 分发到不同的tq
    if (create_task_thread(cycle) != NGX_OK)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno, 
            "create task thread failed");
		
        exit(PROCESS_FATAL_EXIT);
    }

    /*
     * 入口函数 ：listen_rev_handler ！important
     * */

    // THREAD_DN
	if (create_dn_thread(cycle) != NGX_OK)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno, 
            "create dn thread failed");
		
        exit(PROCESS_FATAL_EXIT);
    }

	// THREAD_CLI
	// same like dn thread
    if (create_cli_thread(cycle) != NGX_OK)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno, 
            "create cli thread failed");
		
        exit(PROCESS_FATAL_EXIT);
    }

    // 当前的 process
    process = get_process(process_get_curslot());
    
    if (channel_add_event(process->channel[1],
        EVENT_READ_EVENT, channel_handler, nullptr) != NGX_OK)
    {
        exit(PROCESS_FATAL_EXIT);
    }

    for ( ;; ) 
	{


        if (process_quit_check()) 
		{
            stop_cli_thread();
			stop_dn_thread();
            stop_task_thread(cycle);
			stop_paxos_thread();
			
            break;
        }
		
        thread_event_process(main_thread);
    }
	
    wait_for_thread_exit();
    process_worker_exit(cycle);
}

// paxos thread
int create_paxos_thread(cycle_t *cycle)
{
    paxos_thread = (dfs_thread_t *)pool_calloc(cycle->pool, 
		sizeof(dfs_thread_t));
    if (!paxos_thread) 
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "pool_calloc err");
		
        return NGX_ERROR;
    }

    // init task queue
    // set event_base and event_timer
    // init conn_pool
    if (thread_setup(paxos_thread, THREAD_PAXOS) == NGX_ERROR)
    {
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "thread_setup err");
			
        return NGX_ERROR;
    }
    
    paxos_thread->run_func = thread_paxos_cycle;
    paxos_thread->running = NGX_TRUE;
    task_queue_init(&paxos_thread->tq);
    paxos_thread->state = THREAD_ST_UNSTART;
		
    if (thread_create(paxos_thread) != NGX_OK)
    {
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "thread_create err");
			
        return NGX_ERROR;
    }

	threads_total_add(1);
    wait_for_thread_registration();
	
    if (paxos_thread->state != THREAD_ST_OK) 
	{
        return NGX_ERROR;
    }

	if (nn_paxos_run() != NGX_OK)
    {
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "run paxos err");
			
        return NGX_ERROR;
    }
    
    return NGX_OK;
}

// paxos 线程
// ngx_module_workethread_init
static void * thread_paxos_cycle(void *arg) //paxos_thread
{
    dfs_thread_t *me = (dfs_thread_t *)arg;

    // set paxos_thread to thread private data
    thread_bind_key(me);

    // 这个初始化应该放在外面去, 线程函数的初始化
    // 目前都是nullptr 不用初始化
//    if (ngx_module_workethread_init(me) != NGX_OK)
//	{
//        goto exit;
//    }

    me->state = THREAD_ST_OK;

    register_thread_initialized();

    // 进程间通信
    // n -> tq_notice
    // data -> task queue
    // open channel and 分配 pfd
    // 根据 pipe fd 添加 event 事件
    // 被唤醒的时候执行 do_paxos_task_handler
    notice_init(&me->event_base, &me->tq_notice, do_paxos_task_handler, &me->tq);

    while (me->running) 
	{
//        FSEditlog * fsEditlog = nn_get_paxos_obj();
//        if(fsEditlog){
//            task_s tmptask;
//            memcpy(&tmptask.key,"1",1);
//
////            printf("master ip:port %s:%d \n",fsEditlog->GetMaster(tmptask.key).GetIP().c_str(),fsEditlog->GetMaster(tmptask.key).GetPort());
//
//        }
        thread_event_process(me); // nn thread
    }

exit:
    thread_clean(me);
	register_thread_exit();
    thread_task_exit(me);

    return nullptr;
}


// thread task cycle
// 初始化 task num/worker_n 个 task_thread
// 分发 task 到不同的处理线程
int create_task_thread(cycle_t *cycle)
{
    conf_server_t *sconf = nullptr;
    int            i = 0;
    
    sconf = (conf_server_t *)cycle->sconf;
    task_num = sconf->worker_n;

    task_threads = (dfs_thread_t *)pool_calloc(cycle->pool, 
		task_num * sizeof(dfs_thread_t));
    if (!task_threads) 
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "pool_calloc err");
		
        return NGX_ERROR;
    }

    for (i = 0; i < task_num; i++) 
	{
        if (thread_setup(&task_threads[i], THREAD_TASK) == NGX_ERROR)
		{
            dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, 
				"thread_setup err");
			
            return NGX_ERROR;
        }
    }
    
    last_task = &task_threads[0];
        
    for (i = 0; i < task_num; i++) 
	{
        task_threads[i].run_func = thread_task_cycle;
        task_threads[i].running = NGX_TRUE;
        task_queue_init(&task_threads[i].tq);
        task_threads[i].state = THREAD_ST_UNSTART;
		
        if (thread_create(&task_threads[i]) != NGX_OK)
		{
            dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, 
				"thread_create err");
			
            return NGX_ERROR;
        }
		
        threads_total_add(1);
    }

    wait_for_thread_registration();
    
    for (i = 0; i < task_num; i++) 
	{
        if (task_threads[i].state != THREAD_ST_OK) 
		{
           dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0,
                   "create_worker thread[%d] err", i);
		   
           return NGX_ERROR;
        }
    }
    
    return NGX_OK;
}

// do_task_handler
static void * thread_task_cycle(void *arg)
{
    dfs_thread_t *me = (dfs_thread_t *)arg;

    thread_bind_key(me);

//    // none
//    if (ngx_module_workethread_init(me) != NGX_OK)
//	{
//        goto exit;
//    }

    me->state = THREAD_ST_OK;

    register_thread_initialized();

    // 把一些task 直接push到paxoas 的tq
    // call from ... listening_rev_handler ... dispatch_task
    notice_init(&me->event_base, &me->tq_notice, do_task_handler, &me->tq);

    while (me->running) 
	{
        thread_event_process(me);
    }

exit:
    thread_clean(me);
	register_thread_exit();
    thread_task_exit(me);

    return nullptr;
}

// datannode thread
static int create_dn_thread(cycle_t *cycle)
{
    int i = 0; 
	
    dn_thread = (dfs_thread_t *)pool_calloc(cycle->pool, sizeof(dfs_thread_t));
    if (!dn_thread) 
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "pool_calloc err");
		
        return NGX_ERROR;
    }
    
    if (thread_setup(dn_thread, THREAD_DN) != NGX_OK)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "thread_setup err");
		
        return NGX_ERROR;
    }

    //
    task_queue_init(&dn_thread->tq);
	
    dn_thread->queue_size = ((conf_server_t*)cycle->sconf)->worker_n;
	// write que queue 数组
    dn_thread->bque = (task_queue_t *)malloc(sizeof(task_queue_t) * dn_thread->queue_size);
    if (!dn_thread->bque)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "queue malloc fail");
    }

    // 初始化 bque 数组的每一个数组
    for (i = 0; i < dn_thread->queue_size; i++ )
	{
        task_queue_init(&dn_thread->bque[i]);
    }
	
    dn_thread->run_func= thread_dn_cycle; //
    dn_thread->running = NGX_TRUE;
    dn_thread->state = THREAD_ST_UNSTART;
	
    if (thread_create(dn_thread) != NGX_OK)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, 
			"thread_create error");
		
        return NGX_ERROR;
    }
	
    threads_total_add(1);
    wait_for_thread_registration();
	
    if (dn_thread->state != THREAD_ST_OK) 
	{
        return NGX_ERROR;
    }
	
    return NGX_OK;
}

// datanode thread
static void * thread_dn_cycle(void * args)
{
    dfs_thread_t *me = (dfs_thread_t *)args;
    array_t      *listens = nullptr;
    
    listens = cycle_get_listen_for_dn();
    thread_bind_key(me); //THREAD_DN

    // 添加读事件
    // 唤醒时执行 net_response_handler => write_back_pack_queue
    notice_init(&me->event_base, &me->tq_notice, net_response_handler, me);

    // 对所有的datanode 监听的listens 添加listening 的 read event
    // rev->handler = ls->handler
    // listen_rev_handler
    if (conn_listening_add_event(&me->event_base, listens) != NGX_OK)
	{
        goto exit;
    }
	
    me->state = THREAD_ST_OK;
    register_thread_initialized();
    
    while (me->running)
	{
        thread_event_process(me);

		usleep(3000);
    }

exit:
    thread_clean(me);
    me->state = THREAD_ST_EXIT;
    register_thread_exit();
	
    return nullptr;
}

static int create_cli_thread(cycle_t *cycle)
{
    int i = 0; 
	
    cli_thread = (dfs_thread_t *)pool_calloc(cycle->pool, sizeof(dfs_thread_t));
    if (!cli_thread) 
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "pool_calloc err");
		
        return NGX_ERROR;
    }
    
    if (thread_setup(cli_thread, THREAD_CLI) != NGX_OK)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "thread_setup err");
		
        return NGX_ERROR;
    }
	
    task_queue_init(&cli_thread->tq);
	
    cli_thread->queue_size = ((conf_server_t*)cycle->sconf)->worker_n;
	
    cli_thread->bque = (task_queue_t *)malloc(sizeof(task_queue_t) * cli_thread->queue_size);
    if (!cli_thread->bque)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "queue malloc fail");
    }
	
    for (i = 0; i < cli_thread->queue_size; i++ )
	{
        task_queue_init(&cli_thread->bque[i]);
    }
	
    cli_thread->run_func= thread_cli_cycle;
    cli_thread->running = NGX_TRUE;
    cli_thread->state = THREAD_ST_UNSTART;
	
    if (thread_create(cli_thread) != NGX_OK)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, 
			"thread_create error");
		
        return NGX_ERROR;
    }
	
    threads_total_add(1);
    wait_for_thread_registration();
	
    if (cli_thread->state != THREAD_ST_OK) 
	{
        return NGX_ERROR;
    }
	
    return NGX_OK;
}

static void * thread_cli_cycle(void * args)
{
    dfs_thread_t *me = (dfs_thread_t *)args;
    array_t      *listens = nullptr;
    
    listens = cycle_get_listen_for_cli();
    thread_bind_key(me);
    
    notice_init(&me->event_base, &me->tq_notice, net_response_handler, me);

    if (conn_listening_add_event(&me->event_base, listens) != NGX_OK)
	{
        goto exit;
    }
	
    me->state = THREAD_ST_OK;
    register_thread_initialized();
    
    while (me->running)
	{
        thread_event_process(me);
    }

exit:
    thread_clean(me);
    me->state = THREAD_ST_EXIT;
    register_thread_exit();
	
    return nullptr;
}

static int channel_add_event(int fd, int event, 
	                                event_handler_pt handler, void *data)
{
    event_t      *ev = nullptr;
    event_t      *rev = nullptr;
    event_t      *wev = nullptr;
    conn_t       *c = nullptr;
    event_base_t *base = nullptr;

    c = conn_get_from_mem(fd);

    if (!c) 
	{
        return NGX_ERROR;
    }
    
    base = thread_get_event_base();

    c->pool = nullptr;
    c->conn_data = data;

    rev = c->read;
    wev = c->write;

    ev = (event == EVENT_READ_EVENT) ? rev : wev;
    ev->handler = handler;

    if (event_add(base, ev, event, 0) == NGX_ERROR)
	{
        return NGX_ERROR;
    }

    return NGX_OK;
}

// handle channel event
static void channel_handler(event_t *ev)
{
    int            n = 0;
    conn_t        *c = nullptr;
    channel_t      ch;
    event_base_t  *ev_base = nullptr;
    process_t     *process = nullptr;
    
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
		
        if (n == NGX_ERROR)
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
            if (event_add(ev_base, ev, EVENT_READ_EVENT, 0) == NGX_ERROR)
			{
                return;
            }
        }
		
        if (n == NGX_AGAIN)
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
			
            if (close(process->channel[0]) == NGX_ERROR) {
                dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno,
                    "close() channel failed");
            }
			
            process->channel[0] = NGX_INVALID_FILE;
            process->pid = NGX_INVALID_PID;
            break;
			
        case CHANNEL_CMD_BACKUP:
            //findex_db_backup();
            break;
        }
    }
}

static void stop_cli_thread()
{
    cli_thread->running = NGX_FALSE;
}

static void stop_dn_thread()
{
    dn_thread->running = NGX_FALSE;
}

static void stop_task_thread(cycle_t *cycle)
{
    conf_server_t *sconf = nullptr;
    int            i = 0;

    sconf = (conf_server_t *)cycle->sconf;
    task_num = sconf->worker_n;
	
    for (i = 0; i < task_num; i++) 
	{
        task_threads[i].running = NGX_FALSE;
    }
}

static void stop_paxos_thread()
{
    paxos_thread->running = NGX_FALSE;
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

