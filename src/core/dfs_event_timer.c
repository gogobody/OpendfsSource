#include "dfs_event_timer.h"
#include "dfs_error_log.h"

// 在Nginx中，timer是自己实现的，而且实现的方法完全不同，而是通过红黑树来维护所有的timer节点
// 在worker进程的每一次循环中都会调用ngx_process_events_and_timers函数，
// 在该函数中就会调用处理定时器的函数ngx_event_expire_timers
int event_timer_init(event_timer_t *timer, curtime_ptr handler, log_t *log)
{
    rbtree_init(&timer->timer_rbtree, &timer->timer_sentinel, 
		rbtree_insert_timer_value);
    timer->time_handler = handler;
    timer->log = log;
	
    return DFS_OK;
}

// 每次该函数都不断的从红黑树中取出时间值最小的，查看他们是否已经超时，然后执行他们的函数，直到取出的节点的时间没有超时为止。
void event_timers_expire(event_timer_t *timer)
{
    event_t       *ev = NULL;
    rbtree_node_t *node = NULL;
    rbtree_node_t *root = NULL;
    rbtree_node_t *sentinel = NULL;

    for ( ;; ) 
	{
        sentinel = timer->timer_rbtree.sentinel;
        root = timer->timer_rbtree.root;

        if (root == sentinel) 
		{
            return;
        }

        node = rbtree_min(root, sentinel);

        if (node->key <= timer->time_handler()) 
		{
            ev = (event_t *) ((char *) node - offsetof(event_t, timer));

            rbtree_delete(&timer->timer_rbtree, &ev->timer);

            ev->timer_set = 0;
            ev->timedout = 1;

            ev->handler(ev);

            continue;
        }

        break;
    }
}

//用于返回当前红黑树当中的超时时间，说白了就是返回红黑树中最左边的元素的超时时间
rb_msec_t event_find_timer(event_timer_t *ev_timer)
{
    rb_msec_int_t  timer = 0;
    rbtree_node_t *node = NULL;
    rbtree_node_t *root = NULL;
    rbtree_node_t *sentinel = NULL;

    if (ev_timer->timer_rbtree.root == &ev_timer->timer_sentinel)
	{
        return EVENT_TIMER_INFINITE;
    }

    root = ev_timer->timer_rbtree.root;
    sentinel = ev_timer->timer_rbtree.sentinel;
    node = rbtree_min(root, sentinel); 	//找到红黑树中key最小的节点

    timer = node->key - ev_timer->time_handler();

    return (timer > 0 ? timer : 0);
}

// 然后还有一个函数ngx_event_del_timer，它用于将某个事件从红黑树当中移除。
void event_timer_del(event_timer_t *ev_timer, event_t *ev)
{
    if (!ev->timer_set) 
	{
        return;
    }

    dfs_log_debug(ev_timer->log, DFS_LOG_DEBUG, 0, "delete timer: %p, event:%p",
        &ev->timer, ev);

    rbtree_delete(&ev_timer->timer_rbtree, &ev->timer);

    ev->timer_set = 0;
}

// timer说白了就是一个int的值，表示超时的事件，用于表示红黑树节点的key
// 该函数用于将事件加入到红黑树中，首先设置超时时间，也就是当前的时间加上传进来的超时时间。
// 然后再将timer域加入到红黑树中就可以了，这里timer域的定义说白了是一棵红黑树节点。

void event_timer_add(event_timer_t *ev_timer, event_t *ev, rb_msec_t timer)
{
    rb_msec_t     key;
    rb_msec_int_t diff;

    key = ev_timer->time_handler() + timer; //表示该event的超时时间，为当前时间的值加上超时变量
    if (ev->timer_set) 
	{
        /*
         * Use a previous timer value if difference between it and a new
         * value is less than EVENT_TIMER_LAZY_DELAY milliseconds: this allows
         * to minimize the rbtree operations for dfs connections.
         */
        diff = (rb_msec_int_t) (key - ev->timer.key);
        if (abs(diff) < EVENT_TIMER_LAZY_DELAY) 
		{
            return;
        }
		
        event_timer_del(ev_timer, ev);
    }

    ev->timer.key = key;

    rbtree_insert(&ev_timer->timer_rbtree, &ev->timer);

    dfs_log_debug(ev_timer->log, DFS_LOG_DEBUG, 0, "add timer: ev: %p, timer:%p",
        ev, &ev->timer);

    ev->timer_set = 1;
}

