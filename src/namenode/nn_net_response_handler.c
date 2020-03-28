#include "nn_net_response_handler.h"

#define task_data(q, type, link) \
    (type *) ((uchar_t *) q - offsetof(type, link))

static void write_back_pack_task(task_queue_node_t *node, int send);

//
int write_back(task_queue_node_t *node)
{
    task_t       *task = NULL;
    nn_wb_t      *wbt = NULL; // write back
    pthread_t     id = -1;
    dfs_thread_t *th = NULL;

    task = &node->tk;
    wbt  = (nn_wb_t *)task->opq;
	/*
	*struct nn_wb_s 
{
    nn_conn_t    *mc;
    dfs_thread_t *thread;
};
	*/
	
    queue_init(&node->qe);

	// 重新push到不同的队列里 // dn 和 cli push 到bque队列，nn 线程push到 tq队列里
    if (THREAD_DN == wbt->thread->type || THREAD_CLI == wbt->thread->type)
	{
        th = get_local_thread();
        id = th->thread_id;
        id %= wbt->thread->queue_size; // 做个简单的 hash

        push_task(&wbt->thread->bque[id], node); // 写回到当前线程的bque
    }
	else // 如果是 namenode 线程
	{
        push_task(&wbt->thread->tq, node);
    }
    
    return notice_wake_up(&wbt->thread->tq_notice);
}

// n->call_back
void net_response_handler(void *data)
{
    queue_t       q;
    task_queue_t *tq = NULL;
    dfs_thread_t *th = NULL;
    int           i = 0;   
    
    th = (dfs_thread_t *)data;
	
    if (THREAD_DN == th->type || THREAD_CLI == th->type) 
	{
        for (i = 0; i < th->queue_size; i++) 
        {
            tq = &th->bque[i];
			
            queue_init(&q);
            pop_all(tq, &q);
            write_back_pack_queue(&q, DFS_TRUE);
        }
    } 
	else 
	{
        tq = &th->tq; //task queue
		
        queue_init(&q);
        pop_all(tq, &q);
        write_back_pack_queue(&q, DFS_TRUE);
    }
}

void write_back_notice_call(void *data)
{
    queue_t       q;
    task_queue_t *tq = NULL;

    tq = (task_queue_t *)data;

    queue_init(&q);
    pop_all(tq, &q);
    write_back_pack_queue(&q, DFS_TRUE);
}

// 
static void write_back_pack_task(task_queue_node_t *node, int send)
{
    task_t  *task = NULL;
    nn_wb_t *wbt = NULL;

    task = &node->tk;
    wbt  = (nn_wb_t *)task->opq;
	
    nn_conn_outtask(wbt->mc, task);
}

// write tasks back which in q, send is status code
void write_back_pack_queue(queue_t *q, int send)
{
    queue_t           *qn = NULL;
    task_queue_node_t *node = NULL;
    
    while (!queue_empty(q)) 
	{
        qn = queue_head(q);
        queue_remove(qn);
        node = queue_data(qn, task_queue_node_t, qe);
        
        write_back_pack_task(node, send);
    }
}

int write_back_task(task_t* t)
{
    task_queue_node_t *node = NULL;
    node = queue_data(t, task_queue_node_t, tk);
	
    return write_back(node);
}

int trans_task(task_t *task, dfs_thread_t *thread)
{
    task_queue_node_t *node = NULL;

    node  = task_data(task, task_queue_node_t, tk);
     
    push_task(&thread->tq, node);
    
    return notice_wake_up(&thread->tq_notice);
}

