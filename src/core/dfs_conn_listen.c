#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "dfs_conn_listen.h"
#include "dfs_time.h"
#include "dfs_memory.h"
#include "dfs_event.h"
#include "dfs_epoll.h"

#define DFS_INET_ADDRSTRLEN            (sizeof("255.255.255.255") - 1)

int conn_listening_open(array_t *listening, log_t *log)
{
    int          s = DFS_INVALID_FILE;
    int          reuseaddr = 1;
    uint32_t     i = 0;
    uint32_t     tries = 0;
    uint32_t     failed = 0;
    listening_t *ls = NULL;

    if (!listening || !log) 
	{
        return DFS_ERROR;
    }

    for (tries = 5; tries; tries--) //bind和listen最多重试5次
	{
        failed = 0;
        ls = (listening_t *)listening->elts; // element?
		
        for (i = 0; i < listening->nelts; i++) 
		{
            if (ls[i].ignore) 
			{
                continue;
            }

            if (ls[i].fd != DFS_INVALID_FILE) 
			{
                dfs_log_error(log, DFS_LOG_ALERT, 0,
                    "conn_listening_open: %V, fd:%d already opened",
                    &ls[i].addr_text, ls[i].fd);
				
                continue;
            }

            if (ls[i].inherited) 
			{
                continue;
            }

            s = socket(ls[i].family, ls[i].type, 0);
            if (s == DFS_INVALID_FILE) 
			{
                dfs_log_error(log, DFS_LOG_ERROR, errno,
                    "conn_listening_open: create socket on %V failed",
                    &ls[i].addr_text);
				
                return DFS_ERROR;
            }
            /*
                默认情况下,server重启,调用socket,bind,然后listen,会失败.因为该端口正在被使用.如果设定SO_REUSEADDR,那么server重启才会成功.因此,
                所有的TCP server都必须设定此选项,用以应对server重启的现象.
                */
            if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR,
                (const void *) &reuseaddr, sizeof(int)) == DFS_ERROR) 
            {
                dfs_log_error(log, DFS_LOG_ERROR, errno,
                    "conn_listening_open: SO_REUSEADDR %V failed",
                    &ls[i].addr_text);
				
                goto error;
            }

            if (ls[i].rcvbuf != -1) 
			{
                if (setsockopt(s, SOL_SOCKET, SO_RCVBUF,
                    (const void *) &ls[i].rcvbuf, sizeof(int)) == DFS_ERROR) 
                {
                    dfs_log_error(log, DFS_LOG_ALERT, 0,
                        "conn_listening_open: SO_RCVBUF fd:%d "
                        "rcvbuf:%d addr:%V failed, ignored",
                        s, ls[i].rcvbuf, &ls[i].addr_text);
                }

            }

            if (ls[i].sndbuf != -1) 
		    {
                if (setsockopt(s, SOL_SOCKET, SO_SNDBUF,
                    (const void *) &ls[i].sndbuf, sizeof(int)) == DFS_ERROR) 
                {
                    dfs_log_error(log, DFS_LOG_ALERT, 0,
                        "conn_listening_open: SO_SNDBUF fd:%d "
                        "rcvbuf:%d addr:%V failed, ignored",
                        s, ls[i].sndbuf, &ls[i].addr_text);
                }

            }

            // we can't set linger onoff = 1 on listening socket
            if (conn_nonblocking(s) == DFS_ERROR) 
			{
                dfs_log_error(log, DFS_LOG_EMERG, errno,
                    "conn_listening_open: noblocking fd:%d "
                    "addr:%V failed", &ls[i].addr_text);
				
                goto error;
            }

            dfs_log_debug(log, DFS_LOG_DEBUG, 0,
                "conn_listening_open: bind fd:%d on addr:%V",
                s, &ls[i].addr_text);
			
            if (bind(s, ls[i].sockaddr, ls[i].socklen) == DFS_ERROR) 
			{
                dfs_log_error(log, DFS_LOG_EMERG, errno,
                    "conn_listening_open: bind fd:%d on addr:%V failed",
                    s, &ls[i].addr_text);
				
                close(s);
				
                if (errno != DFS_EADDRINUSE) 
				{
                    return DFS_ERROR;
                }

                failed = 1;
				
                continue;
            }

            if (listen(s, ls[i].backlog) == DFS_ERROR) 
			{
                dfs_log_error(log, DFS_LOG_EMERG, errno,
                    "conn_listening_open: listen fd:%d on addr:%V, "
                    "backlog:%d failed", s, &ls[i].addr_text, ls[i].backlog);
				
                goto error;
            }

            ls[i].listen = 1;
            ls[i].open = 1;
            ls[i].fd = s;
            ls[i].log = log;
			
            dfs_log_debug(log, DFS_LOG_DEBUG, 0,
                "ls[%d] %V ,fd %d", i, &ls[i].addr_text, s);
        }

        if (!failed) 
		{
            break;
        }

        dfs_log_error(log, DFS_LOG_NOTICE, 0,
            "conn_listening_open: bind failed, try again after 500ms");
		
        time_msleep(500);
    }

    if (failed) 
	{
        dfs_log_error(log, DFS_LOG_EMERG, 0,
            "conn_listening_open: listening socket bind failed");
		
        return DFS_ERROR;
    }

    return DFS_OK;
	
error:
    close(s);
	
    return DFS_ERROR;
}

//ngx_listening_t创建空间，并通过addr赋值初始化
listening_t * conn_listening_add(array_t *listening, pool_t *pool, 
                                        log_t *log, in_addr_t addr, 
                                        in_port_t port, event_handler_pt handler, 
                                        int rbuff_len, int sbuff_len)
{
    uchar_t            *address = NULL;
    listening_t        *ls = NULL;
    struct sockaddr_in *sin = NULL;

    if (!listening || !pool || !log ||  (port <= 0)) 
	{
        return NULL;
    }
    
    sin = (struct sockaddr_in *)pool_alloc(pool, sizeof(struct sockaddr_in));
    if (!sin) 
	{
        dfs_log_error(log, DFS_LOG_ALERT, 0,
            "conn_listening_add: pooll alloc sockaddr failed");
		
        return NULL;
    }
	
    sin->sin_family = AF_INET;
    sin->sin_addr.s_addr = addr;
    sin->sin_port = htons(port);
    address = (uchar_t *)inet_ntoa(sin->sin_addr);
    
    ls = (listening_t *)array_push(listening);
    if (!ls) 
	{
        dfs_log_error(log, DFS_LOG_ALERT, 0,
            "conn_listening_add: push listening socket failed!");
		
        return NULL;
    }
	
    memory_zero(ls, sizeof(listening_t));
    ls->addr_text.data = (uchar_t *)pool_calloc(pool,
        INET_ADDRSTRLEN - 1 + sizeof(":65535") - 1);
	
    if (!ls->addr_text.data) 
	{
        dfs_log_error(log, DFS_LOG_ALERT, 0,
            "conn_listening_add: pool alloc ls->addr text failed");
		
        return NULL;
    }
	
    ls->addr_text.len = string_xxsprintf(ls->addr_text.data,
        "%s:%d", address, port) - ls->addr_text.data;
    ls->fd = DFS_INVALID_FILE;
    ls->family = AF_INET;
    ls->type = SOCK_STREAM;
    ls->sockaddr = (struct sockaddr *) sin;
    ls->socklen = sizeof(struct sockaddr_in);
    ls->backlog = CONN_DEFAULT_BACKLOG;
   	ls->rcvbuf = rbuff_len > CONN_DEFAULT_RCVBUF? rbuff_len: CONN_DEFAULT_RCVBUF;
    ls->sndbuf = sbuff_len > CONN_DEFAULT_SNDBUF? sbuff_len: CONN_DEFAULT_SNDBUF;
    ls->conn_psize = CONN_DEFAULT_POOL_SIZE;
    ls->log = log;
    ls->handler = handler;
    ls->open = 0;
    ls->linger = 1;

    return ls;
}

int conn_listening_close(array_t *listening)
{
    size_t       i = 0;
    listening_t *ls = NULL;

    for (i = 0; i < listening->nelts; i++) 
	{
        if (ls[i].fd != DFS_INVALID_FILE) 
		{
            close(ls[i].fd);
        }
    }

    return DFS_OK;
}

// thread_event_process
// listen for cli
int conn_listening_add_event(event_base_t *base, array_t *listening)
{
    conn_t      *c = NULL;
    event_t     *rev = NULL;
    uint32_t     i = 0;
    listening_t *ls = NULL;
      
    ls = (listening_t *)listening->elts;// cli
	
    for (i = 0; i < listening->nelts; i++) 
	{
        c = ls[i].connection;
		
        if (!c) 
		{
            //为当前监听套接字的文件描述符分配一个connection，函数返回值c是当前监听套接字关联的connection
            c = conn_get_from_mem(ls->fd); // init conn
            if (!c) 
			{
                dfs_log_debug(ls[i].log, DFS_LOG_DEBUG, 0,
                    "add listening %V,fd %d",
                    &ls[i].addr_text, ls[i].fd);
				
                return DFS_ERROR;
            }
			
            dfs_log_debug(ls[i].log, DFS_LOG_DEBUG, 0,
                "add listening %V,fd %d ", &ls[i].addr_text, ls[i].fd);
            
            c->listening = &ls[i]; //当前连接的监听端口
            c->log = ls[i].log;
            ls[i].connection = c; //当前监听端口的connection
            rev = c->read;  //rev指向当前connection的读事件
            rev->accepted = DFS_TRUE; //表示当前的读事件是监听端口的accept事件，可以用于epoll区分是一般的读事件还是监听对口的accept事件
            rev->handler = ls->handler; // listen_rev_handler
        }
		else 
		{
            rev = c->read;
        }
		
        // setup listenting event
        if (epoll_add_event(base, rev, EVENT_READ_EVENT, 0) == DFS_ERROR)
		{
            return DFS_ERROR;
        }
    }

    return DFS_OK;
}

int conn_listening_del_event(event_base_t *base, array_t *listening)
{
    conn_t      *c = NULL;
    uint32_t     i = 0;
    listening_t *ls = NULL;

    ls = (listening_t *)listening->elts;
	
    for (i = 0; i < listening->nelts; i++) 
	{
        c = ls[i].connection;
		
        if (event_delete(base, c->read, EVENT_READ_EVENT, 0) == DFS_ERROR) 
		{
            return DFS_ERROR;
        }
    }

    return DFS_OK;
}

