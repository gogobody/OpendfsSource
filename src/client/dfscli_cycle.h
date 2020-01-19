#ifndef DFS_CLI_CYCLE_H
#define DFS_CLI_CYCLE_H

#include "dfs_types.h"
#include "dfs_string.h"
#include "dfs_array.h"
#include "dfs_memory_pool.h"


typedef struct cycle_s 
{
	/*
	 * 保存着所有模块存储配置项的结构体的指针，它首先是一个数组，每个数组
	 * 成员又是一个指针，这个指针指向另一个存储着指针的数组
	 */
    void      *sconf;
    pool_t    *pool; // 用于该 ngx_cycle_t 的内存池
    /*
     * 日志模块中提供了生成基本ngx_lot_t日志对象的功能，这里的log实际上是在还没有执行
     * ngx_init_cycle 方法前，也就是还没有解析配置前，如果有信息需要输出到日志，就会
     * 暂时使用log对象，它会输出到屏幕。在ngx_init_cycle方法执行后，将会根据nginx.conf
     * 配置文件中的配置项，构造出正确的日志文件，此时会对log重新赋值.
     */
    log_t     *error_log;
    string_t   conf_file;//配置文件
} cycle_t;

extern cycle_t *dfs_cycle;

cycle_t  *cycle_create(); //create circle struct
int       cycle_init(cycle_t *cycle);
int       cycle_free(cycle_t *cycle);

#endif

