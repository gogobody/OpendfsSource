#include <stdio.h>

#include "dfs_lock.h"
#include "nn_time.h"

#define dfs_time_trylock(lock)  (*(lock) == 0 && CAS(lock, 0, 1))
#define dfs_time_unlock(lock)    *(lock) = 0
#define dfs_memory_barrier()    __asm__ volatile ("" ::: "memory")

#define CACHE_TIME_SLOT    64
static uint32_t            slot;
static uchar_t             dfs_cache_log_time[CACHE_TIME_SLOT]
                               [sizeof("1970/09/28 12:00:00")];

_xvolatile rb_msec_t       dfs_current_msec;
_xvolatile string_t        dfs_err_log_time;
_xvolatile struct timeval *dfs_time;
 struct timeval cur_tv;
_xvolatile uint64_t time_lock = 0;
//为避免每次都调用OS的gettimeofday，nginx采用时间缓存，每个worker进程都能自行维护；
int time_init(void)
{
    dfs_err_log_time.len = sizeof("1970/09/28 12:00:00.xxxx") - 1;
    
    dfs_time = &cur_tv;
    time_update();
	
    return DFS_OK;
}
//更新时间缓存,它会在每次运行信号处理函数的时候被调用，
void time_update(void)
{
    struct timeval  tv;
    struct tm       tm;
    time_t          sec;
    uint32_t        msec = 0;
    uchar_t        *p0 = nullptr;
    rb_msec_t       nmsec;

    if (!dfs_time_trylock(&time_lock))  //原子变量ngx_time_lock来对时间变量进行写加锁
	{
        return;
    }

    time_gettimeofday(&tv);//-宏定义，调用os的gettimeofday(tp, null)
    sec = tv.tv_sec;
    msec = tv.tv_usec / 1000;
	
	dfs_current_msec = (rb_msec_t) sec * 1000 + msec;
	nmsec = dfs_time->tv_sec * 1000 + dfs_time->tv_usec/1000; 
	if ( dfs_current_msec - nmsec < 10) //如果缓存的时间  -- 当前时间，直接返回，否则更新下一个slot数组元素
	{
		dfs_time_unlock(&time_lock);
		
		return;
    }
	slot++;
	slot ^=(CACHE_TIME_SLOT - 1);
    dfs_time->tv_sec = tv.tv_sec;
	dfs_time->tv_usec = tv.tv_usec;
	
    time_localtime(sec, &tm);
    p0 = &dfs_cache_log_time[slot][0]; //读当前时间缓存
	sprintf((char*)p0, "%4d/%02d/%02d %02d:%02d:%02d.%04d",
        tm.tm_year, tm.tm_mon,
        tm.tm_mday, tm.tm_hour,
        tm.tm_min, tm.tm_sec, msec);
    dfs_memory_barrier(); //它的作用实际上还是和防止读操作混乱有关，它告诉编译器不要将其后面的语句进行优化。不要打乱其运行顺序
    
    dfs_err_log_time.data = p0;

    dfs_time_unlock(&time_lock);
}

_xvolatile string_t *time_logstr()
{
    return &dfs_err_log_time;
}

rb_msec_t time_curtime(void)
{
    return dfs_current_msec;
}

