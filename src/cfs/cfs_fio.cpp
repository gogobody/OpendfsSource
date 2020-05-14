#include "cfs.h"
#include "cfs_fio.h"
#include "dfs_queue.h"
#include "dfs_buffer.h"
#include "dfs_memory.h"
#include "dn_conf.h"

static uint32_t cfs_fio_manager_refill(uint32_t n, 
	fio_manager_t *fio_manager);
static file_io_t *cfs_fio_manager_get_task(fio_manager_t *fio_manager);
static void cfs_fio_manager_indeed_free(uint64_t n, 
	fio_manager_t *fio_manager);
static size_t my_align(size_t d, uint32_t a);

// init fio_manager
// 初始化 n 个fio ，并且添加到 fio manager 的free queue
int cfs_fio_manager_init(cycle_t *cycle, fio_manager_t *fio_manager)
{
    queue_t       *one = nullptr;
    uint32_t       n = 0;
    file_io_t     *fio = nullptr;
	conf_server_t *sconf = (conf_server_t *)cycle->sconf;

    memory_zero(fio_manager, sizeof(fio_manager_t));

    queue_init(&fio_manager->freeq);
    queue_init(&fio_manager->task_used);

    fio_manager->idle = MAX_TASK_IDLE;
    fio_manager->size = /*sconf->dio_task_buf_size*/1024 * 512;
	fio_manager->max = /*sconf->dio_task_max_num*/(5 * GB_SIZE) / 
		(fio_manager->size + sizeof(buffer_t) + sizeof(file_io_t));
    fio_manager->threads = /*sconf->dio_thread_num*/20;
    fio_manager->batch = fio_manager->idle >> 2;
    fio_manager->refill_level = fio_manager->idle >> 3;

    // 初始化 MAX_TASK_IDLE 个fio ，并且添加到 fio manager 的free queue
    n = cfs_fio_manager_refill(MAX_TASK_IDLE, fio_manager);
    if (n == MAX_TASK_IDLE) 
	{
        return NGX_OK;
    }

    // 失败后的处理: 归还存储空间
    while (n) 
	{
        if (queue_empty(&fio_manager->freeq)) 
		{
            break;
        }

        one = queue_head(&fio_manager->freeq);
    	queue_remove(one);

    	fio = queue_data(one, file_io_t, q);
    	memory_free(fio, sizeof(file_io_t) + sizeof(buffer_t) 
			+ fio_manager->size);

        n--;
    }

    return NGX_ERROR;
}

file_io_t *cfs_fio_manager_alloc(fio_manager_t *fio_manager)
{
    uint32_t   n = 0;
    file_io_t *fio = nullptr;

    if (queue_empty(&fio_manager->freeq)) 
	{
        if (fio_manager->nelts >= fio_manager->max) 
		{
            return nullptr;
        }

        n = cfs_fio_manager_refill(fio_manager->batch, fio_manager);
        if (!n) 
		{
            return nullptr;
        }
    }

    fio = cfs_fio_manager_get_task(fio_manager);
    fio->b->last = fio->b->pos = fio->b->start;
    fio->index = AIO_NOTFIN;
    fio->result = AIO_PENDING;
    fio->fd = -1;
    fio->able = AIO_ABLE;
    fio->type = TASK_STORE_BODY;
    fio->offset = 0;

    queue_insert_tail(&fio_manager->task_used, &fio->used);

    return fio;
}

// 初始化 n 个fio ，并且添加到 fio manager 的free queue
static uint32_t cfs_fio_manager_refill(uint32_t n, fio_manager_t *fio_manager)
{
    uint32_t    i = 0;
    uint64_t    batch = 0;
    file_io_t  *fio = nullptr;
    size_t      hdrlength = 0;
    size_t      extralength = 0;
    size_t      reallength = 0;

    batch = n;

    if (fio_manager->nelts + n >= fio_manager->max) 
	{
        batch = fio_manager->max - fio_manager->nelts;
    }

    // 初始化batch个fio
    while (i < batch) 
	{
        hdrlength = sizeof(file_io_t) + sizeof(buffer_t);
        extralength = fio_manager->size;
		
        fio = (file_io_t  *)memory_calloc(hdrlength);
        if (!fio) 
		{
            break;
        }

        fio->b = (buffer_t *)(fio + 1);
        reallength = my_align(extralength, 512); // 512 字节对齐
        // 预对齐内存的分配 512 的倍数
        // 给 buffer 分配内存
        posix_memalign((void **)&fio->b->start, 512, reallength);

        fio->b->temporary = NGX_FALSE;
        fio->b->pos = fio->b->start; /* 待处理缓冲区起始位置 */
        fio->b->last = fio->b->start;
        fio->b->end = fio->b->last + reallength; /* end of buffer 缓冲区结束地址 */
        fio->b->memory = NGX_TRUE;
        fio->b->in_file = NGX_FALSE;

        queue_insert_tail(&fio_manager->freeq, &fio->q);

        i++;
    }

    fio_manager->nelts += i;
    fio_manager->free += i;

    return i;
}

// get fio from freeqs
static file_io_t *cfs_fio_manager_get_task(fio_manager_t *fio_manager)
{
    uint32_t    n = 0;
    queue_t    *one = nullptr;
    file_io_t  *fio = nullptr;

    if (fio_manager->free < fio_manager->refill_level) 
	{
        n = cfs_fio_manager_refill(fio_manager->batch, fio_manager);
    }

    if (queue_empty(&fio_manager->freeq)) 
	{
        return nullptr;
    }

    one = queue_head(&fio_manager->freeq);

    queue_remove(one);

    fio = queue_data(one, file_io_t, q);

    fio_manager->free--;
    fio_manager->busy++;

    return fio;
}

int cfs_fio_manager_free(file_io_t *fio, fio_manager_t *fio_manager)
{
    uint64_t frees = 0;

    queue_remove(&fio->used);

    fio->index = AIO_NOTFIN;
	fio->result = AIO_PENDING;
	fio->fd = -1;
    fio->able = AIO_ABLE;
    fio->type = TASK_STORE_BODY;
    fio->b->last = fio->b->pos = fio->b->start;

    queue_insert_head(&fio_manager->freeq, &fio->q);

    fio_manager->free++;
	fio_manager->busy--;

    if (fio_manager->free > fio_manager->idle) 
	{
        // free 1/4 of (free - idle) for each time
        frees = (fio_manager->free - fio_manager->idle) >> 2;
        cfs_fio_manager_indeed_free(frees, fio_manager);
    }

    return NGX_OK;
}

static void cfs_fio_manager_indeed_free(uint64_t n, 
	fio_manager_t *fio_manager)
{
    uint64_t    i = 0;
    uint64_t    free = 0;
    queue_t    *one = nullptr;
    file_io_t  *fio = nullptr;

    free = n ? n : (fio_manager->free - fio_manager->idle);

    if (!free) 
	{
        return;
    }

    while (i < free) 
	{
        if (queue_empty(&fio_manager->freeq)) 
		{
        	break;
        }

        one = queue_head(&fio_manager->freeq);
        queue_remove(one);

        fio = queue_data(one, file_io_t, q);
        memory_free(fio->b->start, fio->b->end - fio->b->start);
        memory_free(fio, sizeof(file_io_t) + sizeof(buffer_t));

        i++;
    }

    fio_manager->free -= i;
    fio_manager->nelts -= i;

    return;
}

int cfs_fio_manager_destroy(fio_manager_t *fio_manager)
{
	queue_t	  *one = nullptr;
	file_io_t *fio = nullptr;

	while (!queue_empty(&fio_manager->freeq)) 
	{
    	one = queue_head(&fio_manager->freeq);
    	queue_remove(one);

    	fio = queue_data(one, file_io_t, q);
    	memory_free(fio, sizeof(file_io_t) + sizeof(buffer_t) 
			+ fio_manager->size);
    }

	return NGX_OK;
}

static size_t my_align(size_t d, uint32_t a)
{
    if (d % a) 
	{
        d += a - (d % a);
	}

	return d;
}

