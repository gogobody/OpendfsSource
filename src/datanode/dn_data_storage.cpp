#include "dn_data_storage.h"
#include "dfs_types.h"
#include "dfs_math.h"
#include "dfs_memory.h"
#include "dfs_commpool.h"
#include "dfs_mblks.h"
#include "dn_conf.h"
#include "dn_time.h"
#include "dn_process.h"
#include "dn_ns_service.h"

#define BLK_NUM_IN_DN 100000

uint32_t blk_scanner_running = NGX_TRUE;

static queue_t g_storage_dir_q;
static int     g_storage_dir_n = 0;
static char    g_last_version[56] = "";

static blk_cache_mgmt_t *g_dn_bcm = nullptr;

static int init_storage_dirs(cycle_t *cycle);
static int create_storage_dirs(cycle_t *cycle);
static int check_version(char *path);
static int check_namespace(char *path, int64_t namespaceID);
static int create_storage_subdirs(char *path);
static blk_cache_mgmt_t *blk_cache_mgmt_new_init();
static blk_cache_mgmt_t *blk_cache_mgmt_create(size_t index_num);
static int blk_mem_mgmt_create(blk_cache_mem_t *mem_mgmt, 
	size_t index_num);
static struct mem_mblks *blk_mblks_create(blk_cache_mem_t *mem_mgmt, 
	size_t count);
static void *allocator_malloc(void *priv, size_t mem_size);
static void allocator_free(void *priv, void *mem_addr);
static void blk_mem_mgmt_destroy(blk_cache_mem_t *mem_mgmt);
static void blk_cache_mgmt_release(blk_cache_mgmt_t *bcm);
static int uint64_cmp(const void *s1, const void *s2, size_t sz);
static size_t req_hash(const void *data, size_t data_size, 
	size_t hashtable_size);
static int get_disk_id(long block_id, char *path);
static int recv_blk_report(dn_request_t *r);
static int scan_current_dir(char *dir);
static void get_namespace_id(char *src, char *id);
static int scan_namespace_dir(char *dir, long namespace_id);
static int scan_subdir(char *dir, long namespace_id);
static int scan_subdir_subdir(char *dir, long namespace_id);
static void get_blk_id(char *src, char *id);

// 主进程
//数据节点master初始化，pool and cfs
int dn_data_storage_master_init(cycle_t *cycle)
{
	cycle->cfs = pool_alloc(cycle->pool, sizeof(cfs_t));
	if (!cycle->cfs) 
	{
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
			"pool_alloc err");

		return NGX_ERROR;
	}
	// 初始化 cfs 的各项函数
	if (cfs_setup(cycle->pool, (cfs_t *)cycle->cfs, cycle->error_log) 
		!= NGX_OK)
	{
        return NGX_ERROR;
    }
	
    return NGX_OK;
}

// 子进程
// from dn_worker_process 的worker_processer
// 入口函数
int dn_data_storage_worker_init(cycle_t *cycle)
{
    queue_init(&g_storage_dir_q);

	if (init_storage_dirs(cycle) != NGX_OK)
	{
	    return NGX_ERROR;
	}
    puts("##dn_data_storage_worker_init");
	if (create_storage_dirs(cycle) != NGX_OK)
	{
	    return NGX_ERROR;
	}
    // faio thread process queue task
	if (cfs_prepare_work(cycle) != NGX_OK)  // cfs_faio_ioinit(int thread_num)
	{
        return NGX_ERROR;
    }
    // blk cache management
	g_dn_bcm = blk_cache_mgmt_new_init();
    if (!g_dn_bcm) 
	{
        return NGX_ERROR;
    }

    // init blk report queue
	blk_report_queue_init();
	
    return NGX_OK;
}

int dn_data_storage_worker_release(cycle_t *cycle)
{
    blk_cache_mgmt_release(g_dn_bcm);
	g_dn_bcm = nullptr;

	blk_report_queue_release();
	
    return NGX_OK;
}


// worker thread
// init faio \ fio
// init notifier eventfd
// 初始化 io events 队列 posted events, posted bad events
int dn_data_storage_thread_init(dfs_thread_t *thread)
{
    // init fio_manager
    // 初始化 n 个fio ，并且添加到 fio manager 的free queue
    if (cfs_fio_manager_init(dfs_cycle, &thread->fio_mgr) != NGX_OK)
	{
        return NGX_ERROR;
	}
    // 初始化 notifier
    // 创建 event fd 初始化 0
	if (cfs_notifier_init(&thread->faio_notify) != NGX_OK)
	{
        return NGX_ERROR;
    }
    // 初始化 io events 队列 posted events, posted bad events
    return cfs_ioevent_init(&thread->io_events);
}

// init the  storage dirs from config file
static int init_storage_dirs(cycle_t *cycle)
{
    conf_server_t *sconf = (conf_server_t *)cycle->sconf;
    uchar_t       *str = sconf->data_dir.data;
    char          *saveptr = nullptr;
    uchar_t       *token = nullptr;
	char           dir[PATH_LEN] = "";

    for (int i = 0; ; str = nullptr, token = nullptr, i++)
    {
    	// data dir = "/data01/block,/data02/block,/data03/block"
        token = (uchar_t *)strtok_r((char *)str, ",", &saveptr); //分解字符串为一组字符串
        if (token == nullptr)
        {
            break;
        }

		storage_dir_t *sd = (storage_dir_t *)pool_alloc(cycle->pool, 
			sizeof(storage_dir_t));
		if (!sd) 
		{
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
				"pool_alloc err");

			return NGX_ERROR;
		}

        sd->id = i;
		string_xxsprintf((uchar_t *)dir, "%s/current", token);
		strcpy(sd->current, dir);
		queue_insert_tail(&g_storage_dir_q, &sd->me);
		g_storage_dir_n++;
    }

    return NGX_OK;
}

// create storage dirs
static int create_storage_dirs(cycle_t *cycle)
{
	queue_t *head = &g_storage_dir_q;
	queue_t *entry = queue_next(head);

    while (head != entry)
    {
        storage_dir_t *sd = queue_data(entry, storage_dir_t, me);
		
		entry = queue_next(entry);

	    if (access(sd->current, F_OK) != NGX_OK)
	    {
	        if (mkdir(sd->current, 
				S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH) != NGX_OK)
	        {
	            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
					"mkdir %s err", sd->current);
		
	            return NGX_ERROR;
	        }
	    }
    }
	
    return NGX_OK;
}

// namenode
int setup_ns_storage(dfs_thread_t *thread)
{
    char path[PATH_LEN] = "";
	
    queue_t *head = &g_storage_dir_q;
	queue_t *entry = queue_next(head);

    while (head != entry)
    {
        storage_dir_t *sd = queue_data(entry, storage_dir_t, me);
		
		entry = queue_next(entry);

        sprintf(path, "%s/VERSION", sd->current);
        if (check_version(path) != NGX_OK)
		{
            exit(PROCESS_KILL_EXIT);
		}

        sprintf(path, "%s/NS-%ld", sd->current, thread->ns_info.namespaceID);
		if (check_namespace(path, thread->ns_info.namespaceID) != NGX_OK)
		{
            exit(PROCESS_KILL_EXIT);
		}
	}
	
    return NGX_OK;
}

static int check_version(char *path)
{
    if (access(path, F_OK) != NGX_OK)
	{
	    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0664);
	    if (fd < 0) 
	    {
		    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
				"open %s err", path);
		
            return NGX_ERROR;
	    }

        if (0 == strcmp(g_last_version, "")) 
		{
		    sprintf(g_last_version, "DS-%s-%ld", 
				dfs_cycle->listening_ip, dfs_current_msec);
		}
		
		char version[128] = "";
	    sprintf(version, "storageID=%s\n", g_last_version);

		if (write(fd, version, strlen(version)) < 0) 
        {
		    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
				"write %s err", path);

			close(fd);

		    return NGX_ERROR;
	    }

	    close(fd);
	}
	else 
	{
		int fd = open(path, O_RDONLY);
	    if (fd < 0) 
	    {
		    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno, 
				"open %s err", path);
		
            return NGX_ERROR;
	    }

        char rBuf[128] = "";
	    if (read(fd, rBuf, sizeof(rBuf)) < 0) 
        {
		    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
				"read %s err", path);

		    close(fd);

		    return NGX_ERROR;
	    }

        char version[128] = "";
	    sscanf(rBuf, "%*[^=]=%s", version);

	    close(fd);

		if (0 == strcmp(g_last_version, "")) 
		{
            strcpy(g_last_version, version);
		}
		else if (0 != strcmp(g_last_version, version)) 
		{
		    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, 0, 
				"storageID in %s is incompatible with others.", path);
			
		    return NGX_ERROR;
		}
	}
	
    return NGX_OK;
}

//
static int check_namespace(char *path, int64_t namespaceID)
{
    char bbwDir[PATH_LEN] = "";
    char curDir[PATH_LEN] = "";
	char verDir[PATH_LEN] = "";
	
    if (access(path, F_OK) != NGX_OK)
	{
	    if (mkdir(path, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH) != NGX_OK)
	    {
	        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
				"mkdir %s err", path);
		
	        return NGX_ERROR;
	    }

		sprintf(bbwDir, "%s/blocksBeingWritten", path);
		if (mkdir(bbwDir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH) != NGX_OK)
	    {
	        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
				"mkdir %s err", bbwDir);
		
	        return NGX_ERROR;
	    }

        sprintf(curDir, "%s/current", path);
		if (mkdir(curDir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH) != NGX_OK)
	    {
	        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
				"mkdir %s err", curDir);
		
	        return NGX_ERROR;
	    }

		sprintf(verDir, "%s/VERSION", curDir);
        int fd = open(verDir, O_RDWR | O_CREAT | O_TRUNC, 0664);
	    if (fd < 0) 
	    {
		    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
				"open %s err", verDir);
		
            return NGX_ERROR;
	    }

		char version[128] = "";
	    sprintf(version, "namespaceID=%ld\n", namespaceID);

		if (write(fd, version, strlen(version)) < 0) 
        {
		    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
				"write %s err", verDir);

			close(fd);

		    return NGX_ERROR;
	    }

	    close(fd);
        // 创建了很多个subdir
		create_storage_subdirs(curDir);
	}
	else 
	{
	    sprintf(verDir, "%s/current/VERSION", path);
	
        int fd = open(verDir, O_RDONLY);
	    if (fd < 0) 
	    {
		    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno, 
				"open %s err", verDir);
		
            return NGX_ERROR;
	    }

        char rBuf[128] = "";
	    if (read(fd, rBuf, sizeof(rBuf)) < 0) 
        {
		    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
				"read %s err", verDir);

		    close(fd);

		    return NGX_ERROR;
	    }

        int64_t dn_ns_id = 0;
	    sscanf(rBuf, "%*[^=]=%ld", &dn_ns_id);

	    close(fd);

		if (dn_ns_id != namespaceID) 
		{
		    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, 0, 
				"Incompatible namespaceIDs in %s, "
				"namenode namespaceID = %ld, datanode namespaceID = %ld", 
				verDir, namespaceID, dn_ns_id);
			
		    return NGX_ERROR;
		}
	}
	
    return NGX_OK;
}

static int create_storage_subdirs(char *path)
{
    char subDir[PATH_LEN] = "";
	char ssubDir[PATH_LEN] = "";

	for (int i = 0; i < SUBDIR_LEN; i++) 
	{	
        sprintf(subDir, "%s/subdir%d", path, i);
		if (access(subDir, F_OK) != NGX_OK)
		{
            if (mkdir(subDir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH) != NGX_OK)
	        {
	            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
					"mkdir %s err", subDir);
		
	            return NGX_ERROR;
	        }

			for (int j = 0; j < SUBDIR_LEN; j++) 
			{
                sprintf(ssubDir, "%s/subdir%d", subDir, j);
				if (mkdir(ssubDir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH) 
					!= NGX_OK)
	            {
	                dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
						"mkdir %s err", ssubDir);
		
	                return NGX_ERROR;
	            }
			}
		}
	}
	
    return NGX_OK;
}

// mgmt is management
static blk_cache_mgmt_t *blk_cache_mgmt_new_init()
{
    size_t index_num = dfs_math_find_prime(BLK_NUM_IN_DN);  //blk num

    blk_cache_mgmt_t *bcm = blk_cache_mgmt_create(index_num);
    if (!bcm) 
	{
        return nullptr;
    }

    pthread_rwlock_init(&bcm->cache_rwlock, nullptr);

    return bcm;
}

// mgmt is management // create meme management
static blk_cache_mgmt_t *blk_cache_mgmt_create(size_t index_num)
{
    blk_cache_mgmt_t *bcm = (blk_cache_mgmt_t *)memory_alloc(sizeof(*bcm));
    if (!bcm) 
	{
        goto err_out;
    }
    
    if (blk_mem_mgmt_create(&bcm->mem_mgmt, index_num) != NGX_OK)
	{
        goto err_mem_mgmt;
    }

    bcm->blk_htable = dfs_hashtable_create(uint64_cmp, index_num, 
		req_hash, bcm->mem_mgmt.allocator);
    if (!bcm->blk_htable) 
	{
        goto err_htable;
    }

    return bcm;

err_htable:
    blk_mem_mgmt_destroy(&bcm->mem_mgmt);
	
err_mem_mgmt:
    memory_free(bcm, sizeof(*bcm));

err_out:
    return nullptr;
}


//create mem management 
static int blk_mem_mgmt_create(blk_cache_mem_t *mem_mgmt, 
	size_t index_num)
{
    assert(mem_mgmt);

    size_t mem_size = BLK_POOL_SIZE(index_num);

    mem_mgmt->mem = (uchar_t *)memory_calloc(mem_size);
    if (!mem_mgmt->mem) 
	{
        goto err_mem;
    }

    mem_mgmt->mem_size = mem_size;

	mpool_mgmt_param_t param;
    param.mem_addr = mem_mgmt->mem;
    param.mem_size = mem_size;
    // allocator
    // allocator.init => create pool
    mem_mgmt->allocator = dfs_mem_allocator_new_init(
		DFS_MEM_ALLOCATOR_TYPE_COMMPOOL, &param);
    if (!mem_mgmt->allocator) 
	{
        goto err_allocator;
    }

    // 创建index 个blk info t的块
    mem_mgmt->free_mblks = blk_mblks_create(mem_mgmt, index_num);
    if (!mem_mgmt->free_mblks) 
	{
        goto err_mblks;
    }

    return NGX_OK;

err_mblks:
    dfs_mem_allocator_delete(mem_mgmt->allocator);
	
err_allocator:
    memory_free(mem_mgmt->mem, mem_mgmt->mem_size);
	
err_mem:
    return NGX_ERROR;
}

static struct mem_mblks *blk_mblks_create(blk_cache_mem_t *mem_mgmt, 
	size_t count)
{
    assert(mem_mgmt);
	
    mem_mblks_param_t mblk_param;
    mblk_param.mem_alloc = allocator_malloc;
    mblk_param.mem_free = allocator_free;
    mblk_param.priv = mem_mgmt->allocator;

    // mem_mblks_new = > mem_mblks_new_fn
    // 初始化新的 mem mblks
    return mem_mblks_new_fn(sizeof(block_info_t), count, &mblk_param);
}

// 调用参数 allocator 的 alloc 分配mem size 大小的内存
static void *allocator_malloc(void *priv, size_t mem_size)
{
    if (!priv) 
	{
        return nullptr;
    }

	dfs_mem_allocator_t *allocator = (dfs_mem_allocator_t *)priv;
	
    return allocator->alloc(allocator, mem_size, nullptr);
}

static void allocator_free(void *priv, void *mem_addr)
{
    if (!priv || !mem_addr) 
	{
        return;
    }

    dfs_mem_allocator_t *allocator = (dfs_mem_allocator_t *)priv;
    allocator->free(allocator, mem_addr, nullptr);
}

static void blk_mem_mgmt_destroy(blk_cache_mem_t *mem_mgmt)
{
    mem_mblks_destroy(mem_mgmt->free_mblks); 
    dfs_mem_allocator_delete(mem_mgmt->allocator);
    memory_free(mem_mgmt->mem, mem_mgmt->mem_size);
}

static void blk_cache_mgmt_release(blk_cache_mgmt_t *bcm)
{
    assert(bcm);

	pthread_rwlock_destroy(&bcm->cache_rwlock);

    blk_mem_mgmt_destroy(&bcm->mem_mgmt);
    memory_free(bcm, sizeof(*bcm));
}

static int uint64_cmp(const void *s1, const void *s2, size_t sz)
{
    return *(uint64_t *)s1 == *(uint64_t *)s2 ? NGX_FALSE : NGX_TRUE;
}

// 取余
static size_t req_hash(const void *data, size_t data_size, 
	size_t hashtable_size)
{
    uint64_t u = *(uint64_t *)data;
	
    return u % hashtable_size;
}

// 去hash table 里面找到对应 id 的blk info
block_info_t *block_object_get(long id)
{
    pthread_rwlock_rdlock(&g_dn_bcm->cache_rwlock); //读锁定读写锁

	block_info_t *blk = (block_info_t *)dfs_hashtable_lookup(g_dn_bcm->blk_htable, 
		&id, sizeof(id));

	pthread_rwlock_unlock(&g_dn_bcm->cache_rwlock);
	
    return blk;
}

// 更新 hashtable 和 g_blk_report
// 数据节点每次初始化就需要重建一次hash table
int block_object_add(char *path, long ns_id, long blk_id)
{
    block_info_t *blk = nullptr;
    // 去hash table 里面找到对应 id 的blk info
	blk = block_object_get(blk_id);
	if (blk) 
	{
	    // check diff
        return NGX_OK;
	}

	struct stat sb;
	stat(path, &sb);
	long blk_sz = sb.st_size;

	pthread_rwlock_wrlock(&g_dn_bcm->cache_rwlock); // 写锁定？

	// 从之前初始化的 gbcm缓存中分配一个blk
	blk = (block_info_t *)mem_get0(g_dn_bcm->mem_mgmt.free_mblks);
	if (!blk)
	{
	    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0, "mem_get0 err");
	}

	queue_init(&blk->me);
	
    blk->id = blk_id;
	blk->size = blk_sz;
	strcpy(blk->path, path);

	blk->ln.key = &blk->id;
    blk->ln.len = sizeof(blk->id);
    blk->ln.next = nullptr;

	dfs_hashtable_join(g_dn_bcm->blk_htable, &blk->ln);

	pthread_rwlock_unlock(&g_dn_bcm->cache_rwlock);

    // blk info插入 g_blk_report
    // 不在hashtable里的向nn上报
    notify_blk_report(blk);
	
    return NGX_OK;
}

//
int block_object_del(long blk_id)
{
    block_info_t *blk = nullptr;

	blk = block_object_get(blk_id);
	if (!blk) 
	{
        return NGX_ERROR;
	}

	unlink(blk->path);
	
	pthread_rwlock_wrlock(&g_dn_bcm->cache_rwlock);

    dfs_hashtable_remove_link(g_dn_bcm->blk_htable, &blk->ln);

	mem_put(blk);// reback mem

	pthread_rwlock_unlock(&g_dn_bcm->cache_rwlock);
    
    return NGX_OK;
}

int block_read(dn_request_t *r, file_io_t *fio)
{
    return NGX_OK;
}

void io_lock(volatile uint64_t *lock)
{
    uint64_t l;

    do 
	{
        l = *lock;
    } while (!CAS(lock, l, *lock + 1));
}

uint64_t io_unlock(volatile uint64_t *lock)
{
    uint64_t l;

    do 
	{
        l = *lock;
    } while (!CAS(lock, l, *lock - 1));

    return l - 1;
}

int io_lock_zero(volatile uint64_t *lock)
{
    return CAS(lock, 0, 0);
}

int get_block_temp_path(dn_request_t *r)
{
    char tmpDir[PATH_LEN] = "";
	char curDir[PATH_LEN] = "";

	get_disk_id(r->header.block_id, curDir);
	sprintf(tmpDir, "%s/NS-%ld/blocksBeingWritten/blk_%ld", 
		curDir, r->header.namespace_id, r->header.block_id);
	
	r->path = string_xxxpdup(r->pool, (uchar_t *)tmpDir, strlen(tmpDir));
	if (!r->path) 
	{
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
			"string_xxxpdup %s err", tmpDir);
		
	    return NGX_ERROR;
	}
	
    return NGX_OK;
}

int write_block_done(dn_request_t *r)
{
    char curDir[PATH_LEN] = "";
	char blkDir[PATH_LEN] = "";
	int  suddir_id = 0;
	int  suddir_id2 = 0;

	suddir_id = r->header.block_id % SUBDIR_LEN;
	suddir_id2 = (r->header.block_id % 1000) % SUBDIR_LEN;

	get_disk_id(r->header.block_id, curDir);
	sprintf(blkDir, "%s/NS-%ld/current/subdir%d/subdir%d/blk_%ld", 
		curDir, r->header.namespace_id, suddir_id, suddir_id2, 
		r->header.block_id);

	// 调用rename快速移动文件，但是rename不能跨分区跨磁盘
	if (rename((char *)r->path, blkDir) != NGX_OK)
	{
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
			"rename %s to %s err", r->path, blkDir);
		
        return NGX_ERROR;
	}

	strcpy((char *)r->path, blkDir);
	    
    return recv_blk_report(r);
}

static int get_disk_id(long block_id, char *path)
{
    queue_t *head = nullptr;
	queue_t *entry = nullptr;
	int      disk_id = 0;

	disk_id = block_id % g_storage_dir_n;

	head = &g_storage_dir_q;
	entry = queue_next(head);

	while (head != entry) 
	{
        storage_dir_t *sd = queue_data(entry, storage_dir_t, me);

		entry = queue_next(entry);

		if (sd->id == disk_id) 
		{
		    strcpy(path, sd->current);
			
            break;
		}
	}

	return NGX_OK;
}

static int recv_blk_report(dn_request_t *r)
{
    block_info_t *blk = nullptr;
		
    pthread_rwlock_wrlock(&g_dn_bcm->cache_rwlock);

	blk = (block_info_t *)mem_get0(g_dn_bcm->mem_mgmt.free_mblks);
	if (!blk)
	{
	    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0, "mem_get0 err");
	}

	queue_init(&blk->me);
	
    blk->id = r->header.block_id;
	blk->size = r->header.len;
	strcpy(blk->path, (const char *)r->path);

	blk->ln.key = &blk->id;
    blk->ln.len = sizeof(blk->id);
    blk->ln.next = nullptr;

	dfs_hashtable_join(g_dn_bcm->blk_htable, &blk->ln);

	pthread_rwlock_unlock(&g_dn_bcm->cache_rwlock);

	// 提示name node 收到 blk
    notify_nn_receivedblock(blk);
    
    return NGX_OK;
}

// scanner线程
void *blk_scanner_start(void *arg)
{
	conf_server_t *sconf = nullptr;
	int            blk_report_interval = 0;
	unsigned long  last_blk_report = 0;

	sconf = (conf_server_t *)dfs_cycle->sconf;
    blk_report_interval = sconf->block_report_interval;

	//struct timeval now;
	//gettimeofday(&now, nullptr);
	//unsigned long diff = now.tv_sec * 1000 + now.tv_usec / 1000;

	//last_blk_report = diff;

	while (blk_scanner_running)  // 默认 true
	{
        //gettimeofday(&now, nullptr);
	    //diff = (now.tv_sec * 1000 + now.tv_usec / 1000) - last_blk_report;

		//if (diff >= blk_report_interval) 
		//{
        //
		//}

		// scan dir
		queue_t *head = &g_storage_dir_q;
		queue_t *entry = queue_next(head);

		while (head != entry) 
		{
            storage_dir_t *sd = queue_data(entry, storage_dir_t, me);

			entry = queue_next(entry);

			scan_current_dir(sd->current);
		}

		sleep(blk_report_interval);
	}
	
    return nullptr;
}

static int scan_current_dir(char *dir)
{
    char           root[PATH_LEN] = "";
	DIR           *p_dir = nullptr;
	struct dirent *ent = nullptr;
	
	p_dir = opendir(dir);
	if (nullptr == p_dir)
	{
	    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
			"opendir %s err", dir);
		
        return NGX_ERROR;
	}

	while (nullptr != (ent = readdir(p_dir)))
	{
        if (ent->d_type == 8) 
		{
	        // file
		}
		else if (0 == strncmp(ent->d_name, "NS-", 3)) 
		{
            char namespace_id[16] = "";
			get_namespace_id(ent->d_name, namespace_id);

            sprintf(root, "%s/%s/current", dir, ent->d_name);
			scan_namespace_dir(root, atol(namespace_id));
		}
	}

	closedir(p_dir);
	
    return NGX_OK;
}

static void get_namespace_id(char *src, char *id)
{
    char *pTemp = src + 3;

    while (*pTemp != '\0') 
	{
        *id++ = *pTemp++;
	}
}

    static int scan_namespace_dir(char *dir, long namespace_id)
{
    char           root[PATH_LEN] = "";
	DIR           *p_dir = nullptr;
	struct dirent *ent = nullptr;
	
	p_dir = opendir(dir);
	if (nullptr == p_dir)
	{
	    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
			"opendir %s err", dir);
		
        return NGX_ERROR;
	}

	while (nullptr != (ent = readdir(p_dir)))
	{
        if (ent->d_type == 8) 
		{
	        // file
		}
		else if (0 == strncmp(ent->d_name, "subdir", 6)) 
		{
            sprintf(root, "%s/%s", dir, ent->d_name);
            scan_subdir(root, namespace_id);
		}
	}

	closedir(p_dir);
	
    return NGX_OK;
}

static int scan_subdir(char *dir, long namespace_id)
{
    char           root[PATH_LEN] = "";
	DIR           *p_dir = nullptr;
	struct dirent *ent = nullptr;
	
	p_dir = opendir(dir);
	if (nullptr == p_dir)
	{
	    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
			"opendir %s err", dir);
		
        return NGX_ERROR;
	}

	while (nullptr != (ent = readdir(p_dir)))
	{
        if (ent->d_type == 8) 
		{
	        // file
		}
		else if (0 == strncmp(ent->d_name, "subdir", 6)) 
		{
		    sprintf(root, "%s/%s", dir, ent->d_name);
            scan_subdir_subdir(root, namespace_id);
		}
	}

	closedir(p_dir);
	
    return NGX_OK;
}

static int scan_subdir_subdir(char *dir, long namespace_id)
{
    char           path[PATH_LEN] = "";
	DIR           *p_dir = nullptr;
	struct dirent *ent = nullptr;
	
	p_dir = opendir(dir);
	if (nullptr == p_dir)
	{
	    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
			"opendir %s err", dir);
		
        return NGX_ERROR;
	}

	while (nullptr != (ent = readdir(p_dir)))
	{
	    // 如果是常规文件，
        if (DT_REG == ent->d_type && 0 == strncmp(ent->d_name, "blk_", 4))
		{
	        char blk_id[16] = "";
			get_blk_id(ent->d_name, blk_id);

			sprintf(path, "%s/%s", dir, ent->d_name);
            // 更新 hashtable 和 g_blk_report
			block_object_add(path, namespace_id, atol(blk_id));
		}
	}

	closedir(p_dir);
	
    return NGX_OK;
}

static void get_blk_id(char *src, char *id)
{
    char *pTemp = src + 4;

    while (*pTemp != '\0') 
	{
        *id++ = *pTemp++;
	}
}

