#include "nn_module.h"
#include "nn_error_log.h"
#include "nn_rpc_server.h"
#include "nn_paxos.h"
#include "nn_file_index.h"
#include "nn_dn_index.h"
#include "nn_blk_index.h"

static int dfs_mod_max = 0;
//int       (*master_init)(cycle_t *cycle); //初始化master
//int       (*master_release)(cycle_t *cycle); 
//int       (*worker_init)(cycle_t *cycle); 
//int       (*worker_release)(cycle_t *cycle);
//int       (*worker_thread_init)(dfs_thread_t *thread);//初始化线程
//int       (*worker_thread_release)(dfs_thread_t *thread);
dfs_module_t nn_modules[] = 
{
    // please miss the beginning;
    {
        string_make("errlog"),
        0,
        PROCESS_MOD_INIT,
        nullptr,
        nn_error_log_init,
        nn_error_log_release,
        nullptr,
        nullptr,
        nullptr,
        nullptr
    },

    {
        string_make("paxos"),
        0,
        PROCESS_MOD_INIT,
        nullptr,
        nullptr,
        nullptr,
        nn_paxos_worker_init, // paxos worker init // 配置当前运行节点的IP/PORT参数 // 初始化FSEditlog 对象
        nn_paxos_worker_release,
        nullptr,
        nullptr
    },

    {
        string_make("rpc_server"),
        0,
        PROCESS_MOD_INIT,
        nullptr,
        nullptr,
        nullptr,
        nn_rpc_worker_init, // empty
        nn_rpc_worker_release,
        nullptr,
        nullptr
    },

    {
        string_make("file_index"),
        0,
        PROCESS_MOD_INIT,
        nullptr,
        nullptr,
        nullptr,
        nn_file_index_worker_init, //初始化fi_cache_mgmt_t fcm 预先分配index_num个 fi_store_t // init timer // init g_checkpoint_q
        nn_file_index_worker_release,
        nullptr,
        nullptr
    },

	{
        string_make("dn_index"),
        0,
        PROCESS_MOD_INIT,
        nullptr,
        nullptr,
        nullptr,
        nn_dn_index_worker_init, // // 初始化 dcm data cache management // 创建index num 个dn_store_t // 初始化一些timer
        nn_dn_index_worker_release,
        nullptr,
        nullptr
    },

	{
        string_make("blk_index"),
        0,
        PROCESS_MOD_INIT,
        nullptr,
        nullptr,
        nullptr,
        nn_blk_index_worker_init, // 初始化 g_nn_bcm // 创建 blk_store_t
        nn_blk_index_worker_release,
        nullptr,
        nullptr
    },

    {string_null, 0, 0, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr}
};

void ngx_module_setup(void)
{
    int i = 0;
	
    for (i = 0; nn_modules[i].name.data != nullptr; i++)
	{
        nn_modules[i].index = dfs_mod_max++;
    }
}

// nn_error_log_init
int ngx_module_master_init(cycle_t *cycle)
{
    int i = 0;
	
    for (i = 0; i < dfs_mod_max; i++) 
	{
        if (nn_modules[i].master_init != nullptr &&
            nn_modules[i].master_init(cycle) == NGX_ERROR)
        {
            printf("process_master_init: module %s init failed\n",
                nn_modules[i].name.data);
			
            return NGX_ERROR;
        }

        nn_modules[i].flag = PROCESS_MOD_FREE;
    }
	
    return NGX_OK;
}

int ngx_module_master_release(cycle_t *cycle)
{
    int i = 0;
	
    for (i = dfs_mod_max - 1; i >= 0; i--) 
	{
        if (nn_modules[i].flag != PROCESS_MOD_FREE  ||
            nn_modules[i].master_release == nullptr)
        {
            continue;
        }
		
        if (nn_modules[i].master_release(cycle) == NGX_ERROR)
		{
            dfs_log_error(cycle->error_log, DFS_LOG_ERROR, 0,
                "%s deinit fail \n",nn_modules[i].name.data);
			
            return NGX_ERROR;
        }
		
        nn_modules[i].flag = PROCESS_MOD_INIT;
    }
	
    return NGX_OK;
}

int ngx_module_woker_init(cycle_t *cycle)
{
    int i = 0;
	
    for (i = 0; i < dfs_mod_max; i++) 
	{
		//nn_paxos_worker_init
		//nn_rpc_worker_init
		//nn_file_index_worker_init
		//nn_dn_index_worker_init
		//nn_blk_index_worker_init
        if (nn_modules[i].worker_init != nullptr &&
            nn_modules[i].worker_init(cycle) == NGX_ERROR)
        {
            dfs_log_error(cycle->error_log, DFS_LOG_ERROR, 0,
                "dfs_module_init_woker: module %s init failed\n",
                nn_modules[i].name.data);
			
            return NGX_ERROR;
        }
    }
	
    return NGX_OK;
}

int ngx_module_woker_release(cycle_t *cycle)
{
    int i = 0;
	
    for (i = 0; i < dfs_mod_max; i++) 
	{
        if (nn_modules[i].worker_release!= nullptr &&
            nn_modules[i].worker_release(cycle) == NGX_ERROR)
        {
            dfs_log_error(cycle->error_log, DFS_LOG_ERROR, 0,
                "dfs_module_init_woker: module %s init failed\n",
                nn_modules[i].name.data);
			
            return NGX_ERROR;
        }
    }
	
    return NGX_OK;
}

int ngx_module_workethread_init(dfs_thread_t *thread)
{
    int i = 0;
	
    for (i = 0; i < dfs_mod_max; i++) 
	{
        if (nn_modules[i].worker_thread_init != nullptr &&
            nn_modules[i].worker_thread_init(thread) == NGX_ERROR)
        {
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, 0,
                "dfs_module_init_woker: module %s init failed\n",
                nn_modules[i].name.data);
			
            return NGX_ERROR;
        }
    }
	
    return NGX_OK;
}

int ngx_module_wokerthread_release(dfs_thread_t *thread)
{
    int i = 0;

    for (i = 0; i < dfs_mod_max; i++) 
	{
        if (nn_modules[i].worker_thread_release != nullptr &&
            nn_modules[i].worker_thread_release(thread) == NGX_ERROR)
        {
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, 0,
                "dfs_module_init_woker: module %s init failed\n",
                nn_modules[i].name.data);
			
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}

