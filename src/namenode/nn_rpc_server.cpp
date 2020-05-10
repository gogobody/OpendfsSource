#include "nn_rpc_server.h"
#include "nn_thread.h"
#include "nn_net_response_handler.h"
#include "dfs_task.h"
#include "nn_task_handler.h"
#include "nn_conf.h"
#include "nn_file_index.h"
#include "nn_dn_index.h"

int nn_rpc_worker_init(cycle_t *cycle)
{
    //conf_server_t *conf = nullptr;
	//conf = (conf_server_t *)cycle->sconf;

    return DFS_OK;
}

int nn_rpc_worker_release(cycle_t *cycle)
{
    (void) cycle;

    return DFS_OK;
}

// do task
// 重新push到不同的队列里 // dn 和 cli push 到bque队列，其他线程push到 tq队列里
// from notice_wake_up
int nn_rpc_service_run(task_t *task)
{
    int optype = task->cmd;
	
	switch (optype)
    {
    case NN_MKDIR:
		nn_mkdir(task);// push task to paxos thread tq
		break;

	case NN_RMR:
		nn_rmr(task);// push task to paxos thread tq
		break;

	case NN_LS:
		nn_ls(task); // diff:
		break;
		
	case NN_GET_FILE_INFO: // no use
		nn_get_file_info(task);
		break;
    // cli put file
    //
	case NN_CREATE:
		nn_create(task); // push task to paxos thread tq
		break;

	case NN_GET_ADDITIONAL_BLK:
		nn_get_additional_blk(task); // same
		break;

	case NN_CLOSE:
		nn_close(task); // same
		break;

	case NN_RM:
		nn_rm(task); // same
		break;

	case NN_OPEN:
		nn_open(task); // diff :
		break;

	case DN_REGISTER:
		nn_dn_register(task); // diff :
		break;

	case DN_HEARTBEAT:
		nn_dn_heartbeat(task); // include blk del
		break;

	case DN_RECV_BLK_REPORT: // 当dn接收到 blk 后向nn 上报
		nn_dn_recv_blk_report(task); //
		break;

	case DN_DEL_BLK_REPORT: // no use
		nn_dn_del_blk_report(task);
		break;

	case DN_BLK_REPORT: // 当 dn 重启后在dn 内重建映射，并向 nn上报
		nn_dn_blk_report(task);
		break;
		
	default:
		dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0, 
			"unknown optype: ", optype);
		
		return DFS_ERROR;
	}
	
    return DFS_OK;
}

