#include <string>
#include "nn_paxos.h"
#include "dfs_task.h"
#include "FSEditlog.h"
#include "phxeditlog.pb.h"
#include "fs_permission.h"
#include "nn_conf.h"
#include "nn_task_queue.h"
#include "nn_thread.h"
#include "nn_net_response_handler.h"
#include "nn_blk_index.h"
#include "nn_dn_index.h"

using namespace phxpaxos;
using namespace phxeditlog;
using namespace std;

static FSEditlog *g_editlog = nullptr;
static uint32_t   g_edit_op_num = 0;

extern uint64_t g_fs_object_num;
extern _xvolatile rb_msec_t dfs_current_msec;

static int do_paxos_task(task_t *task);
static int log_mkdir(task_t *task);
static int log_rmr(task_t *task);
static int inc_edit_op_num();
static void *checkpoint_start(void *arg);
static int log_create(task_t *task);
static int log_get_additional_blk(task_t *task);
static int log_close(task_t *task);
static int log_rm(task_t *task);

FSEditlog* nn_get_paxos_obj(){
    return g_editlog;
}

static int parse_ipport(const char * pcStr, NodeInfo & oNodeInfo)
{
    char sIP[32] = {0};
    int iPort = -1;

    int count = sscanf(pcStr, "%[^':']:%d", sIP, &iPort);
    if (count != 2)
    {
        return NGX_ERROR;
    }

    oNodeInfo.SetIPPort(sIP, iPort);

    return NGX_OK;
}

static int parse_ipport_list(const char * pcStr, 
	NodeInfoList & vecNodeInfoList)
{
    string sTmpStr;
    int iStrLen = strlen(pcStr);

    for (int i = 0; i < iStrLen; i++)
    {
        if (pcStr[i] == ',' || i == iStrLen - 1)
        {
            if (i == iStrLen - 1 && pcStr[i] != ',')
            {
                sTmpStr += pcStr[i];
            }
            
            NodeInfo oNodeInfo;
            int ret = parse_ipport(sTmpStr.c_str(), oNodeInfo);
            if (ret != 0)
            {
                return ret;
            }

            vecNodeInfoList.push_back(oNodeInfo);

            sTmpStr = "";
        }
        else
        {
            sTmpStr += pcStr[i];
        }
    }

    return NGX_OK;
}

// paxos worker init
// 配置当前运行节点的IP/PORT参数
// 初始化FSEditlog 对象
int nn_paxos_worker_init(cycle_t *cycle)
{
    conf_server_t *sconf = (conf_server_t *)cycle->sconf;

    NodeInfo oMyNode; //当前运行节点的IP/PORT参数
    if (parse_ipport((const char *)sconf->my_paxos.data, oMyNode) != NGX_OK)
    {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, 0, 
            "parse myip:myport err");
		
        return NGX_ERROR;
    }

    NodeInfoList vecNodeList; // Paxos由多个节点构成，这个列表设置这些节点的IP/PORT信息。
    // 当开启了PhxPaxos的成员管理功能后，这个信息仅仅会被接受一次作为集群的初始化，后面将会无视这个参数的存在。
    if (parse_ipport_list((const char *)sconf->ot_paxos.data, vecNodeList) 
		!= NGX_OK)
    {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, 0, 
            "parse ip:port err");
		
        return NGX_ERROR;
    }
    
    string editlogDir = string((const char *)sconf->editlog_dir.data);
    
    int iGroupCount = (int)sconf->paxos_group_num;

    g_editlog = new FSEditlog(sconf->enableMaster, oMyNode, vecNodeList, editlogDir, iGroupCount);
    if (nullptr == g_editlog)
    {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, 0, 
            "new FSEditlog(...) err");
     
        return NGX_ERROR;
    }

    return NGX_OK;
}

int nn_paxos_worker_release(cycle_t *cycle)
{
    if (nullptr != g_editlog)
    {
        delete g_editlog;
		g_editlog = nullptr;
    }

    return NGX_OK;
}

int nn_paxos_run()
{

    return g_editlog->RunPaxos();
}

void set_checkpoint_instanceID(const uint64_t llInstanceID)
{
    g_editlog->setCheckpointInstanceID(llInstanceID);
}

// paxos event call back
// pop task from task queue and do_paxos_task()
void do_paxos_task_handler(void *q) // param task queue
{
    task_queue_node_t *tnode = nullptr;
	task_t            *t = nullptr;
	queue_t           *cur = nullptr;
	queue_t            qhead;
	task_queue_t      *tq = nullptr;
    dfs_thread_t      *thread = nullptr;

	tq = (task_queue_t *)q; // task que
    thread = get_local_thread(); // THREAD_TASK
	
    queue_init(&qhead);
	pop_all(tq, &qhead);
	
	cur = queue_head(&qhead);
	
	while (!queue_empty(&qhead) && thread->running)
	{
		tnode = queue_data(cur, task_queue_node_t, qe);
		t = &tnode->tk;
		
        queue_remove(cur);
		
        do_paxos_task(t);
		
		cur = queue_head(&qhead);
	}
}

//paxos thread
static int do_paxos_task(task_t *task)
{
    int optype = task->cmd;

	switch (optype)
    {
    case NN_MKDIR:
		log_mkdir(task);
		break;

	case NN_RMR:
		log_rmr(task);
		break;
		
	case NN_GET_FILE_INFO:
		break;

	// cli put file
	case NN_CREATE:
		log_create(task);
		break;

	case NN_GET_ADDITIONAL_BLK:
		log_get_additional_blk(task);
		break;

	case NN_CLOSE:
		log_close(task);
		break;

	case NN_RM:
		log_rm(task);
		break;

	case NN_OPEN:
		break;
		
	default:
		dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0, 
			"unknown optype: ", optype);
		
		return NGX_ERROR;
	}
	
    return NGX_OK;
}

int check_traverse(uchar_t *path, task_t *task, 
	fi_inode_t *finodes[], int num)
{
    uchar_t err[1024] = "";

	for (int i = 0; i < num; i++) 
	{
	    if (nullptr == finodes[i])
		{
            break;
		}
		
		if (check_permission(task, finodes[i], EXECUTE, err) != NGX_OK)
		{
		    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0, 
				"check_permission err: %s, path: %s", err, path);

			return NGX_ERROR;
		}
	}
	
    return NGX_OK;
}

//
int check_ancestor_access(uchar_t *path, task_t *task, 
	short access, fi_inode_t *finode)
{
    uchar_t err[1024] = "";

	if (check_permission(task, finode, access, err) != NGX_OK)
	{
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0, 
			"check_permission err: %s, path: %s", err, path);

		return NGX_ERROR;
    }
	
    return NGX_OK;
}

//
static int log_mkdir(task_t *task)
{
    int            expect_mkdir_num = 0;
	int            parent_index = 0;
	fi_inode_t    *finodes[PATH_LEN]={};

	conf_server_t *sconf = (conf_server_t *)dfs_cycle->sconf;
	
    task_queue_node_t *node = queue_data(task, task_queue_node_t, tk);

    printf("master ip:port %s:%d \n",g_editlog->GetMaster(task->key).GetIP().c_str(),g_editlog->GetMaster(task->key).GetPort());

    if (!g_editlog->IsIMMaster(task->key)) // 不是master 节点 就重定向到master节点
	{
        task->ret = MASTER_REDIRECT;
		task->master_nodeid = g_editlog->GetMaster(task->key).GetNodeID();
        printf("master node id:%d\n ",task->master_nodeid);

		return write_back(node);
	}

	// 如果是master节点
	fi_store_t *fi = get_store_obj((uchar_t *)task->key);
	if (fi) 
	{
		task->ret = KEY_EXIST;

		return write_back(node);
	}

	// 不存在就创建
	uchar_t path[PATH_LEN] = "";
	get_store_path((uchar_t *)task->key, path);

	uchar_t names[PATH_LEN][PATH_LEN];
    int names_sz = get_path_names(path, names); // 按照 "/" 分割

	uchar_t keys[PATH_LEN][PATH_LEN];
    get_path_keys(names, names_sz, keys); // encode 分成每一层/a/b/c /a /a/b /a/b/c

	if (0 == string_strncmp("/", path, string_strlen(path)))
	{
	    goto do_paxos;
	}

	// init finodes?


	// 找到哪一级目录不存在
	parent_index = get_path_inodes(keys, names_sz, finodes);

	if (parent_index > 0 && finodes[parent_index]->is_directory == NGX_FALSE)
	{
	    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0, 
			"Parent path is not a directory: %s", path);

		task->ret = NOT_DIRECTORY;

		return write_back(node);
	}
	// authentic check permission
    if (!is_super(task->user, &dfs_cycle->admin))
    {
        if (parent_index < 0) // PERMISSION_DENY
		{
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0, 
				"mkdir %s err, user: %s, group: %s", 
				path, task->user, task->group);

			task->ret = PERMISSION_DENY;

			return write_back(node);
		}
		
        if (check_traverse(path, task, finodes, names_sz) != NGX_OK)
	    {
            task->ret = PERMISSION_DENY;

			return write_back(node);
	    }

		if (check_ancestor_access(path, task, WRITE, finodes[parent_index]) 
			!= NGX_OK)
	    {
            task->ret = PERMISSION_DENY;

			return write_back(node);
	    }
    }

	expect_mkdir_num = names_sz - parent_index - 1;

    // 是否超过了最大目录数
	if (is_FsObjectExceed(expect_mkdir_num))
	{
	    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0, 
			"fs object exceed, current num: %ld, max num: %ld, path: %s", 
			g_fs_object_num, sconf->index_num, path);
		
        task->ret = FSOBJECT_EXCEED;

		return write_back(node);
	}

do_paxos:
	string sPaxosValue;
	PhxEditlogSMCtx oEditlogSMCtx;
	string sKey;
	LogOperator lopr; // protobuf
	lopr.set_optype(task->cmd);
	lopr.mutable_mkr()->set_permission(task->permission); // mutable 若该对象存在，则直接返回该对象，若不存在则新new 一个
	lopr.mutable_mkr()->set_owner(task->user);
	lopr.mutable_mkr()->set_group(task->group);
	lopr.mutable_mkr()->set_modification_time(dfs_current_msec);

	for (int i = 0; i < names_sz; i++) 
	{
	    if (1 == names_sz) 
		{
			// create "/" dir only
			sKey = string((const char *)task->key);
		}
		else if (parent_index < 0)  // 整个目录都不在
		{
            // create "/home/..." dir at one time
            sKey = string((const char *)keys[i]);
		}
		else
		{
		    // parent path is exist
		    if (nullptr != finodes[i]
				&& (0 == string_strncmp(finodes[i]->key, 
				keys[i], string_strlen(keys[i])))) 
			{
			    continue;
			}

			sKey = string((const char *)keys[i]);
		}

		lopr.mutable_mkr()->set_key(sKey);
	    lopr.SerializeToString(&sPaxosValue);
        // 写入
	    g_editlog->Propose(sKey, sPaxosValue, oEditlogSMCtx);
	}

	task->ret = SUCC;

	inc_edit_op_num();
	
	return write_back(node);
}

static int log_rmr(task_t *task)
{
    task_queue_node_t *node = queue_data(task, task_queue_node_t, tk);

	if (!g_editlog->IsIMMaster(task->key)) 
	{
        task->ret = MASTER_REDIRECT;
		task->master_nodeid = g_editlog->GetMaster(task->key).GetNodeID();

		return write_back(node);
	}

	fi_store_t *fi = get_store_obj((uchar_t *)task->key);
	if (!fi) 
	{
		task->ret = KEY_NOTEXIST;

		return write_back(node);
	}
	else if (!fi->fin.is_directory)
	{
        task->ret = NOT_DIRECTORY;

		return write_back(node);
	}

	uchar_t path[PATH_LEN] = "";
	get_store_path((uchar_t *)task->key, path);

	fi_inode_t finode = fi->fin;
	
    if (!is_super(task->user, &dfs_cycle->admin))
    {
		if (check_ancestor_access(path, task, WRITE, &finode) != NGX_OK)
	    {
            task->ret = PERMISSION_DENY;

			return write_back(node);
	    }
    }
	
    string sPaxosValue;
	PhxEditlogSMCtx oEditlogSMCtx;
	LogOperator lopr;
	lopr.set_optype(task->cmd);
	lopr.mutable_rmr()->set_key((const char *)task->key);
	lopr.mutable_rmr()->set_modification_time(dfs_current_msec);
	lopr.SerializeToString(&sPaxosValue);

	g_editlog->Propose((const char *)task->key, sPaxosValue, oEditlogSMCtx);

	task->ret = SUCC;

	inc_edit_op_num();
	
	return write_back(node);
}

// inc g_edit_op_num
static int inc_edit_op_num()
{
    conf_server_t *conf = (conf_server_t *)dfs_cycle->sconf;
	
    g_edit_op_num++;
	if (g_edit_op_num == conf->checkpoint_num) // 操作数达到了 checkpoint_num
	{
	    pthread_t pid;
		
	    if (pthread_create(&pid, nullptr, &checkpoint_start, nullptr) != NGX_OK)
		{
		    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno, 
				"create checkpoint thread failed");

			//return NGX_ERROR;
		}
	    
	    g_edit_op_num = 0; 
	}
	
    return NGX_OK;
}

static void * checkpoint_start(void *arg)
{
	do_checkpoint();
	
    return nullptr;
}

// cli put file
static int log_create(task_t *task)
{
	int                parent_index = 0;
	fi_inode_t        *finodes[PATH_LEN];
	conf_server_t     *sconf = nullptr;
	create_blk_info_t  blk_info;
	create_resp_info_t resp_info;

	sconf = (conf_server_t *)dfs_cycle->sconf;

    memset(&resp_info, 0x00, sizeof(create_resp_info_t));
	memset(&blk_info, 0x00, sizeof(create_blk_info_t));
	memcpy(&blk_info, task->data, sizeof(create_blk_info_t));

	task->data = nullptr;
	task->data_len = 0;
	
    task_queue_node_t *node = queue_data(task, task_queue_node_t, tk);

    // 不是主master
	if (!g_editlog->IsIMMaster(task->key))  // key 是dst 目录
	{
        task->ret = MASTER_REDIRECT;
		task->master_nodeid = g_editlog->GetMaster(task->key).GetNodeID();

		return write_back(node);
	}

	// master 节点
	fi_store_t *fi = get_store_obj((uchar_t *)task->key); // key 是dst 目录
	if (fi) // 元数据存在
	{
	    //
	    if (fi->state == KEY_STATE_OK) 
		{
            task->ret = KEY_EXIST;
		}
		else 
		{
            task->ret = KEY_STATE_CREATING;
		}
		
		return write_back(node);
	}
    // 元数据不存在
	uchar_t path[PATH_LEN] = "";
	get_store_path((uchar_t *)task->key, path); // decode path

	uchar_t names[PATH_LEN][PATH_LEN];
    int names_sz = get_path_names(path, names); //

	uchar_t keys[PATH_LEN][PATH_LEN];
    get_path_keys(names, names_sz, keys); // 把每一级目录都 encode方便查找是否已经创建过

	parent_index = get_path_inodes(keys, names_sz, finodes);

	if ((parent_index <= 0) || (parent_index > 0 
		&& finodes[parent_index]->is_directory == NGX_FALSE))
	{
	    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0, 
			"Parent path is not a directory: %s", path);

		task->ret = NOT_DIRECTORY;

		return write_back(node);
	}
	
    if (!is_super(task->user, &dfs_cycle->admin))
    {
        if (parent_index < 0) 
		{
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0, 
				"create %s err, user: %s, group: %s", 
				path, task->user, task->group);

			task->ret = PERMISSION_DENY;

			return write_back(node);
		}
		
        if (check_traverse(path, task, finodes, names_sz) != NGX_OK)
	    {
            task->ret = PERMISSION_DENY;

			return write_back(node);
	    }

		if (check_ancestor_access(path, task, WRITE, finodes[parent_index]) 
			!= NGX_OK)
	    {
            task->ret = PERMISSION_DENY;

			return write_back(node);
	    }
    }

	if (is_FsObjectExceed(1))
	{
	    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0, 
			"fs object exceed, current num: %ld, max num: %ld, path: %s", 
			g_fs_object_num, sconf->index_num, path);
		
        task->ret = FSOBJECT_EXCEED;

		return write_back(node);
	}

	// 从 g_dn_q 中选择一个存储节点 datanode
	if (generate_dns(blk_info.blk_rep, &resp_info) != NGX_OK)
	{
        task->ret = NOT_DATANODE;

		return write_back(node);
	}

	//
	resp_info.blk_id = generate_uid();
	resp_info.namespace_id = dfs_cycle->namespace_id;
	
	LogOperator lopr;
	lopr.set_optype(task->cmd);
	lopr.mutable_cre()->set_key((const char *)task->key);
	lopr.mutable_cre()->set_permission(task->permission);
	lopr.mutable_cre()->set_owner(task->user);
	lopr.mutable_cre()->set_group(task->group);
	lopr.mutable_cre()->set_modification_time(dfs_current_msec);
	lopr.mutable_cre()->set_blk_id(resp_info.blk_id);
	lopr.mutable_cre()->set_blk_sz(blk_info.blk_sz);
	lopr.mutable_cre()->set_blk_rep(blk_info.blk_rep);
	// add blk seq
	lopr.mutable_cre()->set_blk_seq(blk_info.blk_seq);
    lopr.mutable_cre()->set_total_blk(blk_info.total_blk);

	string sPaxosValue;
	lopr.SerializeToString(&sPaxosValue);

    PhxEditlogSMCtx oEditlogSMCtx;
	oEditlogSMCtx.data = get_local_thread();
	
	g_editlog->Propose((const char *)task->key, sPaxosValue, oEditlogSMCtx);

	// response {blk_id, namespace_id, dn_ips}
	task->data_len = sizeof(create_resp_info_t);
	task->data = malloc(task->data_len);
	if (nullptr == task->data)
	{
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0, "malloc err");
	}
	
	memcpy(task->data, &resp_info, task->data_len);

	task->ret = SUCC;

	inc_edit_op_num();
	
	return write_back(node);
}

static int log_get_additional_blk(task_t *task)
{
	create_blk_info_t  blk_info;
	create_resp_info_t resp_info;

    memset(&resp_info, 0x00, sizeof(create_resp_info_t));
	memset(&blk_info, 0x00, sizeof(create_blk_info_t));
	memcpy(&blk_info, task->data, sizeof(create_blk_info_t));

	task->data = nullptr;
	task->data_len = 0;
	
    task_queue_node_t *node = queue_data(task, task_queue_node_t, tk);

	if (!g_editlog->IsIMMaster(task->key)) 
	{
        task->ret = MASTER_REDIRECT;
		task->master_nodeid = g_editlog->GetMaster(task->key).GetNodeID();

		return write_back(node);
	}

	fi_store_t *fi = get_store_obj((uchar_t *)task->key);
	if (fi && fi->state != KEY_STATE_CREATING) 
	{
        task->ret = FAIL;
		
		return write_back(node);
	}

    if (generate_dns(blk_info.blk_rep, &resp_info) != NGX_OK)
	{
        task->ret = NOT_DATANODE;

		return write_back(node);
	}

	resp_info.blk_id = generate_uid();
	resp_info.namespace_id = dfs_cycle->namespace_id;

	string sPaxosValue;
	PhxEditlogSMCtx oEditlogSMCtx;
	LogOperator lopr;
	lopr.set_optype(task->cmd);
	lopr.mutable_gab()->set_key((const char *)task->key);
	lopr.mutable_gab()->set_blk_id(resp_info.blk_id);
	lopr.mutable_gab()->set_blk_sz(blk_info.blk_sz);
	lopr.mutable_gab()->set_blk_rep(blk_info.blk_rep);
    
	lopr.SerializeToString(&sPaxosValue);

	g_editlog->Propose((const char *)task->key, sPaxosValue, oEditlogSMCtx);

	// response {blk_id, namespace_id, dn_ips}
	task->data_len = sizeof(create_resp_info_t);
	task->data = malloc(task->data_len);
	if (nullptr == task->data)
	{
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0, "malloc err");
	}
	
	memcpy(task->data, &resp_info, task->data_len);

	task->ret = SUCC;

	inc_edit_op_num();
	
	return write_back(node);
}

static int log_close(task_t *task)
{
    task_queue_node_t *node = queue_data(task, task_queue_node_t, tk);

	uint64_t len = *(uint64_t *)task->data;
	
	task->data = nullptr;
	task->data_len = 0;

	//if (!g_editlog->IsIMMaster(task->key)) 
	//{
    //    task->ret = MASTER_REDIRECT;
	//	task->master_nodeid = g_editlog->GetMaster(task->key).GetNodeID();
    //
	//	return write_back(node);
	//}

	fi_store_t *fi = get_store_obj((uchar_t *)task->key);
	if (fi && fi->state != KEY_STATE_CREATING) 
	{
        task->ret = FAIL;
		
		return write_back(node);
	}

	string sPaxosValue;
	PhxEditlogSMCtx oEditlogSMCtx;
	LogOperator lopr;
	lopr.set_optype(task->cmd);
	lopr.mutable_cle()->set_key((const char *)task->key);
	lopr.mutable_cle()->set_modification_time(dfs_current_msec);
	lopr.mutable_cle()->set_len(len);
	lopr.mutable_cle()->set_blk_rep(task->ret);
    
	lopr.SerializeToString(&sPaxosValue);

	g_editlog->Propose((const char *)task->key, sPaxosValue, oEditlogSMCtx);

	task->ret = SUCC;

	inc_edit_op_num();
	
	return write_back(node);
}

static int log_rm(task_t *task)
{
    task_queue_node_t *node = queue_data(task, task_queue_node_t, tk);

	if (!g_editlog->IsIMMaster(task->key)) 
	{
        task->ret = MASTER_REDIRECT;
		task->master_nodeid = g_editlog->GetMaster(task->key).GetNodeID();

		return write_back(node);
	}

	fi_store_t *fi = get_store_obj((uchar_t *)task->key);
	if (!fi) 
	{
		task->ret = KEY_NOTEXIST;

		return write_back(node);
	}
	else if (fi->fin.is_directory)
	{
        task->ret = NOT_FILE;

		return write_back(node);
	}

	uchar_t path[PATH_LEN] = "";
	get_store_path((uchar_t *)task->key, path);

	fi_inode_t finode = fi->fin;
	
    if (!is_super(task->user, &dfs_cycle->admin))
    {
		if (check_ancestor_access(path, task, WRITE, &finode) != NGX_OK)
	    {
            task->ret = PERMISSION_DENY;

			return write_back(node);
	    }
    }
	
    string sPaxosValue;
	PhxEditlogSMCtx oEditlogSMCtx;
	LogOperator lopr;
	lopr.set_optype(task->cmd);
	lopr.mutable_rm()->set_key((const char *)task->key);
	lopr.mutable_rm()->set_modification_time(dfs_current_msec);
	lopr.SerializeToString(&sPaxosValue);

	g_editlog->Propose((const char *)task->key, sPaxosValue, oEditlogSMCtx);

	task->ret = SUCC;

	inc_edit_op_num();
	
	return write_back(node);
}

