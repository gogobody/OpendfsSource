#include "dfscli_put.h"
#include "dfs_memory_pool.h"
#include "dfs_task_cmd.h"
#include "dfs_task.h"
#include "dfscli_conf.h"
#include "dfscli_cycle.h"

static int dfs_create(rw_context_t *rw_ctx);

static int dfs_close(rw_context_t *rw_ctx);

static int dfs_write_blk(rw_context_t *rw_ctx);

static void *write_blk_start(void *arg);

static int write_blk_to_dn(long fsize, int out_fd, int in_fd);

static int dfs_write_next_blk(rw_context_t *rw_ctx);

static int dfs_get_additional_blk(rw_context_t *rw_ctx);


// put a file to remote
// if u only send one file ,then blk_seq and total blk should be 1
int dfscli_put(char *src, char *dst, int blk_seq, int total_blk) {
    conf_server_t *sconf = nullptr;
    rw_context_t *rw_ctx = nullptr;

    sconf = (conf_server_t *) dfs_cycle->sconf;

    rw_ctx = (rw_context_t *) pool_alloc(dfs_cycle->pool,
                                         sizeof(rw_context_t));
    if (!rw_ctx) {
        dfscli_log(DFS_LOG_WARN, "dfscli_put, pool_alloc err");

        return NGX_ERROR;
    }

    // add file list info
    if (blk_seq < 1 || total_blk < 1 || blk_seq > total_blk) {
        dfscli_log(DFS_LOG_WARN, "dfscli_put, plz input right blk_seq or total blk");
        return NGX_ERROR;
    }
    rw_ctx->blk_seq = blk_seq;
    rw_ctx->total_blk = total_blk;

    // end
    strcpy(rw_ctx->src, src);
    strcpy(rw_ctx->dst, dst);
    rw_ctx->blk_sz = sconf->blk_sz; // default 256MB
    rw_ctx->blk_rep = sconf->blk_rep; // default 3
    // connect to namenode and send task
    // task.cmd NN_CREATE
    // task.key dst
    // task - userinfo - blk_info
    // recv task ret,blk_seq id,namespace id
    if (dfs_create(rw_ctx) != NGX_OK) {
        return NGX_ERROR;
    }
    // write to dn
    dfs_write_blk(rw_ctx);

    dfs_close(rw_ctx);

    if (rw_ctx->nn_fd > 0) {
        close(rw_ctx->nn_fd);
        rw_ctx->nn_fd = -1;
    }

    for (int i = 0; i < rw_ctx->dn_num; i++) {
        if (rw_ctx->dn_fd[i] > 0) {
            close(rw_ctx->dn_fd[i]);
            rw_ctx->dn_fd[i] = -1;
        }
    }

    return NGX_OK;
}

// connect to namenode and send task
// task.cmd NN_CREATE
// task.key dst
// task - userinfo - blk_info
// recv task ret
// rw_ctx->nn_fd = sockfd;
// rw_ctx->blk_id = resp_info.blk_id;
// rw_ctx->namespace_id = resp_info.namespace_id;
// rw_ctx->dn_num = resp_info.dn_num;
static int dfs_create(rw_context_t *rw_ctx) {
    conf_server_t *sconf = nullptr;
    server_bind_t *nn_addr = nullptr;
    create_blk_info_t blk_info;
    create_resp_info_t resp_info;

    sconf = (conf_server_t *) dfs_cycle->sconf;
    nn_addr = (server_bind_t *) sconf->namenode_addr.elts;
    // conn to name node
    int sockfd = dfs_connect((char *) nn_addr[0].addr.data, nn_addr[0].port);
    if (sockfd < 0) {
        return NGX_ERROR;
    }

    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = NN_CREATE;
    keyEncode((uchar_t *) rw_ctx->dst, (uchar_t *) out_t.key);

    getUserInfo(&out_t);

    out_t.permission = 755;

    memset(&blk_info, 0x00, sizeof(create_blk_info_t));
    blk_info.blk_sz = rw_ctx->blk_sz;
    blk_info.blk_rep = rw_ctx->blk_rep;

    // send file list info to nn
    blk_info.blk_seq = rw_ctx->blk_seq;
    blk_info.total_blk = rw_ctx->total_blk;
    // end

    out_t.data_len = sizeof(create_blk_info_t);
    out_t.data = &blk_info;

    char sBuf[BUF_SZ] = "";
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    int ws = write(sockfd, sBuf, sLen);
    if (ws != sLen) {
        dfscli_log(DFS_LOG_WARN, "write err, ws: %d, sLen: %d", ws, sLen);

        close(sockfd);

        return NGX_ERROR;
    }

    int pLen = 0;
    int rLen = recv(sockfd, &pLen, sizeof(int), MSG_PEEK);
    if (rLen < 0) {
        dfscli_log(DFS_LOG_WARN, "recv err, rLen: %d", rLen);

        close(sockfd);

        return NGX_ERROR;
    }

    char *pNext = (char *) pool_alloc(dfs_cycle->pool, pLen);
    if (!pNext) {
        dfscli_log(DFS_LOG_WARN, "pool_alloc err, pLen: %d", pLen);

        close(sockfd);

        return NGX_ERROR;
    }

    rLen = read(sockfd, pNext, pLen);
    if (rLen < 0) {
        dfscli_log(DFS_LOG_WARN, "read err, rLen: %d", rLen);

        close(sockfd);

        return NGX_ERROR;
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(pNext, rLen, &in_t);

    if (in_t.ret != NGX_OK) {
        if (in_t.ret == KEY_EXIST) {
            dfscli_log(DFS_LOG_WARN,
                       "create err, file %s is exist.", rw_ctx->dst);
        } else if (in_t.ret == KEY_STATE_CREATING) {
            dfscli_log(DFS_LOG_WARN,
                       "create err, file %s is creating.", rw_ctx->dst);
        } else if (in_t.ret == NOT_DIRECTORY) {
            dfscli_log(DFS_LOG_WARN,
                       "create err, parent path is not a directory.");
        } else if (in_t.ret == PERMISSION_DENY) {
            dfscli_log(DFS_LOG_WARN, "create err, permission deny.");
        } else if (in_t.ret == NOT_DATANODE) {
            dfscli_log(DFS_LOG_WARN, "create err, no datanode.");
        } else {
            dfscli_log(DFS_LOG_WARN, "create err, ret: %d", in_t.ret);
        }

        close(sockfd);

        return NGX_ERROR;
    } else if (nullptr != in_t.data && in_t.data_len > 0) {
        memset(&resp_info, 0x00, sizeof(create_resp_info_t));
        memcpy(&resp_info, in_t.data, in_t.data_len);

        rw_ctx->nn_fd = sockfd;
        rw_ctx->blk_id = resp_info.blk_id;
        rw_ctx->namespace_id = resp_info.namespace_id;
        rw_ctx->dn_num = resp_info.dn_num;
        memcpy(rw_ctx->dn_ips, resp_info.dn_ips, sizeof(resp_info.dn_ips));
    }

    return NGX_OK;
}

static int dfs_close(rw_context_t *rw_ctx) {
    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = NN_CLOSE;
    keyEncode((uchar_t *) rw_ctx->dst, (uchar_t *) out_t.key);
    out_t.ret = rw_ctx->write_done_blk_rep;
    out_t.data = &rw_ctx->fsize;
    out_t.data_len = sizeof(rw_ctx->fsize);

    char sBuf[BUF_SZ] = "";
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    int ws = write(rw_ctx->nn_fd, sBuf, sLen);
    if (ws != sLen) {
        dfscli_log(DFS_LOG_WARN, "write err, ws: %d, sLen: %d", ws, sLen);

        return NGX_ERROR;
    }

    char rBuf[BUF_SZ] = "";
    int rLen = read(rw_ctx->nn_fd, rBuf, sizeof(rBuf));
    if (rLen < 0) {
        dfscli_log(DFS_LOG_WARN, "read err, rLen: %d", rLen);

        return NGX_ERROR;
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(rBuf, rLen, &in_t);

    if (in_t.ret != NGX_OK) {
        dfscli_log(DFS_LOG_WARN, "close err, ret: %d", in_t.ret);

        return NGX_ERROR;
    }

    return NGX_OK;
}

//write blk to remote 
static int dfs_write_blk(rw_context_t *rw_ctx) {
    int ret = -1;
    short dn_num = rw_ctx->dn_num; // dn_num for nn in dfs_create

    for (short i = 0; i < dn_num; i++) {
        rw_ctx->dn_index = i;

        ret = pthread_create(&rw_ctx->thread_id[i], nullptr,
                             &write_blk_start, rw_ctx);
        if (ret != 0) {
            dfscli_log(DFS_LOG_WARN, "pthread_create[%d] err, %s",
                       i, strerror(errno));

            return NGX_ERROR;
        }
    }

    for (short i = 0; i < dn_num; i++) {
        ret = pthread_join(rw_ctx->thread_id[i], nullptr);
        if (ret != 0) {
            dfscli_log(DFS_LOG_WARN, "pthread_join[%d] err, %s",
                       i, strerror(errno));

            return NGX_ERROR;
        }

        if (rw_ctx->res[i] != NGX_OK) {
            dfscli_log(DFS_LOG_WARN, "write blk %ld to %s err",
                       rw_ctx->blk_id, rw_ctx->dn_ips[i]);

            return NGX_ERROR;
        }

        rw_ctx->write_done_blk_rep++;
    }

    return NGX_OK;
}

//write blk start
// send header to datanode: header.op_type = OP_WRITE_BLOCK
static void *write_blk_start(void *arg) {
    rw_context_t *rw_ctx = (rw_context_t *) arg;
    short dn_index = rw_ctx->dn_index;
    int res = -1;
    conf_server_t *sconf = (conf_server_t *) dfs_cycle->sconf;

    int datafd = open(rw_ctx->src, O_RDONLY);//open source file
    if (datafd < 0) {
        dfscli_log(DFS_LOG_WARN, "open %s err, %s",
                   rw_ctx->src, strerror(errno));

        rw_ctx->res[dn_index] = NGX_ERROR;

        return nullptr;
    }
    // connect to datanode
    int sockfd = dfs_connect(rw_ctx->dn_ips[dn_index], DN_PORT);
    if (sockfd < 0) {
        rw_ctx->res[dn_index] = NGX_ERROR;

        close(datafd);

        return nullptr;
    }
    // stat the file 
    struct stat datastat;
    fstat(datafd, &datastat);
    long fsize = datastat.st_size;
    //long blk_sz = (fsize > sconf->blk_sz) ? sconf->blk_sz : fsize;
    long blk_sz = fsize;

    rw_ctx->fsize = fsize;

    data_transfer_header_t header;
    memset(&header, 0x00, sizeof(data_transfer_header_t));

    header.op_type = OP_WRITE_BLOCK;
    header.namespace_id = rw_ctx->namespace_id;
    header.block_id = rw_ctx->blk_id;
    header.generation_stamp = 0;
    header.start_offset = 0;
    header.len = blk_sz;
    // 发送切片序列给datanode
    header.blk_seq = rw_ctx->blk_seq;
    header.total_blk = rw_ctx->total_blk;
    // 
    res = send(sockfd, &header, sizeof(data_transfer_header_t), 0);
    if (res < 0) {
        dfscli_log(DFS_LOG_WARN, "send header to %s err, %s",
                   rw_ctx->dn_ips[dn_index], strerror(errno));

        rw_ctx->res[dn_index] = NGX_ERROR;

        close(datafd);
        close(sockfd);

        return nullptr;
    }
    //rsp
    data_transfer_header_rsp_t rsp;
    memset(&rsp, 0x00, sizeof(data_transfer_header_rsp_t));
    res = recv(sockfd, &rsp, sizeof(data_transfer_header_rsp_t), 0);
    if (res < 0 || (rsp.op_status != OP_STATUS_SUCCESS && rsp.err != NGX_OK)) {
        dfscli_log(DFS_LOG_WARN, "recv header rsp from %s err, %s",
                   rw_ctx->dn_ips[dn_index], strerror(errno));

        rw_ctx->res[dn_index] = NGX_ERROR;

        close(datafd);
        close(sockfd);

        return nullptr;
    }
    // write blk to datanode
    // transfer file between two sockfd, data fd
    long write_sz = write_blk_to_dn(blk_sz, sockfd, datafd);
    if (write_sz != blk_sz) {
        dfscli_log(DFS_LOG_WARN, "sendfile to %s err, %s",
                   rw_ctx->dn_ips[dn_index], strerror(errno));

        rw_ctx->res[dn_index] = NGX_ERROR;

        close(datafd);
        close(sockfd);

        return nullptr;
    }

    memset(&rsp, 0x00, sizeof(data_transfer_header_rsp_t));
    // recv rsp
    res = recv(sockfd, &rsp, sizeof(data_transfer_header_rsp_t), 0);
    if (res < 0 || (rsp.op_status != OP_STATUS_SUCCESS && rsp.err != NGX_OK)) {
        dfscli_log(DFS_LOG_WARN, "recv write done rsp from %s err, %s",
                   rw_ctx->dn_ips[dn_index], strerror(errno));

        rw_ctx->res[dn_index] = NGX_ERROR;

        close(datafd);
        close(sockfd);

        return nullptr;
    }

    dfscli_log(DFS_LOG_INFO, "put file %s to remote %s succesfully.",
               rw_ctx->src, rw_ctx->dst);

    close(datafd);
    close(sockfd);

    /*
	rw_ctx->done = blk_sz;

	if (fsize > sconf->blk_sz) 
	{
        if (dfs_write_next_blk(rw_ctx) != NGX_OK)
		{
            dfscli_log(DFS_LOG_WARN, "dfs_write_next_blk err");

			rw_ctx->res[dn_index] = NGX_ERROR;

		    return nullptr;
		}
	}
	*/

    rw_ctx->res[dn_index] = NGX_OK;

    return nullptr;
}

// transfer file between two sockfd, data fd
static int write_blk_to_dn(long fsize, int out_fd, int in_fd) // sockfd, data fd
{
    loff_t off = 0;
    long send_sz = 0;
    long done = 0;

    while (fsize > 0) {
        send_sz = sendfile(out_fd, in_fd, &off, fsize);
        //sendfile系统调用在两个文件描述符之间直接传递数据(完全在内核中操作)，
        //从而避免了数据在内核缓冲区和用户缓冲区之间的拷贝，操作效率很高，被称之为零拷贝。
        if (send_sz < 0) {
            if (EAGAIN == errno || ECONNRESET == errno) {
                continue;
            }

            dfscli_log(DFS_LOG_WARN, "sendfile err, %s", strerror(errno));

            return NGX_ERROR;
        }

        fsize -= send_sz;
        done += send_sz;
    }

    return done;
}

static int dfs_write_next_blk(rw_context_t *rw_ctx) {
    if (dfs_get_additional_blk(rw_ctx) != NGX_OK) {
        return NGX_ERROR;
    }

    // connect dn

    // write blk to dn

    return NGX_OK;
}

static int dfs_get_additional_blk(rw_context_t *rw_ctx) {
    create_blk_info_t blk_info;
    create_resp_info_t resp_info;

    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = NN_GET_ADDITIONAL_BLK;
    keyEncode((uchar_t *) rw_ctx->dst, (uchar_t *) out_t.key);

    memset(&blk_info, 0x00, sizeof(create_blk_info_t));
    blk_info.blk_sz = rw_ctx->blk_sz;
    blk_info.blk_rep = rw_ctx->blk_rep;

    out_t.data_len = sizeof(create_blk_info_t);
    out_t.data = &blk_info;

    char sBuf[BUF_SZ] = "";
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    int ws = write(rw_ctx->nn_fd, sBuf, sLen);
    if (ws != sLen) {
        dfscli_log(DFS_LOG_WARN, "write err, ws: %d, sLen: %d", ws, sLen);

        close(rw_ctx->nn_fd);

        return NGX_ERROR;
    }

    int pLen = 0;
    int rLen = recv(rw_ctx->nn_fd, &pLen, sizeof(int), MSG_PEEK);
    if (rLen < 0) {
        dfscli_log(DFS_LOG_WARN, "recv err, rLen: %d", rLen);

        close(rw_ctx->nn_fd);

        return NGX_ERROR;
    }

    char *pNext = (char *) pool_alloc(dfs_cycle->pool, pLen);
    if (!pNext) {
        dfscli_log(DFS_LOG_WARN, "pool_alloc err, pLen: %d", pLen);

        close(rw_ctx->nn_fd);

        return NGX_ERROR;
    }

    rLen = read(rw_ctx->nn_fd, pNext, pLen);
    if (rLen < 0) {
        dfscli_log(DFS_LOG_WARN, "read err, rLen: %d", rLen);

        close(rw_ctx->nn_fd);

        return NGX_ERROR;
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(pNext, rLen, &in_t);

    if (in_t.ret != NGX_OK) {
        dfscli_log(DFS_LOG_WARN, "get_additional_blk err, ret: %d", in_t.ret);

        close(rw_ctx->nn_fd);

        return NGX_ERROR;
    } else if (nullptr != in_t.data && in_t.data_len > 0) {
        memset(&resp_info, 0x00, sizeof(create_resp_info_t));
        memcpy(&resp_info, in_t.data, in_t.data_len);

        rw_ctx->blk_id = resp_info.blk_id;
        rw_ctx->namespace_id = resp_info.namespace_id;
        rw_ctx->dn_num = resp_info.dn_num;
        memcpy(rw_ctx->dn_ips, resp_info.dn_ips, sizeof(resp_info.dn_ips));
    }

    return NGX_OK;
}

