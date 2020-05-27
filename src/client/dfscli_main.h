#ifndef DFS_CLI_MAIN_H
#define DFS_CLI_MAIN_H

#include "dfs_types.h"
#include "dfs_task.h"

/**
 * ANSI Colors
 */
#define ANSI_BOLD     "\033[1m"
#define ANSI_CYAN     "\033[36m"
#define ANSI_MAGENTA  "\033[35m"
#define ANSI_RED      "\033[31m"
#define ANSI_YELLOW   "\033[33m"
#define ANSI_BLUE     "\033[34m"
#define ANSI_GREEN    "\033[32m"
#define ANSI_WHITE    "\033[37m"
#define ANSI_RESET    "\033[0m"

#define PATH_LEN    256
#define KEY_LEN     256
#define OWNER_LEN   16
#define GROUP_LEN   16
#define LOGMSG_LEN  4096
#define BUF_SZ      4096

#define DN_PORT 8100 // set default datanode port is 8100

#define MIN(a, b) ((a) < (b) ? (a) : (b))

typedef struct rw_context_s //读写环境变量
{
    char      src[PATH_LEN];//源目录
	char      dst[PATH_LEN];//目标
	int       nn_fd; //namenode fd
	int       dn_fd[3];//datanode fd
    uint64_t  blk_sz;
	short     blk_rep;
	uint64_t  blk_id;
	uint64_t  namespace_id;
	short     dn_num; //data node number?
	short     dn_index;
	char      dn_ips[3][32]; //data node ips
	pthread_t thread_id[3];
	int       res[3];
	uint64_t  fsize;
	short     write_done_blk_rep;
	// add blk here
	int       blk_seq; // 当前序列
	int       total_blk; // 总的序列
} rw_context_t;

int dfscli_daemon();
int dfs_connect(char* ip, int port); //connect to ip port
void keyEncode(uchar_t *path, uchar_t *key);//encode with base64
void keyDecode(uchar_t *key, uchar_t *path);
void getUserInfo(task_t *out_t);
void dfscli_log(int level, const char *fmt, ...);

#endif

