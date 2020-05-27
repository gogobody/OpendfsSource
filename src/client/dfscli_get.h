#ifndef DFS_CLI_GET_H
#define DFS_CLI_GET_H

#include "dfs_types.h"
#include "dfscli_main.h"


// blk num is remote file's total blk num
int dfscli_get(char *src, char *dst, const int *blk_num);

#endif

