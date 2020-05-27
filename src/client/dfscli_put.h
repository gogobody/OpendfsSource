#ifndef DFS_CLI_PUT_H
#define DFS_CLI_PUT_H

#include "dfs_types.h"
#include "dfscli_main.h"

// if u only send one file ,then blk_seq and total total blk should be 1
int dfscli_put(char *src, char *dst, int blk_seq, int total_blk);

#endif

