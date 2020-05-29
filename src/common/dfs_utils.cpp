//
// Created by ginux on 2020/5/28.
//


#include "dfs_utils.h"

int parseIpPort(char * ipstring, int *ip, int *port){
    int count = sscanf(ipstring, "%[^':']:%d",
                       ip,
                       port);
    if (count != 2)
    {
        return NGX_ERROR;
    }
    return NGX_OK;
}