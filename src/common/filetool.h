
#ifndef NGXFS_FILETOOL_H
#define NGXFS_FILETOOL_H

#define _CRT_SECURE_NO_WARNINGS
#include<cstdio>
#include<cstdlib>
#include<cstring>

int splitFile(const char* path, int count,  const char* savename);

int mergeFile(const char* savename, int count, const char* bigfile);

#endif //NGXFS_FILETOOL_H
