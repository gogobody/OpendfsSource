#!/bin/bash
# test in arch linux only
# plz do not change this
set -e
set -o pipefail

echo "now your home dir is: ${HOME}"
#echo "${PWD}"
#defaultDIR="${PWD}/default"
#configDIR="${PWD}/etc"

PREFIX=$(sed '/^#define PREFIX/!d;s/.*  //' ./etc/config.h | sed 's/\"//g')
echo "PREFIX: ${PREFIX}"

#
REAL_DFS_HOME=${HOME}/opendfs
echo "REAL_DFS_HOME: ${REAL_DFS_HOME}"

# in this step ,we need fix home dir
sed -i "s:${PREFIX}:${REAL_DFS_HOME}:" ./etc/datanode.conf
sed -i "s:${PREFIX}:${REAL_DFS_HOME}:" ./etc/namenode.conf
sed -i "s:${PREFIX}:${REAL_DFS_HOME}:" ./etc/config.h

# end
mkdir -p "${PREFIX}"
echo "now your config and disk dir:${PREFIX} , and config file will be copy to this dir"
cp -rf ./etc "${PREFIX}"

if [ -z "$PREFIX" ]; then
    echo "PREFIX is empty, exit after 5s"
    sleep 5
    exit
fi

if [ -n "$PREFIX" ]; then
    echo "" # continue
fi


# datanode
echo "now start to mk datanode dir"
dn_data_dir=$(awk -F '"' '//{a=1}a==1&&$1~/data_dir/{print $2;exit}' ./etc/datanode.conf)
echo "tip: your PREFIX in config.h should be set and should be same with prefix in the conf file"

if [ -z "$dn_data_dir" ]; then
    echo "error: data_dir is empty, exit after 5s"
    sleep 5
    exit
fi

if [ -n "$dn_data_dir" ]; then
    echo "" # continue

fi

if [[ $dn_data_dir =~ $PREFIX ]]
then
    echo "now start to mkdir ${dn_data_dir}"
    mkdir -p "${dn_data_dir}"
else
    echo "error: your PREFIX not same whit the conf, PREFIX is ${PREFIX} while data_dir is ${dn_data_dir}"
    echo "exit after 5s"
    sleep 5
    exit 1
fi

dn_error_log=$( awk -F '"' '//{a=1}a==1&&$1~/error_log/{print $2;exit}' ./etc/datanode.conf)
echo "mkdir ${dn_error_log%\/*}"
mkdir -p "${dn_error_log%\/*}"

dn_pid_file=$( awk -F '"' '//{a=1}a==1&&$1~/pid_file/{print $2;exit}' ./etc/datanode.conf)
echo "mkdir ${dn_pid_file%\/*}"
mkdir -p "${dn_pid_file%\/*}"

dn_coredump_dir=$(awk -F '"' '//{a=1}a==1&&$1~/coredump_dir/{print $2;exit}' ./etc/datanode.conf)
echo "mkdir ${dn_coredump_dir}"
mkdir -p "${dn_coredump_dir}"

#if [ ! -d testgrid  ];then
#  mkdir testgrid
#else
#  echo dir exist
#fi


# namenode
echo "now start to mk namenode dir"
nn_editlog_dir=$(awk -F '"' '//{a=1}a==1&&$1~/editlog_dir/{print $2;exit}' ./etc/namenode.conf)
if [[ $nn_editlog_dir =~ $PREFIX ]]
then
    echo "now start to mkdir ${nn_editlog_dir}"
    mkdir -p "${nn_editlog_dir}"
else
    echo "error: your PREFIX not same whit the conf, PREFIX is ${PREFIX} while data_dir is ${nn_editlog_dir}"
    echo "exit after 5s"

    sleep 5
    exit 1
fi

nn_fsimage_dir=$( awk -F '"' '//{a=1}a==1&&$1~/fsimage_dir/{print $2;exit}' ./etc/namenode.conf)
echo "mkdir ${nn_fsimage_dir}"
mkdir -p "${nn_fsimage_dir}"

nn_error_log=$( awk -F '"' '//{a=1}a==1&&$1~/error_log/{print $2;exit}' ./etc/namenode.conf)
echo "mkdir ${nn_error_log%\/*}"
mkdir -p "${nn_error_log%\/*}"

nn_pid_file=$( awk -F '"' '//{a=1}a==1&&$1~/pid_file/{print $2;exit}' ./etc/namenode.conf)
echo "mkdir ${nn_pid_file%\/*}"
mkdir -p "${nn_pid_file%\/*}"

nn_coredump_dir=$(awk -F '"' '//{a=1}a==1&&$1~/coredump_dir/{print $2;exit}' ./etc/namenode.conf)
echo "mkdir ${nn_coredump_dir}"
mkdir -p "${nn_coredump_dir}"

echo "successful, you have create all dir"

exit 0
