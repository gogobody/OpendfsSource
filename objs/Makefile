
#CC =	cc
CC =	g++
#CFLAGS = -g -DHAVE_UTIMES -D_GNU_SOURCE -W -Wall -Wno-unused-function -Wno-unused -Werror -g
#CFLAGS = -gdwarf-2 -std=c++11 -Wall -Werror -Wpointer-arith
CFLAGS = -gdwarf-2 -std=c++11 -Wall -Wpointer-arith -fPIC
CPP =	cc -E
LINK =	$(CC)


ALL_INCS = -I objs \
	-I include \
	-I src/core \
	-I src/common \
	-I src/faio \
	-I src/cfs \
	-I src/namenode \
	-I src/paxos \
	-I src/datanode \
	-I src/client


CORE_DEPS = src/core/dfs_array.h \
	src/core/dfs_buffer.h \
	src/core/dfs_chain.h \
	src/core/dfs_channel.h \
	src/core/dfs_commpool_allocator.h \
	src/core/dfs_commpool.h \
	src/core/dfs_conf_base.h \
	src/core/dfs_conf.h \
	src/core/dfs_conn.h \
	src/core/dfs_conn_listen.h \
	src/core/dfs_conn_pool.h \
	src/core/dfs_epoll.h \
	src/core/dfs_error_log.h \
	src/core/dfs_event.h \
	src/core/dfs_event_timer.h \
	src/core/dfs_half_life_mempool.h \
	src/core/dfs_hashtable.h \
	src/core/dfs_ipc.h \
	src/core/dfs_list.h \
	src/core/dfs_lock.h \
	src/core/dfs_math.h \
	src/core/dfs_mblks.h \
	src/core/dfs_mem_allocator.h \
	src/core/dfs_memory.h \
	src/core/dfs_memory_pool.h \
	src/core/dfs_mempool_allocator.h \
	src/core/dfs_notice.h \
	src/core/dfs_pipe.h \
	src/core/dfs_queue.h \
	src/core/dfs_rbtree.h \
	src/core/dfs_shmem_allocator.h \
	src/core/dfs_shmem.h \
	src/core/dfs_slabs.h \
	src/core/dfs_string.h \
	src/core/dfs_sys.h \
	src/core/dfs_sysio.h \
	src/core/dfs_time.h \
	src/common/dfs_task_cmd.h \
	src/common/dfs_task_codec.h \
	src/common/dfs_task.h \
	src/common/fs_permission.h \
	src/faio/faio_atomic_lock.h \
	src/faio/faio_data_manager.h \
	src/faio/faio_error.h \
	src/faio/faio_handler_manager.h \
	src/faio/faio_manager.h \
	src/faio/faio_notifier_manager.h \
	src/faio/faio_queue.h \
	src/faio/faio_thread.h \
	src/faio/faio_worker_manager.h \
	src/cfs/cfs_ecb.h \
	src/cfs/cfs_eio.h \
	src/cfs/cfs_faio.h \
	src/cfs/cfs_fio.h \
	src/cfs/cfs.h \
	src/cfs/cfs_xthread.h \
	src/namenode/nn_main.h \
	src/namenode/nn_process.h \
	src/namenode/nn_worker_process.h \
	src/namenode/nn_conf.h \
	src/namenode/nn_request.h \
	src/namenode/nn_error_log.h \
	src/namenode/nn_net_response_handler.h \
	src/namenode/nn_task_handler.h \
	src/namenode/nn_task_queue.h \
	src/namenode/nn_time.h \
	src/namenode/nn_conn_event.h \
	src/namenode/nn_cycle.h \
	src/namenode/nn_signal.h \
	src/namenode/nn_thread.h \
	src/namenode/nn_module.h \
	src/namenode/nn_rpc_server.h \
	src/namenode/nn_paxos.h \
	src/namenode/nn_file_index.h \
	src/namenode/nn_dn_index.h \
	src/namenode/nn_blk_index.h \
	src/datanode/dn_main.h \
	src/datanode/dn_cycle.h \
	src/datanode/dn_signal.h \
	src/datanode/dn_module.h \
	src/datanode/dn_process.h \
	src/datanode/dn_conf.h \
	src/datanode/dn_time.h \
	src/datanode/dn_error_log.h \
	src/datanode/dn_thread.h \
	src/datanode/dn_worker_process.h \
	src/datanode/dn_conn_event.h \
	src/datanode/dn_ns_service.h \
	src/datanode/dn_data_storage.h \
	src/datanode/dn_request.h \
	src/client/dfscli_main.h \
	src/client/dfscli_conf.h \
	src/client/dfscli_cycle.h \
	src/client/dfscli_put.h \
	src/client/dfscli_get.h \
	objs/config.h

CORE_INCS = -I objs \
	-I include \
	-I src/core \
	-I src/common \
	-I src/faio \
	-I src/cfs \
	-I src/namenode \
	-I src/paxos \
	-I src/datanode \
	-I src/client


all: objs/namenode      objs/datanode      objs/dfscli



objs/src/core/dfs_array.o: src/core/dfs_array.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_array.o \
		src/core/dfs_array.c


objs/src/core/dfs_buffer.o: src/core/dfs_buffer.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_buffer.o \
		src/core/dfs_buffer.c


objs/src/core/dfs_chain.o: src/core/dfs_chain.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_chain.o \
		src/core/dfs_chain.c


objs/src/core/dfs_channel.o: src/core/dfs_channel.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_channel.o \
		src/core/dfs_channel.c


objs/src/core/dfs_commpool.o: src/core/dfs_commpool.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_commpool.o \
		src/core/dfs_commpool.c


objs/src/core/dfs_commpool_allocator.o: src/core/dfs_commpool_allocator.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_commpool_allocator.o \
		src/core/dfs_commpool_allocator.c


objs/src/core/dfs_conf_base.o: src/core/dfs_conf_base.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_conf_base.o \
		src/core/dfs_conf_base.c


objs/src/core/dfs_conf.o: src/core/dfs_conf.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_conf.o \
		src/core/dfs_conf.c


objs/src/core/dfs_conn.o: src/core/dfs_conn.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_conn.o \
		src/core/dfs_conn.c


objs/src/core/dfs_conn_listen.o: src/core/dfs_conn_listen.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_conn_listen.o \
		src/core/dfs_conn_listen.c


objs/src/core/dfs_conn_pool.o: src/core/dfs_conn_pool.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_conn_pool.o \
		src/core/dfs_conn_pool.c


objs/src/core/dfs_epoll.o: src/core/dfs_epoll.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_epoll.o \
		src/core/dfs_epoll.c


objs/src/core/dfs_error_log.o: src/core/dfs_error_log.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_error_log.o \
		src/core/dfs_error_log.c


objs/src/core/dfs_event.o: src/core/dfs_event.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_event.o \
		src/core/dfs_event.c


objs/src/core/dfs_event_timer.o: src/core/dfs_event_timer.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_event_timer.o \
		src/core/dfs_event_timer.c


objs/src/core/dfs_half_life_mempool.o: src/core/dfs_half_life_mempool.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_half_life_mempool.o \
		src/core/dfs_half_life_mempool.c


objs/src/core/dfs_hashtable.o: src/core/dfs_hashtable.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_hashtable.o \
		src/core/dfs_hashtable.c


objs/src/core/dfs_ipc.o: src/core/dfs_ipc.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_ipc.o \
		src/core/dfs_ipc.c


objs/src/core/dfs_list.o: src/core/dfs_list.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_list.o \
		src/core/dfs_list.c


objs/src/core/dfs_lock.o: src/core/dfs_lock.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_lock.o \
		src/core/dfs_lock.c


objs/src/core/dfs_math.o: src/core/dfs_math.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_math.o \
		src/core/dfs_math.c


objs/src/core/dfs_mblks.o: src/core/dfs_mblks.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_mblks.o \
		src/core/dfs_mblks.c


objs/src/core/dfs_mem_allocator.o: src/core/dfs_mem_allocator.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_mem_allocator.o \
		src/core/dfs_mem_allocator.c


objs/src/core/dfs_memory.o: src/core/dfs_memory.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_memory.o \
		src/core/dfs_memory.c


objs/src/core/dfs_memory_pool.o: src/core/dfs_memory_pool.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_memory_pool.o \
		src/core/dfs_memory_pool.c


objs/src/core/dfs_mempool_allocator.o: src/core/dfs_mempool_allocator.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_mempool_allocator.o \
		src/core/dfs_mempool_allocator.c


objs/src/core/dfs_notice.o: src/core/dfs_notice.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_notice.o \
		src/core/dfs_notice.c


objs/src/core/dfs_pipe.o: src/core/dfs_pipe.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_pipe.o \
		src/core/dfs_pipe.c


objs/src/core/dfs_queue.o: src/core/dfs_queue.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_queue.o \
		src/core/dfs_queue.c


objs/src/core/dfs_rbtree.o: src/core/dfs_rbtree.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_rbtree.o \
		src/core/dfs_rbtree.c


objs/src/core/dfs_shmem_allocator.o: src/core/dfs_shmem_allocator.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_shmem_allocator.o \
		src/core/dfs_shmem_allocator.c


objs/src/core/dfs_shmem.o: src/core/dfs_shmem.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_shmem.o \
		src/core/dfs_shmem.c


objs/src/core/dfs_slabs.o: src/core/dfs_slabs.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_slabs.o \
		src/core/dfs_slabs.c


objs/src/core/dfs_string.o: src/core/dfs_string.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_string.o \
		src/core/dfs_string.c


objs/src/core/dfs_sys.o: src/core/dfs_sys.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_sys.o \
		src/core/dfs_sys.c


objs/src/core/dfs_sysio.o: src/core/dfs_sysio.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_sysio.o \
		src/core/dfs_sysio.c


objs/src/core/dfs_time.o: src/core/dfs_time.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/core/dfs_time.o \
		src/core/dfs_time.c


objs/src/common/dfs_task_codec.o: src/common/dfs_task_codec.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/common/dfs_task_codec.o \
		src/common/dfs_task_codec.c


objs/src/common/dfs_task.o: src/common/dfs_task.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/common/dfs_task.o \
		src/common/dfs_task.c


objs/src/common/fs_permission.o: src/common/fs_permission.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/common/fs_permission.o \
		src/common/fs_permission.c


objs/src/faio/faio_atomic_lock.o: src/faio/faio_atomic_lock.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/faio/faio_atomic_lock.o \
		src/faio/faio_atomic_lock.c


objs/src/faio/faio_data_manager.o: src/faio/faio_data_manager.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/faio/faio_data_manager.o \
		src/faio/faio_data_manager.c


objs/src/faio/faio_error.o: src/faio/faio_error.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/faio/faio_error.o \
		src/faio/faio_error.c


objs/src/faio/faio_handler_manager.o: src/faio/faio_handler_manager.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/faio/faio_handler_manager.o \
		src/faio/faio_handler_manager.c


objs/src/faio/faio_manager.o: src/faio/faio_manager.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/faio/faio_manager.o \
		src/faio/faio_manager.c


objs/src/faio/faio_notifier_manager.o: src/faio/faio_notifier_manager.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/faio/faio_notifier_manager.o \
		src/faio/faio_notifier_manager.c


objs/src/faio/faio_thread.o: src/faio/faio_thread.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/faio/faio_thread.o \
		src/faio/faio_thread.c


objs/src/faio/faio_worker_manager.o: src/faio/faio_worker_manager.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/faio/faio_worker_manager.o \
		src/faio/faio_worker_manager.c


objs/src/cfs/cfs_eio.o: src/cfs/cfs_eio.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/cfs/cfs_eio.o \
		src/cfs/cfs_eio.c


objs/src/cfs/cfs_faio.o: src/cfs/cfs_faio.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/cfs/cfs_faio.o \
		src/cfs/cfs_faio.c


objs/src/cfs/cfs_fio.o: src/cfs/cfs_fio.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/cfs/cfs_fio.o \
		src/cfs/cfs_fio.c


objs/src/cfs/cfs.o: src/cfs/cfs.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/cfs/cfs.o \
		src/cfs/cfs.c


objs/src/paxos/EditlogSM.o: src/paxos/EditlogSM.cpp
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/paxos/EditlogSM.o \
		src/paxos/EditlogSM.cpp


objs/src/paxos/FSEditlog.o: src/paxos/FSEditlog.cpp
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/paxos/FSEditlog.o \
		src/paxos/FSEditlog.cpp


objs/src/paxos/phxeditlog.pb.o: src/paxos/phxeditlog.pb.cpp
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/paxos/phxeditlog.pb.o \
		src/paxos/phxeditlog.pb.cpp


objs/src/namenode/nn_main.o: src/namenode/nn_main.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_main.o \
		src/namenode/nn_main.c


objs/src/namenode/nn_process.o: src/namenode/nn_process.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_process.o \
		src/namenode/nn_process.c


objs/src/namenode/nn_worker_process.o: src/namenode/nn_worker_process.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_worker_process.o \
		src/namenode/nn_worker_process.c


objs/src/namenode/nn_conf.o: src/namenode/nn_conf.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_conf.o \
		src/namenode/nn_conf.c


objs/src/namenode/nn_request.o: src/namenode/nn_request.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_request.o \
		src/namenode/nn_request.c


objs/src/namenode/nn_error_log.o: src/namenode/nn_error_log.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_error_log.o \
		src/namenode/nn_error_log.c


objs/src/namenode/nn_net_response_handler.o: src/namenode/nn_net_response_handler.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_net_response_handler.o \
		src/namenode/nn_net_response_handler.c


objs/src/namenode/nn_task_handler.o: src/namenode/nn_task_handler.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_task_handler.o \
		src/namenode/nn_task_handler.c


objs/src/namenode/nn_task_queue.o: src/namenode/nn_task_queue.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_task_queue.o \
		src/namenode/nn_task_queue.c


objs/src/namenode/nn_time.o: src/namenode/nn_time.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_time.o \
		src/namenode/nn_time.c


objs/src/namenode/nn_conn_event.o: src/namenode/nn_conn_event.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_conn_event.o \
		src/namenode/nn_conn_event.c


objs/src/namenode/nn_cycle.o: src/namenode/nn_cycle.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_cycle.o \
		src/namenode/nn_cycle.c


objs/src/namenode/nn_signal.o: src/namenode/nn_signal.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_signal.o \
		src/namenode/nn_signal.c


objs/src/namenode/nn_thread.o: src/namenode/nn_thread.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_thread.o \
		src/namenode/nn_thread.c


objs/src/namenode/nn_module.o: src/namenode/nn_module.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_module.o \
		src/namenode/nn_module.c


objs/src/namenode/nn_rpc_server.o: src/namenode/nn_rpc_server.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_rpc_server.o \
		src/namenode/nn_rpc_server.c


objs/src/namenode/nn_paxos.o: src/namenode/nn_paxos.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_paxos.o \
		src/namenode/nn_paxos.c


objs/src/namenode/nn_file_index.o: src/namenode/nn_file_index.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_file_index.o \
		src/namenode/nn_file_index.c


objs/src/namenode/nn_dn_index.o: src/namenode/nn_dn_index.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_dn_index.o \
		src/namenode/nn_dn_index.c


objs/src/namenode/nn_blk_index.o: src/namenode/nn_blk_index.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/namenode/nn_blk_index.o \
		src/namenode/nn_blk_index.c


objs/namenode: objs/src/core/dfs_array.o objs/src/core/dfs_buffer.o objs/src/core/dfs_chain.o objs/src/core/dfs_channel.o objs/src/core/dfs_commpool.o objs/src/core/dfs_commpool_allocator.o objs/src/core/dfs_conf_base.o objs/src/core/dfs_conf.o objs/src/core/dfs_conn.o objs/src/core/dfs_conn_listen.o objs/src/core/dfs_conn_pool.o objs/src/core/dfs_epoll.o objs/src/core/dfs_error_log.o objs/src/core/dfs_event.o objs/src/core/dfs_event_timer.o objs/src/core/dfs_half_life_mempool.o objs/src/core/dfs_hashtable.o objs/src/core/dfs_ipc.o objs/src/core/dfs_list.o objs/src/core/dfs_lock.o objs/src/core/dfs_math.o objs/src/core/dfs_mblks.o objs/src/core/dfs_mem_allocator.o objs/src/core/dfs_memory.o objs/src/core/dfs_memory_pool.o objs/src/core/dfs_mempool_allocator.o objs/src/core/dfs_notice.o objs/src/core/dfs_pipe.o objs/src/core/dfs_queue.o objs/src/core/dfs_rbtree.o objs/src/core/dfs_shmem_allocator.o objs/src/core/dfs_shmem.o objs/src/core/dfs_slabs.o objs/src/core/dfs_string.o objs/src/core/dfs_sys.o objs/src/core/dfs_sysio.o objs/src/core/dfs_time.o objs/src/common/dfs_task_codec.o objs/src/common/dfs_task.o objs/src/common/fs_permission.o objs/src/namenode/nn_main.o objs/src/namenode/nn_process.o objs/src/namenode/nn_worker_process.o objs/src/namenode/nn_conf.o objs/src/namenode/nn_request.o objs/src/namenode/nn_error_log.o objs/src/namenode/nn_net_response_handler.o objs/src/namenode/nn_task_handler.o objs/src/namenode/nn_task_queue.o objs/src/namenode/nn_time.o objs/src/namenode/nn_conn_event.o objs/src/namenode/nn_cycle.o objs/src/namenode/nn_signal.o objs/src/namenode/nn_thread.o objs/src/namenode/nn_module.o objs/src/namenode/nn_rpc_server.o objs/src/namenode/nn_paxos.o objs/src/namenode/nn_file_index.o objs/src/namenode/nn_dn_index.o objs/src/namenode/nn_blk_index.o objs/src/paxos/EditlogSM.o objs/src/paxos/FSEditlog.o objs/src/paxos/phxeditlog.pb.o 
	g++ -o objs/namenode $^  -shared -fPIC -o lib/libphxpaxos.a lib/libleveldb.a lib/libprotobuf.a -fPIC -lpthread -lm -std=c++11 


objs/src/datanode/dn_main.o: src/datanode/dn_main.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/datanode/dn_main.o \
		src/datanode/dn_main.c


objs/src/datanode/dn_cycle.o: src/datanode/dn_cycle.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/datanode/dn_cycle.o \
		src/datanode/dn_cycle.c


objs/src/datanode/dn_signal.o: src/datanode/dn_signal.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/datanode/dn_signal.o \
		src/datanode/dn_signal.c


objs/src/datanode/dn_module.o: src/datanode/dn_module.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/datanode/dn_module.o \
		src/datanode/dn_module.c


objs/src/datanode/dn_process.o: src/datanode/dn_process.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/datanode/dn_process.o \
		src/datanode/dn_process.c


objs/src/datanode/dn_conf.o: src/datanode/dn_conf.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/datanode/dn_conf.o \
		src/datanode/dn_conf.c


objs/src/datanode/dn_time.o: src/datanode/dn_time.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/datanode/dn_time.o \
		src/datanode/dn_time.c


objs/src/datanode/dn_error_log.o: src/datanode/dn_error_log.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/datanode/dn_error_log.o \
		src/datanode/dn_error_log.c


objs/src/datanode/dn_thread.o: src/datanode/dn_thread.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/datanode/dn_thread.o \
		src/datanode/dn_thread.c


objs/src/datanode/dn_worker_process.o: src/datanode/dn_worker_process.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/datanode/dn_worker_process.o \
		src/datanode/dn_worker_process.c


objs/src/datanode/dn_conn_event.o: src/datanode/dn_conn_event.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/datanode/dn_conn_event.o \
		src/datanode/dn_conn_event.c


objs/src/datanode/dn_ns_service.o: src/datanode/dn_ns_service.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/datanode/dn_ns_service.o \
		src/datanode/dn_ns_service.c


objs/src/datanode/dn_data_storage.o: src/datanode/dn_data_storage.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/datanode/dn_data_storage.o \
		src/datanode/dn_data_storage.c


objs/src/datanode/dn_request.o: src/datanode/dn_request.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/datanode/dn_request.o \
		src/datanode/dn_request.c


objs/datanode: objs/src/core/dfs_array.o objs/src/core/dfs_buffer.o objs/src/core/dfs_chain.o objs/src/core/dfs_channel.o objs/src/core/dfs_commpool.o objs/src/core/dfs_commpool_allocator.o objs/src/core/dfs_conf_base.o objs/src/core/dfs_conf.o objs/src/core/dfs_conn.o objs/src/core/dfs_conn_listen.o objs/src/core/dfs_conn_pool.o objs/src/core/dfs_epoll.o objs/src/core/dfs_error_log.o objs/src/core/dfs_event.o objs/src/core/dfs_event_timer.o objs/src/core/dfs_half_life_mempool.o objs/src/core/dfs_hashtable.o objs/src/core/dfs_ipc.o objs/src/core/dfs_list.o objs/src/core/dfs_lock.o objs/src/core/dfs_math.o objs/src/core/dfs_mblks.o objs/src/core/dfs_mem_allocator.o objs/src/core/dfs_memory.o objs/src/core/dfs_memory_pool.o objs/src/core/dfs_mempool_allocator.o objs/src/core/dfs_notice.o objs/src/core/dfs_pipe.o objs/src/core/dfs_queue.o objs/src/core/dfs_rbtree.o objs/src/core/dfs_shmem_allocator.o objs/src/core/dfs_shmem.o objs/src/core/dfs_slabs.o objs/src/core/dfs_string.o objs/src/core/dfs_sys.o objs/src/core/dfs_sysio.o objs/src/core/dfs_time.o objs/src/common/dfs_task_codec.o objs/src/common/dfs_task.o objs/src/common/fs_permission.o objs/src/faio/faio_atomic_lock.o objs/src/faio/faio_data_manager.o objs/src/faio/faio_error.o objs/src/faio/faio_handler_manager.o objs/src/faio/faio_manager.o objs/src/faio/faio_notifier_manager.o objs/src/faio/faio_thread.o objs/src/faio/faio_worker_manager.o objs/src/cfs/cfs_eio.o objs/src/cfs/cfs_faio.o objs/src/cfs/cfs_fio.o objs/src/cfs/cfs.o objs/src/datanode/dn_main.o objs/src/datanode/dn_cycle.o objs/src/datanode/dn_signal.o objs/src/datanode/dn_module.o objs/src/datanode/dn_process.o objs/src/datanode/dn_conf.o objs/src/datanode/dn_time.o objs/src/datanode/dn_error_log.o objs/src/datanode/dn_thread.o objs/src/datanode/dn_worker_process.o objs/src/datanode/dn_conn_event.o objs/src/datanode/dn_ns_service.o objs/src/datanode/dn_data_storage.o objs/src/datanode/dn_request.o
	g++ -o objs/datanode $^  -shared -fPIC -o lib/libphxpaxos.a lib/libleveldb.a lib/libprotobuf.a -fPIC -lpthread -lm -std=c++11 


objs/src/client/dfscli_main.o: src/client/dfscli_main.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/client/dfscli_main.o \
		src/client/dfscli_main.c


objs/src/client/dfscli_conf.o: src/client/dfscli_conf.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/client/dfscli_conf.o \
		src/client/dfscli_conf.c


objs/src/client/dfscli_cycle.o: src/client/dfscli_cycle.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/client/dfscli_cycle.o \
		src/client/dfscli_cycle.c


objs/src/client/dfscli_put.o: src/client/dfscli_put.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/client/dfscli_put.o \
		src/client/dfscli_put.c


objs/src/client/dfscli_get.o: src/client/dfscli_get.c
	$(CC) -c $(CFLAGS) $(ALL_INCS) \
		-o objs/src/client/dfscli_get.o \
		src/client/dfscli_get.c


objs/dfscli: objs/src/core/dfs_array.o objs/src/core/dfs_buffer.o objs/src/core/dfs_chain.o objs/src/core/dfs_channel.o objs/src/core/dfs_commpool.o objs/src/core/dfs_commpool_allocator.o objs/src/core/dfs_conf_base.o objs/src/core/dfs_conf.o objs/src/core/dfs_conn.o objs/src/core/dfs_conn_listen.o objs/src/core/dfs_conn_pool.o objs/src/core/dfs_epoll.o objs/src/core/dfs_error_log.o objs/src/core/dfs_event.o objs/src/core/dfs_event_timer.o objs/src/core/dfs_half_life_mempool.o objs/src/core/dfs_hashtable.o objs/src/core/dfs_ipc.o objs/src/core/dfs_list.o objs/src/core/dfs_lock.o objs/src/core/dfs_math.o objs/src/core/dfs_mblks.o objs/src/core/dfs_mem_allocator.o objs/src/core/dfs_memory.o objs/src/core/dfs_memory_pool.o objs/src/core/dfs_mempool_allocator.o objs/src/core/dfs_notice.o objs/src/core/dfs_pipe.o objs/src/core/dfs_queue.o objs/src/core/dfs_rbtree.o objs/src/core/dfs_shmem_allocator.o objs/src/core/dfs_shmem.o objs/src/core/dfs_slabs.o objs/src/core/dfs_string.o objs/src/core/dfs_sys.o objs/src/core/dfs_sysio.o objs/src/core/dfs_time.o objs/src/common/dfs_task_codec.o objs/src/common/dfs_task.o objs/src/common/fs_permission.o objs/src/client/dfscli_main.o objs/src/client/dfscli_conf.o objs/src/client/dfscli_cycle.o objs/src/client/dfscli_put.o objs/src/client/dfscli_get.o
	g++ -o objs/dfscli $^  -shared -fPIC -o lib/libphxpaxos.a lib/libleveldb.a lib/libprotobuf.a -fPIC -lpthread -lm -std=c++11 


install: objs/namenode
	
	test -d '$(DESTDIR)/home/ubuntu/opendfs' || mkdir -p '$(DESTDIR)/home/ubuntu/opendfs'

	test -d '$(DESTDIR)/home/ubuntu/opendfs/temp' || mkdir -p '$(DESTDIR)/home/ubuntu/opendfs/temp'

	test -d '$(DESTDIR)/home/ubuntu/opendfs/share' || mkdir -p '$(DESTDIR)/home/ubuntu/opendfs/share'

	test -d '$(DESTDIR)/home/ubuntu/opendfs/sbin' || mkdir -p '$(DESTDIR)/home/ubuntu/opendfs/sbin'

	test -d '$(DESTDIR)/home/ubuntu/opendfs/bin' || mkdir -p '$(DESTDIR)/home/ubuntu/opendfs/bin'

	test -d '$(DESTDIR)/home/ubuntu/opendfs/logs' || mkdir -p '$(DESTDIR)/home/ubuntu/opendfs/logs'

	test -d '$(DESTDIR)/home/ubuntu/opendfs/etc' || mkdir -p '$(DESTDIR)/home/ubuntu/opendfs/etc'

	touch '$(DESTDIR)/home/ubuntu/opendfs/logs/namenode.pid'

	
	test -f '$(DESTDIR)/home/ubuntu/opendfs/etc/namenode.conf' || cp  etc/namenode.conf '$(DESTDIR)/home/ubuntu/opendfs/etc'
	
	test -f '$(DESTDIR)/home/ubuntu/opendfs/etc/namenode.conf.default' || 	    cp  etc/namenode.conf.default '$(DESTDIR)/home/ubuntu/opendfs/etc'

	test ! -f '$(DESTDIR)/home/ubuntu/opendfs/sbin/namenode' || mv '$(DESTDIR)/home/ubuntu/opendfs/sbin/namenode' 			'$(DESTDIR)/home/ubuntu/opendfs/sbin/namenode.old'
			
	cp  objs/namenode '$(DESTDIR)/home/ubuntu/opendfs/sbin'

	test ! -f '$(DESTDIR)/home/ubuntu/opendfs/sbin/datanode' || mv '$(DESTDIR)/home/ubuntu/opendfs/sbin/datanode' 			'$(DESTDIR)/home/ubuntu/opendfs/sbin/datanode.old'
			
	cp  objs/datanode '$(DESTDIR)/home/ubuntu/opendfs/sbin'

	test ! -f '$(DESTDIR)/home/ubuntu/opendfs/sbin/dfscli' || mv '$(DESTDIR)/home/ubuntu/opendfs/sbin/dfscli' 			'$(DESTDIR)/home/ubuntu/opendfs/sbin/dfscli.old'
			
	cp  objs/dfscli '$(DESTDIR)/home/ubuntu/opendfs/sbin'

