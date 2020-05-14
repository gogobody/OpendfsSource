#  
namenode :

listening_rev_handler

-> add epoll event

-> call event and call `nn_event_process_handler`

-> `nn_event_process_handler` will call `mc->read/write_event_handler`

-> call `nn_conn_read_handler` and `nn_conn_recv`  rev buf , then call `nn_conn_decode` 
 to decode buf to task
 
 -> call `dispatch_task` to dispatch task to task_threads[hash] (TASK_THREAD)
 ，push task to the task thread->tq and `notice_wake_up`
