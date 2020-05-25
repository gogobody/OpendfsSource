#include "nn_error_log.h"
#include "nn_conf.h"
#include "dfs_ipc.h"
#include "nn_time.h"

int nn_error_log_init(cycle_t *cycle) {
    log_t *slog = nullptr;
    conf_server_t *sconf = nullptr;

    errno = 0;

    sconf = (conf_server_t *) cycle->sconf;
    slog = cycle->error_log;

    slog->file->name = sconf->error_log;
    slog->log_level = sconf->log_level;

    error_log_init(slog, (log_time_ptr) time_logstr, nullptr);

    return NGX_OK;
}

int nn_error_log_release(cycle_t *cycle) {
    return error_log_release(cycle->error_log);
}

void nn_log_paxos(const int level, const char *fmt...) {
//    va_list ap;
//    char strings[1280];
//    va_start(ap, fmt);
//    vsprintf(strings, fmt, ap);
////    uart1_sendstring(string);
//    va_end(ap);
//    printf("## %s \n",strings);


//    error_log_core(dfs_cycle->error_log, level, (char *)__FILE__, __LINE__, 0, fmt, args);
}
