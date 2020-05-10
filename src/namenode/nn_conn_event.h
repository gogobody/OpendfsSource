#ifndef NN_CONN_EVENT_H
#define NN_CONN_EVENT_H

#include "nn_cycle.h"
#include "nn_thread.h"
static void listen_rev_handler(event_t *ev);

int nn_conn_listening_init(cycle_t *cycle);

#endif

