#ifndef __HANDOFF_H__
#define __HANDOFF_H__

#include "http_client.h"

#define DATA 1
#define CONTROL 2
#define HANDOFF_MSG 3
#define READY_MSG 4
#define END_MSG 5

#ifndef TCPOPT_MSS
#define TCPOPT_MSS 2
#endif

#ifndef TCPOPT_WINDOW
#define TCPOPT_WINDOW 3
#endif

#ifndef TCPOPT_SACK_PERM
#define TCPOPT_SACK_PERM 4
#endif

#ifndef TCPOPT_TIMESTAMP
#define TCPOPT_TIMESTAMP 8
#endif

#define MAX_NUM_PEERS 1024

extern int num_peers;

// TODO
struct handoff_in {
	uint32_t epoll_data_u32;
	int epoll_fd;
	int fd;
        int osd_arr_index;
};

struct handoff_out_req;

struct handoff_out_queue {
    struct handoff_out_req* front;
    struct handoff_out_req* rear;
};

// TODO
struct handoff_out {
	uint32_t epoll_data_u32;
	int epoll_fd;
	int fd;
        bool is_fd_in_epoll;
        int osd_arr_index;
        struct handoff_out_queue *queue;
        // handoff out request currently sending out, deququed from queue
        struct http_client *client;
        // receive buffer???
        // uint8_t recv_buf[1024];
};

void handoff_out_connect(struct handoff_out *out_ctx);
void handoff_out_reconnect(struct handoff_out *out_ctx);
void handoff_out_issue(int epoll_fd, uint32_t epoll_data_u32, struct http_client *client,
	struct handoff_out *out_ctx, int osd_arr_index);
void handoff_out_send(struct handoff_out *out_ctx);
void handoff_out_recv(struct handoff_out *out_ctx);

#endif
