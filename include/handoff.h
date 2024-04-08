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
};

// TODO
struct handoff_out {
	uint32_t epoll_data_u32;
	int epoll_fd;
	int fd;
};

struct tcp_info_sub {
        uint8_t tcpi_state;
        uint8_t tcpi_ca_state;
        uint8_t tcpi_retransmits;
        uint8_t tcpi_probes;
        uint8_t tcpi_backoff;
        uint8_t tcpi_options;
        uint8_t tcpi_snd_wscale : 4;
        uint8_t tcpi_rcv_wscale : 4;
};

void handle_new_handoff_in();
void handle_handoff_in_send();
void create_new_handoff_out(int epoll_fd, int *out_fd, int *fds_not_connected, int peer_id);
void handle_new_handoff_out(int epoll_fd, int event_fd, int *out_fds, int peer_count, int *fds_not_connected, int peer_id);
void handle_handoff_out_recv(int epoll_fd, uint8_t *handoff_msg, size_t handoff_msg_len, struct http_client *client);
void handle_handoff_out_send(struct http_client *client, int peer_osd_id);

#endif
