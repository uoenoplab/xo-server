#ifndef __HANDOFF_H__
#define __HANDOFF_H__

extern int num_peers;

// TODO
struct handoff_in {
	int epoll_fd;
	int fd;
};

// TODO
struct handoff_out {
	int epoll_fd;
	int fd;
};

void handle_new_handoff_in();
void handle_handoff_in_send();
void create_new_handoff_out(int epoll_fd, int *out_fd, int *fds_not_connected, int peer_id);
void handle_new_handoff_out(int epoll_fd, int event_fd, int out_fds[num_peers], int *fds_not_connected, int peer_id);
void handle_handoff_out_recv();
void handle_handoff_out_send();

#endif
