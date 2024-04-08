#include "handoff.h"

int num_peers = 0;

void handle_new_handoff_in(){}

// migration request from other nodes
void handle_handoff_in_recv(){}

// response to migration request from other nodes
void handle_handoff_in_send(){}

// // if we don't have a connection yet before send migration request to other node, need to create new connection
void create_new_handoff_out(int epoll_fd, int *out_fd, int *fds_not_connected, int peer_id) {
}
// 	*out_fd = socket(AF_INET, SOCK_STREAM, 0);
// 	if (*out_fd == -1) {
// 		perror("socket");
// 		exit(EXIT_FAILURE);
// 	}

// 	set_socket_non_blocking(*out_fd);

// 	if (connect(*out_fd, (struct sockaddr*)&peer_addrs[peer_id], sizeof(peer_addrs[peer_id])) == -1) {
// 		if (errno != EINPROGRESS) {
// 			perror("connect");
// 			close(*out_fd);
// 			exit(EXIT_FAILURE);
// 		} else {
// 			struct epoll_event event = {0};
// 			event.data.fd = *out_fd;
// 			event.data.u32 = HANDOFF_OUT_EVENT;
// 			event.events = EPOLLOUT;
// 			if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, *out_fd, &event) == -1) {
// 				perror("epoll_ctl");
// 				close(*out_fd);
// 				exit(EXIT_FAILURE);
// 			}
// 		}
// 	} else {
// 		(*fds_not_connected)--;
// 		printf("%s: Connected to peer %d, fds_not_connected %d\n", __func__, peer_id, *fds_not_connected);
// 	}
// }

// if new handoff_out connection is not connected immediately, it is put in epoll and we check when events comes,
// if fail, we issue a connect again
void handle_new_handoff_out(int epoll_fd, int event_fd, int out_fds[num_peers], int *fds_not_connected, int peer_id) {
}
// 	int val;
// 	socklen_t val_slen = sizeof(val);
// 	if (getsockopt(event_fd, SOL_SOCKET, SO_ERROR, &val, &val_slen) < 0) {
// 		perror("getsockopt");
// 		close(event_fd);
// 	} else if (val != 0) {
// 		fprintf(stderr, "Connection failed: %s\n", strerror(val));
// 		close(event_fd);
// 	} else {
// 		printf("fds_not_connected %d\n", *fds_not_connected);
// 		(*fds_not_connected)--;
// 		printf("%s: Connected to peer %d, fds_not_connected %d\n", __func__, peer_id, *fds_not_connected);
// 		return;
// 	}

// 	// Sleep before retry
// 	// sleep(1);

// 	if (peer_id != -1) {
// 		int i;
// 		for (i = 0; i < num_peers; i++)
// 		{
// 			if (event_fd == out_fds[i])
// 				peer_id = i;
// 				break;
// 		}
// 		if (i >= num_peers) {
// 			printf("Unkown can-not-connect server conn %d\n", event_fd);
// 			return;
// 		}
// 	}

// 	out_fds[peer_id] = 0;
// 	create_new_handoff_out(epoll_fd, &out_fds[peer_id], fds_not_connected, peer_id);
// }

// send migration request to other nodes
void handle_handoff_out_send(){}

// receive response of migration request this node sent
void handle_handoff_out_recv(){}
