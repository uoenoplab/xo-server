#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <signal.h>
#include <sys/epoll.h>
#include <errno.h>
#include <sys/uio.h>
#include <pthread.h>
#include <assert.h>
#include <netinet/in.h>
#include <net/if.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <ini.h>

#include "http_client.h"
#include "tls.h"
#include "osd_mapping.h"

#define S3_HTTP_PORT 8080
#define HANDOFF_CTRL_PORT 8081
#define MAX_EVENTS 1000

// 0.  handle incoming conn
// 1.  read
// 2.  check if we are reading from an ongoing HTTP message
// 2.1 create new entry
// 2.2 start populating http req structure
// 3.  check end of header. check if we need to respond right away
// 3.1 metadata req
// 3.1.1 talk to rados and reply
// 3.2 data put
// 3.2.1 setup asyn io context
// 3.3 data get
// 3.3.1 setup asyn io context
// 4.  read payload run io op with io context
// 5.  check end of payload
// 5.1 wait for async completion, respond http OK

const size_t BUF_SIZE = sizeof(char) * 1024 * 1024 * 4;
const int enable = 1;

volatile sig_atomic_t server_running = 1;

char *osd_addr_strs[MAX_OSDS] = { NULL };
struct sockaddr_in osd_addrs[MAX_OSDS];
int osd_ids[MAX_OSDS] = { 0 };
int num_osds = 0;
int num_peers = 0;

struct thread_param {
	int thread_id;
	// int server_fd;
	rados_t *cluster;
	int handoff_accept_eventfd;
};

void split_uint64_to_ints(uint64_t value, int *high, int *low) {
	*high = (int)(value >> 32);
	*low = (int)(value & 0xFFFFFFFF);
}

uint64_t combine_ints_to_uint64(int high, int low) {
	return ((uint64_t)high << 32) | (uint32_t)low;
}

void handleCtrlC(int signum)
{
	printf("Received Ctrl+C. Stopping the server thread [%d]...\n", gettid());
	server_running = 0; // Set the flag to stop the server gracefully.
}

void set_socket_non_blocking(int socket_fd)
{
	int flags = fcntl(socket_fd, F_GETFL, 0);
	if (flags == -1) {
		perror("fcntl");
		exit(EXIT_FAILURE);
	}

	flags |= O_NONBLOCK;
	if (fcntl(socket_fd, F_SETFL, flags) == -1) {
		perror("fcntl");
		exit(EXIT_FAILURE);
	}
}

void handle_new_connection(int epoll_fd, int server_fd, int thread_id)
{
	pid_t tid = gettid();

	int new_socket;
	struct sockaddr_in client_addr;
	socklen_t client_len = sizeof(client_addr);

	// Accept a new client connection
	new_socket = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
	if (new_socket == -1) {
		perror("accept");
		return;
	}

	printf("Thread %d: Accepted connection (%d) from %s:%d\n", thread_id, new_socket, inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

	// Add the new client socket to the epoll event list

	struct epoll_event event;
	event.events = EPOLLIN;
	struct http_client *client = create_http_client(epoll_fd, new_socket);
	event.data.ptr = client;

	set_socket_non_blocking(new_socket);

	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, new_socket, &event) == -1) {
		perror("epoll_ctl");
		close(new_socket);
		free_http_client(client);
	}
}

void handle_client_disconnect(int epoll_fd, struct http_client *client)
{
	int fd = client->fd;
	// Remove the client socket from the epoll event list
	if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL) == -1) {
		perror("epoll_ctl");
	}
	close(fd);
	tls_free_client(client);
	free_http_client(client);
}

static void do_llhttp_execute(struct http_client *client, char *client_data_buffer, ssize_t bytes_received)
{
	enum llhttp_errno ret;

	// Echo the received data back to the client
	ret = llhttp_execute(&(client->parser), client_data_buffer, bytes_received);
	if (ret != HPE_OK) {
		fprintf(stderr, "Parse error: %s %s\n", llhttp_errno_name(ret), client->parser.reason);
		fprintf(stderr, "buf: %s\n", client_data_buffer);
		fprintf(stderr, "Error pos: %ld %ld %p %p\n", bytes_received, llhttp_get_error_pos(&(client->parser)) - client_data_buffer, client_data_buffer, llhttp_get_error_pos(&(client->parser)));
		//handle_client_disconnect(epoll_fd, client); // Handle client disconnection
		exit(1);
		//close(client->fd);
		//free_http_client(client);
	}
}

// return number of bytes left in client_data_buffer after this function
// on error return -1
static ssize_t handle_client_data_ssl(struct http_client *client, SSL_CTX *ssl_ctx,
	char *client_data_buffer, ssize_t bytes_received)
{
	// printf("%s: bytes_received %d\n", __func__, bytes_received);

	// HTTP or HTTPS not decided and SSL not initlized for this conn
	if (client->tls.ssl == NULL) {
		if (tls_init_client(ssl_ctx, client, client_data_buffer, bytes_received) == -1) {
			perror("tls_conn_init");
			return -1;
		}

		// Not enough bytes to identity HTTP or HTTPS
		if (client->tls.is_ssl && client->tls.ssl == NULL) {
			return 0;
		}

		// Not SSL, use HTTP, need to feed bytes to llhttp from preivous recv first
		if (!client->tls.is_ssl) {
			if (client->tls.client_hello_check_off > 0) {
				do_llhttp_execute(client, client->tls.client_hello_check_buf,
					client->tls.client_hello_check_off);
			}
			return bytes_received;
		}
	}

	if (client->tls.is_ktls_set) {
		// do nothing
		return bytes_received;
	}

	if (!client->tls.is_handshake_done) {
		return tls_handle_handshake(client, client_data_buffer, bytes_received);
	}

	fprintf(stderr, "%s: error state as handshake is done but ktls not set\n", __func__);
	return -1; // Shouldn't happen
	// Handle handshake?
}

void handle_client_data(int epoll_fd, struct http_client *client,
	char *client_data_buffer, int thread_id, rados_ioctx_t *bucket_io_ctx,
	rados_ioctx_t *data_io_ctx, SSL_CTX *ssl_ctx)
{
	ssize_t bytes_received;

	memset(client_data_buffer, 0, BUF_SIZE);
	bytes_received = recv(client->fd, client_data_buffer, BUF_SIZE, 0);
	if (bytes_received <= 0) {
		// Client closed the connection or an error occurred
		if (bytes_received == 0) {
			printf("Thread %d: Client disconnected: %d\n", thread_id, client->fd);
		} else if (errno == EAGAIN && client->tls.is_ssl && client->tls.is_ktls_set) {
			printf("recv returned EAGAIN (client->tls.ssl %p)\n", client->tls.ssl);
			return;
		} else {
			perror("recv");
		}

		// Remove the client socket from the epoll event list
		handle_client_disconnect(epoll_fd, client); // Handle client disconnection
		return;
	}

	// printf("%s: bytes_received %d\n", __func__, bytes_received);

	if (client->tls.is_ssl){
		bytes_received = handle_client_data_ssl(client, ssl_ctx, client_data_buffer, bytes_received);
		if (bytes_received == -1) {
			fprintf(stderr, "%s: handle_client_data_ssl returned %ld\n", __func__, bytes_received);
			exit(EXIT_FAILURE);
		}
		if (bytes_received == 0) return;
	}

	//printf("%.*s", (int)bytes_received, client_data_buffer);

	client->bucket_io_ctx = bucket_io_ctx;
	client->data_io_ctx = data_io_ctx;
	do_llhttp_execute(client, client_data_buffer, bytes_received);
}

// TODO
struct handoff_in {
	int epoll_fd;
	int fd;
};

// TODO
struct handoff_out {
	int epoll_fd;
	int fd;
}

void handle_new_handoff_in(){}

// migration request from other nodes
void handle_handoff_in_recv(){}

// response to migration request from other nodes
void handle_handoff_in_send(){}

// if we don't have a connection yet before send migration request to other node, need to create new connection
void create_new_handoff_out(int epoll_fd, int *out_fd, int *fds_not_connected, int peer_id) {
	*out_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (*out_fd == -1) {
		perror("socket");
		exit(EXIT_FAILURE);
	}

	set_socket_non_blocking(*out_fd);

	if (connect(*out_fd, (struct sockaddr*)&peer_addrs[peer_id], sizeof(peer_addrs[peer_id])) == -1) {
		if (errno != EINPROGRESS) {
			perror("connect");
			close(*out_fd);
			exit(EXIT_FAILURE);
		} else {
			struct epoll_event event = {0};
			event.data.fd = *out_fd;
			event.events = EPOLLOUT;
			if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, *out_fd, &event) == -1) {
				perror("epoll_ctl");
				close(*out_fd);
				exit(EXIT_FAILURE);
			}
		}
	} else {
		(*fds_not_connected)--;
		printf("%s: Connected to peer %d, fds_not_connected %d\n", __func__, peer_id, *fds_not_connected);
	}
}

// if new handoff_out connection is not connected immediately, it is put in epoll and we check when events comes,
// if fail, we issue a connect again
void handle_new_handoff_out(int epoll_fd, int event_fd, int out_fds[num_peers], int *fds_not_connected, int peer_id) {
	int val;
	socklen_t val_slen = sizeof(val);
	if (getsockopt(event_fd, SOL_SOCKET, SO_ERROR, &val, &val_slen) < 0) {
		perror("getsockopt");
		close(event_fd);
	} else if (val != 0) {
		fprintf(stderr, "Connection failed: %s\n", strerror(val));
		close(event_fd);
	} else {
		printf("fds_not_connected %d\n", *fds_not_connected);
		(*fds_not_connected)--;
		printf("%s: Connected to peer %d, fds_not_connected %d\n", __func__, peer_id, *fds_not_connected);
		return;
	}

	// Sleep before retry
	// sleep(1);

	if (peer_id != -1) {
		int i;
		for (i = 0; i < num_peers; i++)
		{
			if (event_fd == out_fds[i])
				peer_id = i;
				break;
		}
		if (i >= num_peers) {
			printf("Unkown can-not-connect server conn %d\n", event_fd);
			return;
		}
	}

	out_fds[peer_id] = 0;
	connect_to_peer(epoll_fd, &out_fds[peer_id], fds_not_connected, peer_id);
}

// send migration request to other nodes
void handle_handoff_out_send(){}

// receive response of migration request this node sent
void handle_handoff_out_recv(){}

static void *conn_wait(void *arg)
{
	int handoff_in_fds[num_peers] = { 0 };
	int handoff_out_fds[num_peers] = { 0 };
	int handoff_out_fds_not_connected = num_peers;

	rados_ioctx_t bucket_io_ctx;
	rados_ioctx_t data_io_ctx;

	struct thread_param *param = (struct thread_param*)arg;
	//int server_fd = param->server_fd;
	int server_fd = -1;
	int thread_id = param->thread_id;
	rados_t *cluster = param->cluster;

	char *client_data_buffer = malloc(BUF_SIZE);

	int epoll_fd, event_count;
	struct epoll_event event;
	struct epoll_event *events = (struct epoll_event*)malloc(sizeof(struct epoll_event) * MAX_EVENTS);
	assert(events != NULL);

	SSL_CTX *ssl_ctx = tls_init_ctx("./assets/server.crt", "./assets/server.key");
	if (ssl_ctx == NULL) {
		perror("tls_init_ctx");
		exit(EXIT_FAILURE);
	}

	int err = rados_ioctx_create(*cluster, BUCKET_POOL, &bucket_io_ctx);
	if (err < 0) {
		fprintf(stderr, "cannot open rados pool %s: %s\n", BUCKET_POOL, strerror(-err));
		rados_shutdown(cluster);
		exit(1);
	}

	err = rados_ioctx_create(*cluster, DATA_POOL, &data_io_ctx);
	if (err < 0) {
		fprintf(stderr, "cannot open rados pool %s: %s\n", DATA_POOL, strerror(-err));
		rados_shutdown(cluster);
		exit(1);
	}

	// Create an epoll instance
	if ((epoll_fd = epoll_create1(0)) == -1) {
		perror("epoll_create1");
		exit(EXIT_FAILURE);
	}

	// Add handoff_in eventfd into epoll
	memset(&event, 0 , sizeof(event));
	event.data.fd = targ->efd;
	printf("thread %d eventfd %d\n", targ->id, targ->efd);
	event.events = EPOLLIN;
	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, targ->efd, &event) == -1) {
		perror("epoll_ctl: efd");
		exit(EXIT_FAILURE);
	}

	// Create a TCP socket
	if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror("socket");
		exit(EXIT_FAILURE);
	}

	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0)
		perror("setsockopt(SO_REUSEADDR) failed");

	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &enable, sizeof(int)) < 0)
		perror("setsockopt(SO_REUSEPORT) failed");

	// Initialize server address structure
	struct sockaddr_in server_addr;
	memset(&server_addr, 0, sizeof(server_addr));
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = INADDR_ANY;
	server_addr.sin_port = htons(S3_HTTP_PORT);

	// Bind the socket to the server address
	if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
		perror("bind");
		exit(EXIT_FAILURE);
	}

	// Listen for incoming connections
	if (listen(server_fd, SOMAXCONN) == -1) {
		perror("listen");
		exit(EXIT_FAILURE);
	}

	// Add the server socket to the epoll event list
	memset(&event, 0 , sizeof(event));
	event.events = EPOLLIN;
	struct http_client *server_client = create_http_client(server_fd, server_fd);
	event.data.ptr = server_client;

	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event) == -1) {
		perror("epoll_ctl");
		exit(EXIT_FAILURE);
	}

	while (server_running) {
		// Wait for events using epoll
		//memset(events, 0, sizeof(struct epoll_event) * MAX_EVENTS);
		event_count = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
		if (event_count == -1) {
			if (errno == EINTR) {
				//printf("EINTR\n");
				continue;
			}
			else {
				perror("epoll_wait");
				exit(EXIT_FAILURE);
			}
		}

		for (int i = 0; i < event_count; i++) {
			if (events[i].data.ptr) {
				// Handle events using callback functions
				struct http_client *c = (struct http_client *)events[i].data.ptr;
				if (c->fd == server_fd) {
					handle_new_connection(epoll_fd, server_fd, thread_id);
				}
				else if (events[i].events & EPOLLOUT) {
					send_client_data(c);
				}
				else if (events[i].events & EPOLLIN) {
					handle_client_data(epoll_fd, c, client_data_buffer, thread_id, &bucket_io_ctx, &data_io_ctx, ssl_ctx);
				}
			} else {
				if (events[i].data.fd == param->handoff_accept_eventfd) {
					uint64_t val;
					int in_fd, peer_id;
					read(param->handoff_accept_eventfd, &val, sizeof(val));
					split_uint64_to_ints(val, &in_fd, &peer_id);
					printf("Thread %d received an new handoff client conn %d from peer %d\n",
						param->thread_id, in_fd, peer_id);
					if (handoff_in_fds[peer_id] != 0) {
						// main thread will close old fd and cause global epoll list delete
						fprintf(stderr, "Thread %d overwrite old client conn %d from peer %d\n",
							param->thread_id, in_fd, peer_id);
					}
					handoff_in_fds[peer_id] = in_fd;
					struct epoll_event in_event = {0};
					in_event.data.fd = in_fd;
					in_event.events = EPOLLIN;
					if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, in_fd, &in_event) == -1) {
						perror("epoll_ctl");
						exit(EXIT_FAILURE);
					}
				} else {

					// handle
				}
			}
		}
	}

	close(epoll_fd);
	close(server_fd);

	tls_uninit_ctx(ssl_ctx);
	free(client_data_buffer);
	free_http_client(server_client);
	free(events);

	rados_ioctx_destroy(bucket_io_ctx);
	rados_ioctx_destroy(data_io_ctx);

	return NULL;
}

static void rearrange_osd_addrs(char *ifname)
{
	struct ifreq ifr;
	int fd = socket(AF_INET, SOCK_DGRAM, 0);
	ifr.ifr_addr.sa_family = AF_INET;
	strncpy(ifr.ifr_name , ifname , IFNAMSIZ - 1);
	ioctl(fd, SIOCGIFADDR, &ifr);
	close(fd);

	char *my_ip_address = inet_ntoa(((struct sockaddr_in *)&ifr.ifr_addr )->sin_addr);

	// put osd info of this node to end of array
	for (int i = 0; i < num_osds - 1; i++) {
		if (strcmp(osd_addr_strs[i], my_ip_address) == 0) {
			char *osd_addr_str_tmp = osd_addr_strs[num_osds - 1];
			osd_addr_strs[num_osds - 1] = osd_addr_strs[i];
			osd_addr_strs[i] = osd_addr_str_tmp;

			int osd_id_tmp = osd_ids[num_osds - 1];
			osd_ids[num_osds - 1] = osd_ids[i];
			osd_ids[i] = osd_id_tmp;
		}
	}

	printf("My IP at interface %s is %s ; my OSD ID is %d\n",
		ifname, osd_addr_strs[num_osds - 1], osd_ids[num_osds - 1]);

	for (int i = 0; i < num_osds; i++)
	{
		memset(&osd_addrs[i], 0, sizeof(osd_addrs[i]));
		osd_addrs[i].sin_family = AF_INET;
		osd_addrs[i].sin_port = htons(S3_HTTP_PORT);
		if (inet_pton(AF_INET, osd_addr_strs[i], &osd_addrs[i].sin_addr) <= 0) {
			printf("Invalid address \"%s\" for osd id %d\n", osd_addr_strs[i], osd_ids[i]);
			exit(EXIT_FAILURE);
		}
	}

	num_peers = num_osds - 1;
}

static int ceph_config_parser(void* user, const char* section, const char* name, const char* value)
{
	long osd_id = -1;
	if (strncmp(section, "osd", 3) == 0) {
		char *id = strstr(section, ".");
		if (id != NULL) {
			osd_id = atol(id+1);
			if (strcmp(name, "public_addr") == 0) {
				osd_addr_strs[num_osds] = strdup(value);
				osd_ids[num_osds] = osd_id;
				num_osds++;
			}
		}
	}

	return 1;
}

int handoff_server_listen()
{
	struct sockaddr_in saddr = {0};
	saddr.sin_family = AF_INET;
	saddr.sin_addr.s_addr = INADDR_ANY;
	saddr.sin_port = htons(HANDOFF_CTRL_PORT);

	int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (listen_fd == -1) {
		perror("socket");
		return -1;
	}

	if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable))) {
		perror("setsockopt");
		return -1;
	}

	if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &enable, sizeof(enable))) {
		perror("setsockopt");
		return -1;
	}

	if (bind(listen_fd, (struct sockaddr *)&saddr, sizeof(saddr)) < 0) {
		perror("bind");
		return -1;
	}

	if (listen(listen_fd, SOMAXCONN) < 0) {
		perror("listen");
		return -1;
	}

	set_socket_non_blocking(listen_fd);

	return listen_fd;
}

int handoff_server_create_epoll(int listen_fd)
{
	int epoll_fd = epoll_create1(0);
	if (epoll_fd == -1) {
		perror("epoll_create1");
		return -1;
	}

	struct epoll_event event = {0};
	event.data.fd = listen_fd;
	event.events = EPOLLIN;
	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &event) == -1) {
		perror("epoll_ctl");
		return -1;
	}
}

void handoff_server_loop(int epoll_fd, int listen_fd, struct thread_param params[], const int nproc)
{
	int handoff_in_fds[nproc][num_peers];
	memset(handoff_in_fds, 0, sizeof(handoff_in_fds));

	struct epoll_event events[MAX_EVENTS];
	int n, nfds;
	while (server_running) {
		nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
		if (nfds == -1 && errno != EINTR) {
			perror("epoll_wait");
			exit(EXIT_FAILURE);
		}

		for (n = 0; n < nfds; n++) {
			if ((!(events[n].events & EPOLLIN) && !(events[n].events & EPOLLOUT))) {
				if (events[n].data.fd == listen_fd) {
					fprintf(stderr, "listen_fd returned invalid event %d\n", events[n].events);
					exit(EXIT_FAILURE);
				}
				int i, j, fd = events[n].data.fd;
				for (i = 0; i < nproc; i++)
				{
					for (j = 0; j < num_peers; j++)
					{
						if (fd == handoff_in_fds[i][j]) {
							break;
						}
					}
					if (j < num_peers) break;
				}
				if (i >= nproc) {
					printf("Unkown error or disconnected handoff_in fd %d\n", fd);
					close(fd);
				}
				printf("Disconnected handoff_in fd %d"
					" (host %s, thread %d, osd_id %d, event %d, err %s)\n",
					fd, osd_addr_strs[j], i, osd_ids[j], events[n].events, strerror(errno));
				handoff_in_fds[i][j] = 0;
				close(fd); // fd will be automatically removed from all epolls
			}
			else if (events[n].data.fd == listen_fd) {
				struct sockaddr_in in_addr;
				socklen_t in_len = sizeof(in_addr);
				int in_fd = accept(listen_fd, (struct sockaddr *)&in_addr, &in_len);
				if (in_fd == -1) {
					perror("accept");
					break;
				}

				int i = -1;
				for (; i < num_peers; i++) {
					if (osd_addrs[i].sin_addr.s_addr == in_addr.sin_addr.s_addr) {
						break;
					}
				}

				if (i != -1) {
					printf("Accepted handoff_in fd %d (host %s, port %d, peer_id %d)\n",
						in_fd, osd_addr_strs[i], ntohs(in_addr.sin_port), osd_ids[i]);
				} else {
					printf("Unkown handoff_in client, not in osd list, drop\n");
					close(in_fd);
					exit(EXIT_FAILURE);
				}

				int osd_arr_index = i;
				int thread_id = -1;
				for (int i = 0; i < WORKER_THREADS; i++)
				{
					// printf("handoff_in_fds[%d][%d] %d\n",
					//     i, peer_id, handoff_in_fds[i][peer_id]);
					if (handoff_in_fds[i][osd_arr_index] == 0) {
						thread_id = i;
						break;
					}
				}

				if (thread_id != -1) {
					set_socket_non_blocking(in_fd);
					handoff_in_fds[thread_id][osd_arr_index] = in_fd;
					printf("Dispatch conn %d to thread %d\n", in_fd, thread_id);
					uint64_t val = combine_ints_to_uint64(in_fd, osd_arr_index);
					int ret = write(params[thread_id].handoff_accept_eventfd, &val, sizeof(val));
					if (ret != sizeof(uint64_t)) {
						perror("write eventfd");
						exit(EXIT_FAILURE);
					}
					struct epoll_event event = {0};
					event.data.fd = in_fd;
					event.events = EPOLLRDHUP;
					if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, in_fd, &event) == -1) {
						perror("epoll_ctl");
						exit(EXIT_FAILURE);
					}
				} else {
					// TODO: we should actually scan all handoff_in_fds here to
					// check is any disconnect before this, otherwise cient
					// shouldn't send a new connection?
					printf("Redundant conn fd %d (host %s, port %d, peer_id %d)\n",
						in_fd, peer_addrs_str[peer_id], ntohs(in_addr.sin_port), peer_id);
					close(in_fd);
				}
			} else {
				printf("Unhanlded event fd %d event %d\n", events[n].data.fd, events[n].events);
			}
		}
	}

	close(epoll_fd);
	close(listen_fd);
}

int main(int argc, char *argv[])
{
	rados_t cluster;
	struct sigaction sa;
	const int enable = 1;
	int err;

	if (argc != 3) {
		fprintf(stderr, "Usage: %s [interface] [threads]\n", argv[0]);
		exit(1);
	}

	err = tls_init();
	if (err < 0) {
		fprintf(stderr, "%s: cannot init openssl\n", __func__);
		exit(1);
	}

	//long nproc = sysconf(_SC_NPROCESSORS_ONLN);
	long nproc = atol(argv[2]);
	pthread_t threads[nproc];
	struct thread_param param[nproc];

	// get my IP address and mapping OSDs
	char *ifname = argv[1];
	err = ini_parse("/etc/ceph/ceph.conf", ceph_config_parser, NULL);
	if (err < 0) {
		printf("Can't read '%s'!\n", "/etc/ceph/ceph.conf");
		exit(1);
	}
	else if (err) {
		printf("Bad config file (first error on line %d)!\n", err);
		exit(1);
	}

	rearrange_osd_addrs(ifname);

	err = rados_create2(&cluster, "ceph", "client.admin", 0);
	if (err < 0) {
		fprintf(stderr, "%s: cannot create a cluster handle: %s\n", argv[0], strerror(-err));
		exit(1);
	}

	err = rados_conf_read_file(cluster, "/etc/ceph/ceph.conf");
	if (err < 0) {
		fprintf(stderr, "%s: cannot read config file: %s\n", argv[0], strerror(-err));
		exit(1);
	}

	err = rados_connect(cluster);
	if (err < 0) {
		fprintf(stderr, "%s: cannot connect to cluster: %s\n", argv[0], strerror(-err));
		exit(EXIT_FAILURE);
	}

	int handoff_listen_fd = handoff_server_listen();
	if (handoff_listen_fd < 0) {
		fprintf(stderr, "%s: cannot init handoff server: %s\n", argv[0], strerror(-err));
		exit(EXIT_FAILURE);
	}

	int handoff_epoll_fd = handoff_server_create_epoll(handoff_listen_fd);
	if (handoff_epoll_fd < 0) {
		fprintf(stderr, "%s: cannot init handoff : %s\n", argv[0], strerror(-err));
		exit(EXIT_FAILURE);
	}

	cpu_set_t cpus;
	pthread_attr_t attr;
	sigset_t sigmask;

	sigemptyset(&sigmask);
	sigaddset(&sigmask, SIGINT);

	pthread_attr_init(&attr);

	sa.sa_handler = handleCtrlC;
	sa.sa_flags = 0;
	sigemptyset(&sa.sa_mask);
	sigaction(SIGINT, &sa, NULL);
	sigaction(SIGUSR1, &sa, NULL);

	// TODO: this word is abit confusing, to be fix
	printf("Launching %ld threads\n", S3_HTTP_PORT, nproc);
	for (int i = 0; i < nproc; i++) {
		param[i].thread_id = i;
		//param[i].server_fd = server_fd;
		param[i].cluster = &cluster;
		param[i].efd = eventfd(0, 0);
		if (param[i].efd == -1) {
			perror("eventfd");
			exit(EXIT_FAILURE);
		}

		CPU_ZERO(&cpus);
		CPU_SET((i + 1) % nproc, &cpus);

		pthread_attr_setsigmask_np(&attr, &sigmask);
		pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);

		if (pthread_create(&threads[i], &attr, &conn_wait, &param[i]) != 0) {
			fprintf(stderr, "fail to create thread %d\n", i);
			exit(1);
		}
	}

	handoff_server_loop(handoff_epoll_fd, handoff_listen_fd, param, nproc);
	printf("terminating\n");

	for (int i = 0; i < nproc; i++) {
		if (pthread_kill(threads[i], SIGUSR1) != 0) {
			fprintf(stderr, "fail to signal thread %d\n", i);
			exit(1);
		}

		if (pthread_join(threads[i], NULL) != 0) {
			fprintf(stderr, "fail to join thread %d\n", i);
			exit(1);
		}
	}

	rados_shutdown(cluster);
	pthread_attr_destroy(&attr);

	for (size_t i = 0; i < num_osds; i++) {
		if (osd_addr_strs[i] != NULL) free(osd_addr_strs[i]);
	}

	printf("Server terminated!\n");

	return 0;
}
