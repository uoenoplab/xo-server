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

#include <fcntl.h>

#include "http_client.h"

#define PORT 8080
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

size_t BUF_SIZE = sizeof(char) * 1024 * 1024 * 4;

volatile sig_atomic_t server_running = 1;
rados_t cluster;

struct thread_param {
	int server_fd;
	int thread_id;
};

void handleCtrlC(int signum) {
	printf("Received Ctrl+C. Stopping the server thread [%d]...\n", gettid());
	server_running = 0; // Set the flag to stop the server gracefully.
}

void handle_new_connection(int epoll_fd, int server_fd, int thread_id, rados_ioctx_t *bucket_io_ctx, rados_ioctx_t *data_io_ctx)
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
	event.events = EPOLLIN; // Edge-triggered mode
	struct http_client *client = create_http_client(epoll_fd, new_socket, bucket_io_ctx, data_io_ctx);
	event.data.ptr = client;

	fcntl(new_socket, F_SETFL, fcntl(new_socket, F_GETFL, 0) | O_NONBLOCK);

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
	free_http_client(client);
}

void handle_client_data(int epoll_fd, struct http_client *client, char *client_data_buffer, int thread_id)
{
	ssize_t bytes_received;

	memset(client_data_buffer, 0, BUF_SIZE);
	bytes_received = recv(client->fd, client_data_buffer, BUF_SIZE, 0);
	if (bytes_received <= 0) {
		// Client closed the connection or an error occurred
		if (bytes_received == 0) {
			printf("Thread %d: Client disconnected: %d\n", thread_id, client->fd);
		} else {
			perror("recv");
		}

		// Remove the client socket from the epoll event list
		handle_client_disconnect(epoll_fd, client); // Handle client disconnection
		return;
	}

	enum llhttp_errno ret;

	// Echo the received data back to the client
	ret = llhttp_execute(&(client->parser), client_data_buffer, bytes_received);
	if (ret != HPE_OK) {
		fprintf(stderr, "Parse error: %s %s\n", llhttp_errno_name(ret), client->parser.reason);
		fprintf(stderr, "%s\n", client_data_buffer);
		close(client->fd);
		free_http_client(client);
	}
}

static void *conn_wait(void *arg)
{
	rados_ioctx_t bucket_io_ctx;
	rados_ioctx_t data_io_ctx;

	struct thread_param *param = (struct thread_param*)arg;
	int server_fd = param->server_fd;
	int thread_id = param->thread_id;

	char *client_data_buffer = malloc(BUF_SIZE);

	int epoll_fd, event_count;
	struct epoll_event event;
	struct epoll_event *events = (struct epoll_event*)malloc(sizeof(struct epoll_event) * MAX_EVENTS);
	assert(events != NULL);

	int err = rados_ioctx_create(cluster, BUCKET_POOL, &bucket_io_ctx);
	if (err < 0) {
		fprintf(stderr, "cannot open rados pool %s: %s\n", BUCKET_POOL, strerror(-err));
		rados_shutdown(cluster);
		exit(1);
	}

	err = rados_ioctx_create(cluster, DATA_POOL, &data_io_ctx);
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

	// Add the server socket to the epoll event list
	event.events = EPOLLIN;
	struct http_client *server_client = create_http_client(server_fd, server_fd, NULL, NULL);
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
				printf("EINTR\n");
				continue;
			}
			else {
				perror("epoll_wait");
				exit(EXIT_FAILURE);
			}
		}

		for (int i = 0; i < event_count; i++) {
			// Handle events using callback functions
			struct http_client *c = (struct http_client*)events[i].data.ptr;
			if (c->fd == server_fd) {
				handle_new_connection(epoll_fd, server_fd, thread_id, &bucket_io_ctx, &data_io_ctx);
			}
			else if (events[i].events & EPOLLOUT) {
				send_client_data(c);
			}
			else if (events[i].events & EPOLLIN) {
				handle_client_data(epoll_fd, c, client_data_buffer, thread_id);
			}
		}
	}

	close(epoll_fd);

	free(client_data_buffer);
	free_http_client(server_client);
	free(events);

	rados_ioctx_destroy(bucket_io_ctx);
	rados_ioctx_destroy(data_io_ctx);

	return NULL;
}

int main(int argc, char *argv[])
{
	struct sigaction sa;
	const int enable = 1;
	int err;

	struct sockaddr_in server_addr, client_addr;
	int server_fd;

	//long nproc = sysconf(_SC_NPROCESSORS_ONLN);
	long nproc = 1;
	pthread_t threads[nproc];
	struct thread_param param[nproc];

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

	// Create a TCP socket
	if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror("socket");
		exit(EXIT_FAILURE);
	}

	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0)
		perror("setsockopt(SO_REUSEADDR) failed");

	// Initialize server address structure
	memset(&server_addr, 0, sizeof(server_addr));
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = INADDR_ANY;
	server_addr.sin_port = htons(PORT);

	// Bind the socket to the server address
	if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
		perror("bind");
		exit(EXIT_FAILURE);
	}

	// Listen for incoming connections
	if (listen(server_fd, 100) == -1) {
		perror("listen");
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

	printf("Server is listening on port %d with %ld threads\n", PORT, nproc);
	for (int i = 0; i < nproc; i++) {
		param[i].thread_id = i;
		param[i].server_fd = server_fd;

		CPU_ZERO(&cpus);
		CPU_SET((i + 1) % nproc, &cpus);

		pthread_attr_setsigmask_np(&attr, &sigmask);
		pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);

		if (pthread_create(&threads[i], &attr, &conn_wait, &param[i]) != 0) {
			fprintf(stderr, "fail to create thread %d\n", i);
			exit(1);
		}
	}

	// block until SIGINT
	pause();
	//conn_wait(&param[0]);

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

	close(server_fd);
	rados_shutdown(cluster);
	pthread_attr_destroy(&attr);
	printf("Server terminated!\n");

	return 0;
}
