#include <stdlib.h>
#include <stddef.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/ioctl.h>
#include <linux/sockios.h>
#include <errno.h>
#include <assert.h>
#include <sys/epoll.h>

#include "proto/socket_serialize.pb-c.h"

#include "handoff.h"
#include "http_client.h"
#include "osd_mapping.h"

int num_peers = 0;

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

struct handoff_out_req {
    void* c;
    struct handoff_out_req* next;
};

static struct handoff_out_req* handoff_out_req_create(void *client) {
    struct handoff_out_req* new_req = (struct handoff_out_req*)malloc(sizeof(struct handoff_out_req));
    if (!new_req) return NULL;

	new_req->c = client;
    new_req->next = NULL;

    return new_req;
}

static struct handoff_out_queue* handoff_out_queue_create() {
    struct handoff_out_queue* queue = (struct handoff_out_queue*)malloc(sizeof(struct handoff_out_queue));
    if (!queue) return NULL;

    queue->front = queue->rear = NULL;
    return queue;
}

static int handoff_out_queue_is_empty(struct handoff_out_queue* queue) {
    return queue->front == NULL;
}

// Function to add an element to the queue
static void handoff_out_enqueue(struct handoff_out_queue* queue, void *client)
{
    struct handoff_out_req* new_req = handoff_out_req_create(client);
    if (!new_req) {
        printf("Heap Overflow\n");
        return;
    }

    // If queue is empty, then new node is front and rear both
    if (queue->rear == NULL) {
        queue->front = queue->rear = new_req;
        return;
    }

    // Add the new node at the end of queue and change rear
    queue->rear->next = new_req;
    queue->rear = new_req;
}

// caller have to check queue is empty before dequeue!!!
static void handoff_out_dequeue(struct handoff_out_queue* queue, void **client) {
    struct handoff_out_req* temp = queue->front;
	*client = temp->c;

    queue->front = queue->front->next;
    // If front becomes NULL, then change rear also as NULL
    if (queue->front == NULL) {
        queue->rear = NULL;
    }

    free(temp);
}

// Example usage
int main() {
    struct migration_queue* queue = migration_create_queue();
    migration_enqueue(queue, 10);
    migration_enqueue(queue, 20);
    printf("Dequeued: %d\n", migration_dequeue(queue));
    printf("Dequeued: %d\n", migration_dequeue(queue));
    if (migration_is_queue_empty(queue)) {
        printf("Queue is empty\n");
    } else {
        printf("Queue is not empty\n");
    }
    return 0;
}

static int
restore_queue(int fd, int q, const uint8_t *buf, uint32_t len, int need_repair)
{
	int ret,  max_chunk = len, off = 0;

	if (need_repair)
		ret = setsockopt(fd, IPPROTO_TCP, TCP_REPAIR_QUEUE, &q, sizeof(q));
       		assert(ret == 0);
	do {
		int chunk = len > max_chunk ? max_chunk : len;
		ret = send(fd, buf + off, chunk, 0);
		if (ret <= 0) {
			if (max_chunk > 1024 /* see tcp_export.cpp */) {
				max_chunk >>= 1;
				continue;
			}
			return errno;
		}
		off += ret;
		len -= ret;
	} while (len);
	return 0;
}

static void handoff_out_serialize(struct http_client *client)
{
	int ret = -1;
	int fd = client->fd;
	int epfd = client->epoll_fd;
	struct tcp_info_sub info;

//#ifdef PROFILE
//	struct timespec start_time1, end_time1;
//	clock_gettime(CLOCK_MONOTONIC, &start_time1);
//#endif /* PROFILE */
	ret = setsockopt(fd, IPPROTO_TCP, TCP_REPAIR, &(int){1}, sizeof(int));
	assert(ret == 0);
#ifdef WITH_TLS
	int tls_export_context_size = 0;
	unsigned char tls_export_buf[0xFFFF];
#ifdef PROFILE
	clock_gettime(CLOCK_MONOTONIC, &start_time);
#endif /* PROFILE */
#ifdef WITH_KTLS
	tls_unmake_ktls(fd_state[fd].tls_context, fd);
	assert(ret == 0);
#endif /* WITH_KTLS */
	tls_export_context_size = tls_export_context(fd_state[fd].tls_context, tls_export_buf, sizeof(tls_export_buf), 1);
	assert(tls_export_context_size > 0);
#ifdef PROFILE
	clock_gettime(CLOCK_MONOTONIC, &end_time);
	printf("serialize tls: %lf\n", diff_timespec(&end_time, &start_time));
#endif /* PROFILE */
#endif /* WITH_TLS */
	socklen_t slen = sizeof(info);
	ret = getsockopt(fd, IPPROTO_TCP, TCP_INFO, &info, &slen);
	assert(ret == 0);
	ret = info.tcpi_state == TCP_ESTABLISHED || info.tcpi_state == TCP_CLOSE_WAIT;
	assert(ret == 0);

	int sendq_len, unsentq_len, recvq_len;
	ret = ioctl(fd, SIOCOUTQ, &sendq_len);
	assert(ret == 0);
	ret = ioctl(fd, SIOCOUTQNSD, &unsentq_len);
	assert(ret == 0);
	ret = ioctl(fd, SIOCINQ, &recvq_len);
	assert(ret == 0);

	uint32_t mss, ts;
	socklen_t olen_mss = sizeof(mss);
	socklen_t olen_ts = sizeof(ts);
	ret = getsockopt(fd, IPPROTO_TCP, TCP_MAXSEG, &mss, &olen_mss);
	assert(ret == 0);
	ret = getsockopt(fd, IPPROTO_TCP, TCP_TIMESTAMP, &ts, &olen_ts);
	assert(ret == 0);

	struct tcp_repair_window window;
	slen = sizeof(window);
	ret = getsockopt(fd, IPPROTO_TCP, TCP_REPAIR_WINDOW, &window, &slen);
	assert(ret == 0);

	struct sockaddr_in sin;
	bzero(&sin, sizeof(sin));
	sin.sin_family = AF_INET;
	slen = sizeof(sin);
	ret = getsockname(fd, (struct sockaddr *)&sin, &slen);
	assert(ret == 0);

	struct sockaddr_in sin2;
	bzero(&sin2, sizeof(sin2));
	sin2.sin_family = AF_INET;
	slen = sizeof(sin2);
	ret = getpeername(fd, (struct sockaddr *)&sin2, &slen);
	assert(ret == 0);

	const int qid_snd = TCP_SEND_QUEUE;
	ret = setsockopt(fd, IPPROTO_TCP, TCP_REPAIR_QUEUE, &qid_snd, sizeof(qid_snd));
	assert(ret == 0);
	socklen_t seqno_send;
	slen = sizeof(seqno_send);
	ret = getsockopt(fd, IPPROTO_TCP, TCP_QUEUE_SEQ, &seqno_send, &slen);
	assert(ret == 0);
	const int peek = MSG_PEEK | MSG_DONTWAIT;
	uint8_t *sndbuf;
	if (sendq_len)
	{
		sndbuf = calloc(1, sendq_len + 1);
		assert(sndbuf != NULL);
		ret = recv(fd, sndbuf, sendq_len + 1, peek);
		assert(ret == send_len);
	}

	const int qid_rcv = TCP_RECV_QUEUE;
	ret = setsockopt(fd, IPPROTO_TCP, TCP_REPAIR_QUEUE, &qid_rcv, sizeof(qid_rcv));
	assert(ret == 0);
	socklen_t seqno_recv;
	slen = sizeof(seqno_recv);
	ret = getsockopt(fd, IPPROTO_TCP, TCP_QUEUE_SEQ, &seqno_recv, &slen);
	assert(ret == 0);
	uint8_t *rcvbuf;
	if (recvq_len) {
		rcvbuf = calloc(1, recvq_len + 1);
		assert(rcvbuf != NULL);
		ret = recv(fd, rcvbuf, recvq_len + 1, peek);
		assert(ret == recvq_len);
	}
//#ifdef PROFILE
//	clock_gettime(CLOCK_MONOTONIC, &end_time1);
//	printf("serialize tcp + tls: %lf\n", diff_timespec(&end_time1, &start_time1));
//#endif /* PROFILE */

	/* clean up */
	ret = epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
	assert(ret == 0);
	close(fd);
#ifdef WITH_TLS
	if (fd_state[fd].tls_context) {
		tls_destroy_context(fd_state[fd].tls_context);
//		fd_state[fd].tls_context = NULL;
	}
#endif /* WITH_TLS */

	/* pack & send handoff msg */
	SocketSerialize migration_info = SOCKET_SERIALIZE__INIT;

	migration_info.msg_type = HANDOFF_MSG;
#ifdef WITH_TLS
	//tls variables setting up
	migration_info.buf.len= tls_export_context_size;
	migration_info.buf.data = tls_export_buf;
#endif /* WITH_TLS */
	//tcp variables setting up
	migration_info.sendq_len = sendq_len;
	migration_info.unsentq_len = unsentq_len;
	migration_info.recvq_len = recvq_len;
	migration_info.mss = mss;
	migration_info.timestamp = ts;
	migration_info.send_wscale = info.tcpi_snd_wscale;
	migration_info.recv_wscale = info.tcpi_rcv_wscale;
	migration_info.snd_wl1 = window.snd_wl1;
	migration_info.snd_wnd = window.snd_wnd;
	migration_info.max_window = window.max_window;
	migration_info.rev_wnd = window.rcv_wnd;
	migration_info.rev_wup = window.rcv_wup;
	migration_info.self_addr = sin.sin_addr.s_addr;
	migration_info.self_port = sin.sin_port;
	migration_info.peer_addr = sin2.sin_addr.s_addr;
	migration_info.peer_port = sin2.sin_port;
	migration_info.seq = seqno_send;
	migration_info.ack = seqno_recv;
	migration_info.sendq.len = sendq_len;
	migration_info.sendq.data = sndbuf;
	migration_info.recvq.len = recvq_len;
	migration_info.recvq.data = rcvbuf;

	//sending length and message
//	int ctrl_fd = (control_original_fds[worker_id] == 0) ? control_fake_fds[worker_id] : control_original_fds[worker_id];
//	ssize_t len = write(ctrl_fd, combined_buf, proto_len + sizeof(net_proto_len));
//	ast(len > 0, "write4", NULL);

	// serialize http client
	migration_info.method = client->method;

	migration_info.bucket_name = malloc(sizeof(char) * (strlen(client->bucket_name) + 1));
	snprintf(migration_info.bucket_name, strlen(client->bucket_name) + 1, "%s", client->bucket_name);

	migration_info.object_name = malloc(sizeof(char) * (strlen(client->object_name) + 1));
	snprintf(migration_info.object_name, strlen(client->object_name) + 1, "%s", client->object_name);

	migration_info.uri_str = malloc(sizeof(char) * (strlen(client->uri_str) + 1));
	snprintf(migration_info.uri_str, strlen(client->uri_str) + 1, "%s", client->uri_str);

	migration_info.object_size = client->object_size;

	size_t proto_len = socket_serialize__get_packed_size(&migration_info);
	uint32_t net_proto_len = htonl(proto_len);
	uint8_t *proto_buf = malloc(sizeof(net_proto_len) + proto_len);
	socket_serialize__pack(&migration_info, proto_buf + sizeof(net_proto_len));
	// add length of proto_buf at the begin
	memcpy(proto_buf, &net_proto_len, sizeof(net_proto_len));

	client->proto_buf = proto_buf:
	client->proto_buf_sent = 0;
	client->proto_buf_len = sizeof(net_proto_len) + proto_len;
	// send req to new server
	// int epoll_fd = client->epoll_fd;
	//free_http_client(client); // can't free it before hand off actually implemented
	// handle_handoff_out_recv(epoll_fd, proto_buf, proto_len, client);

	//
	// wait for ready: where to impl this?
	// - install redirection rule
	free(migration_info.uri_str);
	free(migration_info.object_name);
	free(migration_info.bucket_name);
	// free(proto_buf);
}

void handoff_out_connect(struct handoff_out *out_ctx) {
	out_ctx->fd = socket(AF_INET, SOCK_STREAM, 0);
	if (out_ctx->fd == -1) {
		perror("socket");
		exit(EXIT_FAILURE);
	}

	set_socket_non_blocking(out_ctx->fd);

	if (connect(out_ctx->fd, (struct sockaddr*)&osd_addrs[out_ctx->osd_arr_index],
		sizeof(osd_addrs[out_ctx->osd_arr_index])) == -1) {
		if (errno != EINPROGRESS) {
			perror("connect");
			close(out_ctx->fd);
			exit(EXIT_FAILURE);
		}
	}
}

void handoff_out_reconnect(struct handoff_out *out_ctx) {
	if (out_ctx->fd != 0) close(out_ctx->fd);
	handoff_out_connect(out_ctx);
	struct epoll_event event = {0};
	event.data.ptr = out_ctx;
	event.events = EPOLLOUT;
	if (epoll_ctl(out_ctx->epoll_fd, EPOLL_CTL_ADD, out_ctx->fd, &event) == -1) {
		perror("epoll_ctl");
		close(out_ctx->fd);
		exit(EXIT_FAILURE);
	}
}

 // if we don't have a connection yet before send migration request to other node, need to create new connection
void handoff_out_issue(int epoll_fd, uint32_t epoll_data_u32, struct http_client *client,
	struct handoff_out *out_ctx, int osd_arr_index)
{
	out_ctx->osd_arr_index = osd_arr_index;

	// serialize state, we don't delete client until handoff_out is complete
	handoff_out_serialize(client);

	// enqueue this handoff request
	if (out_ctx->queue) {
		out_ctx->queue = handoff_out_queue_create();
	}
	handoff_out_enqueue(out_ctx->queue, client);

	// we have a connected fd and it is in epoll, let the epoll to consume this new request
	if (out_ctx->is_fd_in_epoll)
		return;

	// we don't have a connceted fd, we need to create one
	if (out_ctx->fd == 0) {
		handoff_out_connect(out_ctx);
	}

	// we added this ctx into epoll
	struct epoll_event event = {0};
	out_ctx->epoll_data_u32 = epoll_data_u32;
	event.data.ptr = out_ctx;
	event.events = EPOLLOUT;
	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, out_ctx->fd, &event) == -1) {
		perror("epoll_ctl");
		close(out_ctx->fd);
		exit(EXIT_FAILURE);
	}

	out_ctx->is_fd_in_epoll = true;
	out_ctx->epoll_fd = epoll_fd;
}

// 0. if we dont have a outstaing client, try dequeue from queue,
//    if queue empty, then delete this fd from epoll
// 1. if we have a outstading client, send until whole thing sent out (via epoll)
// 1.a if sent failed with -1, delete current fd then re-connect
// 2. if we have sent out the whole thing, we change to in mode and wait for response
void handoff_out_send(struct handoff_out *out_ctx)
{
	if (out_ctx->client == NULL) {
		if (out_ctx->queue == NULL) {
			fprintf(stderr, "out_ctx->queue shouldn't be NULL here\n");
			exit(EXIT_FAILURE);
		}
		if (handoff_out_queue_is_empty(out_ctx->queue)) {
			if (epoll_ctl(out_ctx->epoll_fd, EPOLL_CTL_DEL, out_ctx->fd, NULL) == -1) {
				perror("epoll_ctl");
				close(out_ctx->fd);
				exit(EXIT_FAILURE);
			}
			out_ctx->is_fd_in_epoll = false;
			return;
		}
		handoff_out_dequeue(out_ctx->queue, &out_ctx->client);
	}

	int ret = send(out_ctx->fd, out_ctx->client->proto_buf + out_ctx->client->proto_buf_sent,
		out_ctx->client->proto_buf_len - out_ctx->client->proto_buf_sent, 0);
	if ((ret == 0) || (ret == -1 && errno != EAGAIN)) {
		perror("handoff_out_send send");
		handoff_out_reconnect(out_ctx);
		return;
	}
	if (ret == -1 && errno == EAGAIN) {
		return;
	}

	// send done
	if (out_ctx->client->proto_buf_len == out_ctx->client->proto_buf_sent) {
		struct epoll_event event = {0};
		event.data.ptr = out_ctx;
		event.events = EPOLLIN;
		if (epoll_ctl(out_ctx->epoll_fd, EPOLL_CTL_MOD, out_ctx->fd, &event) == -1) {
			perror("epoll_ctl");
			close(out_ctx->fd);
			exit(EXIT_FAILURE);
		}
	}
}

void handoff_out_recv(struct handoff_out *out_ctx)
{
	int ret = -1;
	/* unpack protobuf msg */
	SocketSerialize *new_migration_info = socket_serialize__unpack(NULL, handoff_msg_len, handoff_msg);
	assert(new_migration_info != NULL);

	if ((ret == 0) || (ret == -1 && errno != EAGAIN)) {
		perror("handoff_out_recv recv");
		handoff_out_reconnect(out_ctx);
		return;
	}
	if (ret == -1 && errno == EAGAIN) {
		return;
	}

};

// // receive response of migration request this node sent http_client to be removed
// void handle_handoff_in_recv(int epoll_fd, uint8_t *handoff_msg, size_t handoff_msg_len, struct http_client *client)
// {
// 	int ret = -1;
// 	/* unpack protobuf msg */
// 	SocketSerialize *new_migration_info = socket_serialize__unpack(NULL, handoff_msg_len, handoff_msg);
// 	assert(new_migration_info != NULL);
// 	//free(msg_buf);

// 	if (new_migration_info->msg_type == HANDOFF_MSG) {
// 		int rfd;
// 		struct sockaddr_in new_sin, new_sin2;
// 		struct tcp_repair_window new_window;

// 		struct TLSContext *imported_context;

// #ifdef PROFILE
// 		struct timespec start_time2, end_time2;
// 		clock_gettime(CLOCK_MONOTONIC, &start_time2);
// #endif

// 		/* restore tcp connection */
// 		rfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
// 		assert(rfd > 0);
// 		ret = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));
// 		assert(ret == 0);
// 		ret = setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &(int){1}, sizeof(int));
// 		assert(ret == 0);
// 		ioctl(rfd, FIONBIO, &(int){1});
// 		ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR, &(int){1}, sizeof(int));
// 		assert(ret == 0);
// 		const int qid_snd = TCP_SEND_QUEUE;
// 		ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR_QUEUE, &qid_snd, sizeof(qid_snd));
// 		assert(ret == 0);
// 		ret = setsockopt(rfd, IPPROTO_TCP, TCP_QUEUE_SEQ, &new_migration_info->seq, sizeof(new_migration_info->seq));
// 		assert(ret == 0);
// 		const int qid_rcv = TCP_RECV_QUEUE;
// 		ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR_QUEUE, &qid_rcv, sizeof(qid_rcv));
// 		assert(ret == 0);
// 		ret = setsockopt(rfd, IPPROTO_TCP, TCP_QUEUE_SEQ, &new_migration_info->ack, sizeof(new_migration_info->ack));
// 		assert(ret == 0);

// 		new_sin.sin_family = AF_INET;
// 		new_sin.sin_port = new_migration_info->self_port;
// 		// ???
// //		if (fd == control_fake_fds[worker_id])
// //			new_sin.sin_addr.s_addr = inet_addr(fake_ip);
// //		if (fd == control_original_fds[worker_id])
// //			new_sin.sin_addr.s_addr = inet_addr(original_ip);
// 		new_sin.sin_addr.s_addr = inet_addr(get_my_osd_addr_str());
// 		new_sin2.sin_family = AF_INET;
// 		new_sin2.sin_port = new_migration_info->peer_port;
// 		new_sin2.sin_addr.s_addr = new_migration_info->peer_addr;

// 		ret = bind(rfd, (struct sockaddr *)&new_sin, sizeof(new_sin));
// 		assert(ret == 0);
// 		ret = connect(rfd, (struct sockaddr *)&new_sin2, sizeof(new_sin2));
// 		assert(ret == 0);

// 		socklen_t new_ulen = new_migration_info->unsentq_len;
// 		socklen_t new_len = new_migration_info->sendq.len - new_ulen;

// 		if (new_len) {
// 			ret = restore_queue(rfd, TCP_SEND_QUEUE, (const uint8_t *)new_migration_info->sendq.data, new_len, 1);
// 			assert(ret == 0);
// 		}
// 		if (new_ulen) {
// 			ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR, &(int){-1}, sizeof(-1));
// 			assert(ret == 0);
// 			ret = restore_queue(rfd, TCP_SEND_QUEUE, (const uint8_t *)new_migration_info->sendq.data + new_len, new_ulen, 0);
// 			assert(ret == 0);
// 			ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR, &(int){1}, sizeof(int));
// 			assert(ret == 0);
// 		}
// 		if (new_migration_info->recvq.len > 0) {
// 			ret = restore_queue(rfd, TCP_RECV_QUEUE, (const uint8_t *)new_migration_info->recvq.data, new_migration_info->recvq.len, 1);
// 			assert(ret == 0);
// 		}

// 		struct tcp_repair_opt opts[4];
// 		bzero(opts, sizeof(opts));
// 		opts[0].opt_code = TCPOPT_SACK_PERM;
// 		opts[0].opt_val = 0;
// 		opts[1].opt_code = TCPOPT_WINDOW;
// 		opts[1].opt_val = new_migration_info->send_wscale + (new_migration_info->recv_wscale << 16);
// 		opts[2].opt_code = TCPOPT_TIMESTAMP;
// 		opts[2].opt_val = 0;
// 		opts[3].opt_code = TCPOPT_MSS;
// 		opts[3].opt_val = new_migration_info->mss;

// 		ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR_OPTIONS, opts, sizeof(struct tcp_repair_opt) * 4);
// 		assert(ret == 0);
// 		ret = setsockopt(rfd, IPPROTO_TCP, TCP_TIMESTAMP, &new_migration_info->timestamp, sizeof(new_migration_info->timestamp));
// 		assert(ret == 0);

// 		new_window.snd_wl1 = new_migration_info->snd_wl1;
// 		new_window.snd_wnd = new_migration_info->snd_wnd;
// 		new_window.max_window = new_migration_info->max_window;
// 		new_window.rcv_wnd = new_migration_info->rev_wnd;
// 		new_window.rcv_wup = new_migration_info->rev_wup;

// 		ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR_WINDOW, &new_window, sizeof(new_window));
// 		assert(ret == 0);
// #ifdef WITH_TLS
// #ifdef PROFILE
// 		struct timespec start_time11, end_time11;
// 		clock_gettime(CLOCK_MONOTONIC, &start_time11);
// #endif /* PROFILE */
// 		if (new_migration_info->buf.len > 0) {
// 			imported_context = tls_import_context(new_migration_info->buf.data, new_migration_info->buf.len);
// 			if (imported_context) {
// 				fd_state[rfd].tls_context = imported_context;
// 				tls_make_exportable(fd_state[rfd].tls_context, 1);
// 			}
// 			else {
// 				perror("tls_import_context");
// 				exit(0);
// 			}
// 		}
// #ifdef WITH_KTLS
// 		ret = tls_make_ktls(fd_state[rfd].tls_context, rfd);
// 		assert(ret == 0);
// #endif /* WITH_KTLS */
// #ifdef PROFILE
// 		//clock_gettime(CLOCK_MONOTONIC, &end_time11);
// 		//printf("deserialize tls: %lf\n", diff_timespec(&end_time11, &start_time11));
// #endif /* PROFILE */
// #endif /* WITH_TLS */

// 		// cannot recreate it before actually implemented
// 		//struct http_client *client = create_http_client(epoll_fd, rfd);
// 		//snprintf(client->bucket_name, MAX_BUCKET_NAME_SIZE, new_migration_info->bucket_name);
// 		//snprintf(client->object_name, MAX_OBJECT_NAME_SIZE, new_migration_info->object_name);
// 		//client->uri_str = realloc(client->uri_str, sizeof(char) * (strlen(new_migration_info->uri_str) + 1));
// 		//snprintf(client->uri_str, strlen(new_migration_info->uri_str) + 1, new_migration_info->uri_str);
// 		//client->object_size = new_migration_info->object_size;
// 		//client->method = new_migration_info->method;

// 		/* quiting repair mode */
// 		ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR, &(int){-1}, sizeof(int));
// 		assert(ret == 0);
// 		struct epoll_event event = {};
// 		client->fd = rfd;
// 		event.data.ptr = client;
// 		event.events = EPOLLIN;
// 		ret = epoll_ctl(client->epoll_fd, EPOLL_CTL_ADD, client->fd, &event);
// 		assert(ret == 0);

// 		// apply src IP modification
// 		//
// 		// send ready to original server
// 	}

// 	socket_serialize__free_unpacked(new_migration_info, NULL);
// }
