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
#include <fcntl.h>
#include <linux/tls.h>
#include <netinet/tcp.h>

#include "handoff.h"

#include "object_store.h"
#include "http_client.h"
#include "osd_mapping.h"
#include "tls.h"

#include "proto/socket_serialize.pb-c.h"

#define USE_TC

zlog_category_t *zlog_handoff;


/*

          +--------+                 +--------+
          |SERVER A|                 |SERVER B|
          +--------+                 +--------+

      S3 Client HTTP GET
               |
               |
               v
    on_headers_complete_cb
               |
               |             +------------+
               v             |            v
     handoff_out_serialize   |     handoff_in_recv
               |             |            |
               |             |            |
               v             |            v
       handoff_out_issue     |  handoff_in_deserialize
               |             |            |
               |             |            |
               v             |            v
       handoff_out_send------+    handoff_in_send
                                          |
               +--------------------------+
               v
       handoff_out_recv
               |
               v
    handoff_out_apply_rule
               |
               v
     handoff_out_send_done
               |
               +--------------------------+
                                          v
                                  handoff_in_recv_done

*/

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
    queue->num_requests = 0;
    return queue;
}

static int handoff_out_queue_is_empty(struct handoff_out_queue* queue) {
    return queue->front == NULL;
}

// Function to add an element to the queue
static void handoff_out_enqueue_front(struct handoff_out_queue* queue, void *client)
{
	struct handoff_out_req* new_req = handoff_out_req_create(client);
	if (!new_req) {
		zlog_error(zlog_handoff, "Heap Overflow");
    		return;
	}

	queue->num_requests++;

	// If queue is empty, then new node is front and rear both
	if (queue->rear == NULL) {
		queue->front = queue->rear = new_req;
		return;
	}

	// Add the new node at the front of queue
	new_req->next = queue->front;
	queue->front = new_req;
}

// Function to add an element to the queue
static void handoff_out_enqueue(struct handoff_out_queue* queue, void *client)
{
	struct handoff_out_req* new_req = handoff_out_req_create(client);
	if (!new_req) {
		zlog_error(zlog_handoff, "Heap Overflow");
		return;
	}

	queue->num_requests++;

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
	struct handoff_out_req* node = queue->front;
	*client = node->c;

	queue->front = queue->front->next;
	// If front becomes NULL, then change rear also as NULL
	if (queue->front == NULL) {
		queue->rear = NULL;
	}

	free(node);
	queue->num_requests--;
}

static int
restore_queue(int fd, int q, const uint8_t *buf, uint32_t len, int need_repair)
{
	int ret,  max_chunk = len, off = 0;

	if (need_repair) {
		ret = setsockopt(fd, IPPROTO_TCP, TCP_REPAIR_QUEUE, &q, sizeof(q));
       		assert(ret == 0);
	}
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

// **special serialize for reset-handoff**
void handoff_out_serialize_reset(struct http_client *client)
{
	int ret;
	socklen_t slen;

	struct sockaddr_in self_sin;
	bzero(&self_sin, sizeof(self_sin));
	self_sin.sin_family = AF_INET;
	slen = sizeof(self_sin);
	ret = getsockname(client->fd, (struct sockaddr *)&self_sin, &slen);
	assert(ret == 0);

	// apply blocking
	ret = apply_redirection_ebpf(client->client_addr, self_sin.sin_addr.s_addr,
				client->client_port, self_sin.sin_port,
				client->client_addr, client->client_mac, self_sin.sin_addr.s_addr, my_mac,
				client->client_port, self_sin.sin_port, true);
	assert(ret == 0);
	zlog_debug(zlog_handoff, "Applied blocking with eBPF (%d,%d)", ntohs(client->client_port), ntohs(self_sin.sin_port));

	// build reset proto_buf
	SocketSerialize migration_info_reset = SOCKET_SERIALIZE__INIT;
	migration_info_reset.msg_type = HANDOFF_RESET_REQUEST;

	migration_info_reset.self_addr = self_sin.sin_addr.s_addr;
	migration_info_reset.self_port = self_sin.sin_port;
	migration_info_reset.peer_addr = client->client_addr;
	migration_info_reset.peer_port = client->client_port;

	zlog_debug(zlog_handoff, "Serializing connection: client(%" PRIu64 ":%d) server(%" PRIu64 ":%d)",
		migration_info_reset.peer_addr, ntohs(migration_info_reset.peer_port),
		migration_info_reset.self_addr, ntohs(migration_info_reset.self_port));

	int proto_len = socket_serialize__get_packed_size(&migration_info_reset);
	uint32_t net_proto_len = htonl(proto_len);
	client->proto_buf = malloc(sizeof(net_proto_len) + proto_len);
	socket_serialize__pack(&migration_info_reset, client->proto_buf + sizeof(net_proto_len));
	// add length of proto_buf at the begin
	memcpy(client->proto_buf, &net_proto_len, sizeof(net_proto_len));
	client->proto_buf_sent = 0;
	client->proto_buf_len = sizeof(net_proto_len) + proto_len;

	client->to_migrate = client->from_migrate;

	close(client->fd);
	client->fd = -client->fd;
}

// **special serialize for re-handoff**
static void handoff_out_serialize_rehandoff(struct http_client **client_to_handoff_again, SocketSerialize *migration_info)
{
	int ret = 0;

	// apply blocking
	ret = apply_redirection_ebpf(migration_info->peer_addr, get_my_osd_addr().sin_addr.s_addr,
			migration_info->peer_port, migration_info->self_port,
			migration_info->peer_addr, (uint8_t *)&migration_info->peer_mac, get_my_osd_addr().sin_addr.s_addr, my_mac,
			migration_info->peer_port, migration_info->self_port, true);
	assert(ret == 0);
	zlog_debug(zlog_handoff, "Applied blocking with eBPF (%d,%d)", ntohs(migration_info->peer_port), ntohs(migration_info->self_port));

	// we set fd as 0 so it will not considered as reset handoff
	struct http_client *client = create_http_client(-1, 0);
	client->to_migrate = migration_info->acting_primary_osd_id;
	client->acting_primary_osd_id = migration_info->acting_primary_osd_id;
	client->client_addr = migration_info->peer_addr;
	client->client_port = migration_info->peer_port;

	SocketSerialize migration_info_handoff_again = *migration_info;

	migration_info_handoff_again.msg_type = HANDOFF_REQUEST;
	migration_info_handoff_again.self_addr = get_my_osd_addr().sin_addr.s_addr;

	int proto_len = socket_serialize__get_packed_size(&migration_info_handoff_again);
	uint32_t net_proto_len = htonl(proto_len);
	client->proto_buf = malloc(sizeof(net_proto_len) + proto_len);
	socket_serialize__pack(&migration_info_handoff_again, client->proto_buf + sizeof(net_proto_len));
	// add length of proto_buf at the begin
	memcpy(client->proto_buf, &net_proto_len, sizeof(net_proto_len));
	client->proto_buf_sent = 0;
	client->proto_buf_len = sizeof(net_proto_len) + proto_len;

	*client_to_handoff_again = client;
}

void handoff_out_serialize(struct http_client *client)
{
	int ret = -1;
	int fd = client->fd;
	int epfd = client->epoll_fd;
	struct tcp_info_sub info;

	socklen_t slen;
	struct sockaddr_in self_sin;
	bzero(&self_sin, sizeof(self_sin));
	self_sin.sin_family = AF_INET;
	slen = sizeof(self_sin);
	ret = getsockname(fd, (struct sockaddr *)&self_sin, &slen);
	assert(ret == 0);

	// apply blocking
	ret = apply_redirection_ebpf(client->client_addr, self_sin.sin_addr.s_addr,
				client->client_port, self_sin.sin_port,
				client->client_addr, client->client_mac, self_sin.sin_addr.s_addr, my_mac,
				client->client_port, self_sin.sin_port, true);
	assert(ret == 0);
	zlog_debug(zlog_handoff, "Applied blocking with eBPF (%d,%d)", ntohs(client->client_port), ntohs(self_sin.sin_port));

	ret = setsockopt(fd, IPPROTO_TCP, TCP_REPAIR, &(int){1}, sizeof(int));
	assert(ret == 0);

	slen = sizeof(info);
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

	const int qid_snd = TCP_SEND_QUEUE;
	ret = setsockopt(fd, IPPROTO_TCP, TCP_REPAIR_QUEUE, &qid_snd, sizeof(qid_snd));
	assert(ret == 0);
	socklen_t seqno_send;
	slen = sizeof(seqno_send);
	ret = getsockopt(fd, IPPROTO_TCP, TCP_QUEUE_SEQ, &seqno_send, &slen);
	assert(ret == 0);
	const int peek = MSG_PEEK | MSG_DONTWAIT;
	uint8_t *sndbuf = NULL;
	if (sendq_len)
	{
		sndbuf = calloc(1, sendq_len + 1);
		assert(sndbuf != NULL);
		ret = recv(fd, sndbuf, sendq_len + 1, peek);
		assert(ret == sendq_len);
	}

	const int qid_rcv = TCP_RECV_QUEUE;
	ret = setsockopt(fd, IPPROTO_TCP, TCP_REPAIR_QUEUE, &qid_rcv, sizeof(qid_rcv));
	assert(ret == 0);
	socklen_t seqno_recv;
	slen = sizeof(seqno_recv);
	ret = getsockopt(fd, IPPROTO_TCP, TCP_QUEUE_SEQ, &seqno_recv, &slen);
	assert(ret == 0);
	uint8_t *rcvbuf = NULL;
	if (recvq_len) {
		rcvbuf = calloc(1, recvq_len + 1);
		assert(rcvbuf != NULL);
		ret = recv(fd, rcvbuf, recvq_len + 1, peek);
		assert(ret == recvq_len);
	}

	int ktlsbuf_len = 0;
	uint8_t *ktlsbuf_data = NULL;

	if (client->tls.is_ktls_set) {
		ktlsbuf_len = 2 * sizeof(struct tls12_crypto_info_aes_gcm_256);
		ktlsbuf_data = malloc(ktlsbuf_len);

		struct tls12_crypto_info_aes_gcm_256 *crypto_info_send = ktlsbuf_data;
		struct tls12_crypto_info_aes_gcm_256 *crypto_info_recv = ktlsbuf_data +
			sizeof(struct tls12_crypto_info_aes_gcm_256);

		socklen_t optlen = sizeof(struct tls12_crypto_info_aes_gcm_256);
		if (getsockopt(fd, SOL_TLS, TLS_TX, crypto_info_send, &optlen)) {
			zlog_fatal(zlog_tls, "Couldn't get TLS_TX option (%s)", strerror(errno));
			exit(EXIT_FAILURE);
		}

		optlen = sizeof(struct tls12_crypto_info_aes_gcm_256);
		if (getsockopt(fd, SOL_TLS, TLS_RX, crypto_info_recv, &optlen)) {
			zlog_fatal(zlog_tls, "Couldn't get TLS_RX option (%s)", strerror(errno));
			exit(EXIT_FAILURE);
		}
	}

	/* clean up */
	ret = epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
	assert(ret == 0);
	close(fd);

	/* pack & send handoff msg */
	SocketSerialize migration_info = SOCKET_SERIALIZE__INIT;
	migration_info.msg_type = HANDOFF_REQUEST;
	if (client->from_migrate != -1) {
		migration_info.msg_type = HANDOFF_BACK_REQUEST;
	}

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
	migration_info.self_addr = self_sin.sin_addr.s_addr;
	migration_info.self_port = self_sin.sin_port;
	migration_info.peer_addr = client->client_addr;
	memcpy(&(migration_info.peer_mac), client->client_mac, sizeof(uint8_t) * 6);
	migration_info.peer_port = client->client_port;
	migration_info.seq = seqno_send;
	migration_info.ack = seqno_recv;
	migration_info.sendq.len = sendq_len;
	migration_info.sendq.data = sndbuf;
	migration_info.recvq.len = recvq_len;
	migration_info.recvq.data = rcvbuf;
	migration_info.ktlsbuf.len = ktlsbuf_len;
	migration_info.ktlsbuf.data = ktlsbuf_data;

	// serialize http client
	migration_info.method = client->method;

	migration_info.bucket_name = malloc(sizeof(char) * (strlen(client->bucket_name) + 1));
	snprintf(migration_info.bucket_name, strlen(client->bucket_name) + 1, "%s", client->bucket_name);

	migration_info.object_name = malloc(sizeof(char) * (strlen(client->object_name) + 1));
	snprintf(migration_info.object_name, strlen(client->object_name) + 1, "%s", client->object_name);

	migration_info.uri_str = malloc(sizeof(char) * (strlen(client->uri_str) + 1));
	snprintf(migration_info.uri_str, strlen(client->uri_str) + 1, "%s", client->uri_str);

	migration_info.object_size = client->object_size;

	migration_info.acting_primary_osd_id = client->acting_primary_osd_id;

	size_t proto_len = socket_serialize__get_packed_size(&migration_info);
	uint32_t net_proto_len = htonl(proto_len);
	uint8_t *proto_buf = malloc(sizeof(net_proto_len) + proto_len);
	socket_serialize__pack(&migration_info, proto_buf + sizeof(net_proto_len));
	// add length of proto_buf at the begin
	memcpy(proto_buf, &net_proto_len, sizeof(net_proto_len));

	client->proto_buf = proto_buf;
	client->proto_buf_sent = 0;
	client->proto_buf_len = sizeof(net_proto_len) + proto_len;

	free(migration_info.uri_str);
	free(migration_info.object_name);
	free(migration_info.bucket_name);

	if (ktlsbuf_data != NULL) {
		free(ktlsbuf_data);
	}
}

static void set_socket_non_blocking(int socket_fd)
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

void handoff_out_connect(struct handoff_out *out_ctx) {
	zlog_debug(zlog_handoff, "HANDOFF_OUT To connect to OSD %d (%s:%d)",
		osd_ids[out_ctx->osd_arr_index],
		osd_addr_strs[out_ctx->osd_arr_index],
		HANDOFF_CTRL_PORT + out_ctx->thread_id);

	out_ctx->fd = socket(AF_INET, SOCK_STREAM, 0);
	if (out_ctx->fd == -1) {
		zlog_fatal(zlog_handoff, "create socket fail: %s", strerror(errno));
		exit(EXIT_FAILURE);
	}

	if (setsockopt(out_ctx->fd, IPPROTO_TCP, TCP_QUICKACK, &(int){1}, sizeof(int)) == -1) {
		zlog_fatal(zlog_handoff, "setsockopt TCP_QUICKACK fail: %s", strerror(errno));
		exit(EXIT_FAILURE);
	}

	if (setsockopt(out_ctx->fd, IPPROTO_TCP, TCP_NODELAY, &(int){1}, sizeof(int)) == -1) {
		zlog_fatal(zlog_handoff, "setsockopt TCP_NODELAY fail: %s", strerror(errno));
		exit(EXIT_FAILURE);
	}

	set_socket_non_blocking(out_ctx->fd);

	struct sockaddr_in out_addr = osd_addrs[out_ctx->osd_arr_index];
	out_addr.sin_port = htons(HANDOFF_CTRL_PORT + out_ctx->thread_id);
	if (connect(out_ctx->fd, (struct sockaddr*)&out_addr, sizeof(out_addr)) == -1) {
		if (errno != EINPROGRESS) {
			zlog_fatal(zlog_handoff, "Failed to connect to OSD %d (fd=%d): %s", osd_ids[out_ctx->osd_arr_index], out_ctx->fd, strerror(errno));
			close(out_ctx->fd);
			exit(EXIT_FAILURE);
		}
		out_ctx->is_fd_connected = false;
		return;
	}

	out_ctx->is_fd_connected = true;
	out_ctx->reconnect_count = 0;

	return;
}

int handoff_out_reconnect(struct handoff_out *out_ctx) {
	struct epoll_event event = {0};

	if (out_ctx->is_fd_connected == false) {
		int val = 0;
		socklen_t len = sizeof(val);
		int ret = getsockopt(out_ctx->fd, SOL_SOCKET, SO_ERROR, &val, &len);
		if (ret != 0) {
			zlog_fatal(zlog_handoff, "error getting socket error code: %s", strerror(errno));
			exit(EXIT_FAILURE);
		}

		if (val != 0) {
			zlog_error(zlog_handoff, "socket error: %s", strerror(val));
		} else {
			out_ctx->is_fd_connected = true;
			out_ctx->reconnect_count = 0;
			goto connected;
		}
	}

	out_ctx->reconnect_count++;
	out_ctx->is_fd_connected = false;
	if (out_ctx->reconnect_count > MAX_HANDOFF_OUT_RECONNECT) {
		zlog_fatal(zlog_handoff, "HANDOFF_OUT try RE-connect to OSD %d too many times (%s:%d), reconnect count %d",
				osd_ids[out_ctx->osd_arr_index],
				osd_addr_strs[out_ctx->osd_arr_index],
				HANDOFF_CTRL_PORT + out_ctx->thread_id,
			out_ctx->reconnect_count);
		exit(EXIT_FAILURE);
	}

	zlog_error(zlog_handoff, "HANDOFF_OUT Try RE-connect to OSD %d (%s:%d), reconnect count %d",
			osd_ids[out_ctx->osd_arr_index],
			osd_addr_strs[out_ctx->osd_arr_index],
			HANDOFF_CTRL_PORT + out_ctx->thread_id,
			out_ctx->reconnect_count);

	if (out_ctx->fd != 0) close(out_ctx->fd);
	handoff_out_connect(out_ctx);

	event.data.ptr = out_ctx;
	event.events = EPOLLOUT;
	if (epoll_ctl(out_ctx->epoll_fd, EPOLL_CTL_ADD, out_ctx->fd, &event) == -1) {
		zlog_fatal(zlog_handoff, "HANDOFF_OUT Failed to add control conn to epoll (fd=%d,osd=%d): %s", out_ctx->fd, osd_ids[out_ctx->osd_arr_index], strerror(errno));
		close(out_ctx->fd);
		exit(EXIT_FAILURE);
	}

	if (out_ctx->is_fd_connected) {
		goto connected;
	}

	return -1;

connected:

	bool send = false;

	if (out_ctx->client == NULL) {
		send = true;
	} else {
		if (out_ctx->client->proto_buf != NULL) {
			send = true;
		}
	}

	if (send) {
		handoff_out_send(out_ctx);
	} else {
		event.data.ptr = out_ctx;
		event.events = EPOLLIN;
		if (epoll_ctl(out_ctx->epoll_fd, EPOLL_CTL_MOD, out_ctx->fd, &event) == -1) {
			perror("epoll_ctl");
			close(out_ctx->fd);
			exit(EXIT_FAILURE);
		}
		handoff_out_recv(out_ctx);
	}

	return 0;
}

// have to serialize before issue
static void do_handoff_out_issue(int epoll_fd, uint32_t epoll_data_u32, struct http_client *client,
	struct handoff_out *out_ctx, int osd_arr_index, int thread_id, bool urgent)
{
	// enqueue this handoff request
	if (!out_ctx->queue) {
		out_ctx->queue = handoff_out_queue_create();
	}

	if (urgent) {
		handoff_out_enqueue_front(out_ctx->queue, client);
		zlog_debug(zlog_handoff, "HANDOFF_OUT (front)enqueued (backlog=%d) Migration (OSD=%d,conn=%d)",
			out_ctx->queue->num_requests, osd_ids[out_ctx->osd_arr_index], client->fd);
	} else {
		handoff_out_enqueue(out_ctx->queue, client);
		zlog_debug(zlog_handoff, "HANDOFF_OUT enqueued (backlog=%d) Migration (OSD=%d,conn=%d)",
			out_ctx->queue->num_requests, osd_ids[out_ctx->osd_arr_index], client->fd);
	}

	if (out_ctx->client) {
		return;
	}

	// we have a connected fd and it is in epoll, let the epoll to consume this new request
	if (out_ctx->is_fd_in_epoll) {
		return;
	}

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
		zlog_fatal(zlog_handoff, "HANDOFF_OUT Failed to add control conn to epoll (fd=%d,osd=%d): %s", out_ctx->fd, osd_ids[out_ctx->osd_arr_index], strerror(errno));
		close(out_ctx->fd);
		exit(EXIT_FAILURE);
	}

	out_ctx->is_fd_in_epoll = true;
	out_ctx->epoll_fd = epoll_fd;

	if (out_ctx->is_fd_connected) {
		handoff_out_send(out_ctx);
	}
}

void handoff_out_issue(int epoll_fd, uint32_t epoll_data_u32, struct http_client *client,
	struct handoff_out *out_ctx, int osd_arr_index, int thread_id)
{
	return do_handoff_out_issue(epoll_fd, epoll_data_u32, client, out_ctx, osd_arr_index, thread_id, false);
}

void handoff_out_issue_urgent(int epoll_fd, uint32_t epoll_data_u32, struct http_client *client,
	struct handoff_out *out_ctx, int osd_arr_index, int thread_id)
{
	return do_handoff_out_issue(epoll_fd, epoll_data_u32, client, out_ctx, osd_arr_index, thread_id, true);
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
			zlog_fatal(zlog_handoff, "out_ctx->queue shouldn't be NULL here");
			exit(EXIT_FAILURE);
		}
		if (handoff_out_queue_is_empty(out_ctx->queue)) {
			return;
		}
		handoff_out_dequeue(out_ctx->queue, (void **)&out_ctx->client);
	}

	zlog_debug(zlog_handoff, "HANDOFF_OUT dequeued (backlog %d) Migration work (OSD=%d,conn=%d)",
		out_ctx->queue->num_requests, out_ctx->client->to_migrate, out_ctx->client->fd);

	int ret = send(out_ctx->fd, out_ctx->client->proto_buf + out_ctx->client->proto_buf_sent,
		out_ctx->client->proto_buf_len - out_ctx->client->proto_buf_sent, 0);
	if ((ret == 0) || (ret == -1 && errno != EAGAIN)) {
		zlog_error(zlog_handoff, "handoff_out_send send error %d errmsg %s", errno, strerror(errno));
		handoff_out_reconnect(out_ctx);
		return;
	}
	if (ret == -1 && errno == EAGAIN) {
		return;
	}

	out_ctx->client->proto_buf_sent += ret;

	// send done
	if (out_ctx->client->proto_buf_len == out_ctx->client->proto_buf_sent) {
		zlog_debug(zlog_handoff, "HANDOFF_OUT migration request sent (OSD=%d,conn=%d)", out_ctx->client->to_migrate, out_ctx->client->fd);

		if (out_ctx->client->proto_buf != NULL) {
			free(out_ctx->client->proto_buf);
			out_ctx->client->proto_buf = NULL;
		}
		out_ctx->client->proto_buf_len = 0;
		out_ctx->client->proto_buf_sent = 0;
		struct epoll_event event = {0};
		event.data.ptr = out_ctx;
		event.events = EPOLLIN;
		if (epoll_ctl(out_ctx->epoll_fd, EPOLL_CTL_MOD, out_ctx->fd, &event) == -1) {
			zlog_fatal(zlog_handoff, "Change control socket to receive failed (out_ctx->fd=%d): %s", out_ctx->fd, strerror(errno));
			close(out_ctx->fd);
			exit(EXIT_FAILURE);
		}
	}
}

static void handoff_out_send_done(struct handoff_out *out_ctx)
{
	zlog_debug(zlog_handoff, "HANDOFF_OUT send done (osd=%d)", osd_ids[out_ctx->osd_arr_index]);

	uint32_t magic_number = UINT32_MAX;
	int ret = send(out_ctx->fd, &magic_number,
		sizeof(magic_number), NULL);
	if (ret != sizeof(magic_number)) {
		zlog_fatal(zlog_handoff, "Fail to send handoff_out magic number %" PRIu32 ": ret=%d", magic_number, ret);
		exit(EXIT_FAILURE);
	}
}

void handoff_out_recv(struct handoff_out *out_ctx)
{
	if (out_ctx->recv_protobuf == NULL) {
		int ret = recv(out_ctx->fd, &out_ctx->recv_protobuf_len, sizeof(out_ctx->recv_protobuf_len), 0);
		if ((ret == 0) || (ret == -1 && errno != EAGAIN)) {
			perror("handoff_out_recv recv1");
			handoff_out_reconnect(out_ctx);
			return;
		}
		if (ret == -1 && errno == EAGAIN) {
			return;
		}
		if (ret != sizeof(out_ctx->recv_protobuf_len)) {
			zlog_fatal(zlog_handoff, "TCP recv not equal to 4 bytes on header");
			exit(EXIT_FAILURE);
		}
		out_ctx->recv_protobuf_len = ntohl(out_ctx->recv_protobuf_len);
		if (out_ctx->recv_protobuf_len == 0) {
			zlog_fatal(zlog_handoff, "out_ctx->recv_protobuf_len is zero");
			exit(EXIT_FAILURE);
		}
		out_ctx->recv_protobuf = malloc(out_ctx->recv_protobuf_len);
	}

	int ret = recv(out_ctx->fd, out_ctx->recv_protobuf + out_ctx->recv_protobuf_received,
		out_ctx->recv_protobuf_len - out_ctx->recv_protobuf_received, 0);
	if ((ret == 0) || (ret == -1 && errno != EAGAIN)) {
		zlog_error(zlog_handoff, "fail to receive protobuf, reconnect: %s", strerror(errno));
		handoff_out_reconnect(out_ctx);
		return;
	}
	if (ret == -1 && errno == EAGAIN) {
		return;
	}

	out_ctx->recv_protobuf_received += ret;
	if (out_ctx->recv_protobuf_received < out_ctx->recv_protobuf_len)
		return;

	zlog_debug(zlog_handoff, "HANDOFF_OUT Received response for migration request (osd=%d,conn=%d)", out_ctx->client->to_migrate, out_ctx->client->fd);

	SocketSerialize *migration_info = socket_serialize__unpack(NULL,
		out_ctx->recv_protobuf_len, out_ctx->recv_protobuf);
	if (migration_info == NULL) {
		zlog_fatal(zlog_handoff, "unable to unpack recv protobuf");
		exit(EXIT_FAILURE);
	}
	if (migration_info->msg_type != HANDOFF_DONE) {
		zlog_fatal(zlog_handoff, "HANDOFF_OUT Received migration response not HANDOFF_DONE");
		exit(EXIT_FAILURE);
	}

	free(out_ctx->recv_protobuf);
	out_ctx->recv_protobuf = NULL;
	out_ctx->recv_protobuf_len = 0;
	out_ctx->recv_protobuf_received = 0;

	uint8_t fake_server_mac[6];
	memcpy(fake_server_mac, &(migration_info->peer_mac), sizeof(uint8_t) * 6);

	if (out_ctx->client->from_migrate == -1) {
		// normal handoff
		// insert redirection rule: peer mac is fake server mac
#ifdef USE_TC
		ret = apply_redirection(migration_info->peer_addr, migration_info->self_addr,
					migration_info->peer_port, migration_info->self_port,
					migration_info->peer_addr, my_mac, osd_addrs[out_ctx->osd_arr_index].sin_addr.s_addr, fake_server_mac,
					migration_info->peer_port, migration_info->self_port, false, true);
#else
		ret = apply_redirection_ebpf(migration_info->peer_addr, migration_info->self_addr,
					migration_info->peer_port, migration_info->self_port,
					migration_info->peer_addr, my_mac, osd_addrs[out_ctx->osd_arr_index].sin_addr.s_addr, fake_server_mac,
					migration_info->peer_port, migration_info->self_port, false);
#endif
		assert(ret == 0);

		zlog_debug(zlog_handoff, "HANDOFF_OUT Apply redirection rule (osd=%d,conn=%d) client(%" PRIu64 ":%d) server(%" PRIu64 ":%d) to fake server(%u:%d)",
				out_ctx->client->to_migrate, out_ctx->client->fd,
				migration_info->peer_addr, ntohs(migration_info->peer_port),
				migration_info->self_addr, ntohs(migration_info->self_port),
				osd_addrs[out_ctx->osd_arr_index].sin_addr.s_addr, ntohs(migration_info->self_port));
	} else {
		// handoff back and handoff reset
		// remove src IP modification
		zlog_debug(zlog_handoff, "HANDOFF_OUT Handing connection back to original server or conn reset (osd=%d,conn=%d)",
					out_ctx->client->to_migrate, out_ctx->client->fd);
		ret = remove_redirection(migration_info->self_addr, migration_info->peer_addr,
					migration_info->self_port, migration_info->peer_port);
		assert(ret == 0);
		zlog_debug(zlog_handoff, "Remove src ip modification client(%" PRIu64 ":%d) self(%" PRIu64 ":%d)",
					migration_info->peer_addr, ntohs(migration_info->peer_port),
					migration_info->self_addr, ntohs(migration_info->self_port));
	}

	// we don't need this for handoff_reset where fd already closed
	// if (out_ctx->client->fd >= 0) {
	// remove blocking
#ifdef USE_TC
	ret = remove_redirection_ebpf(migration_info->peer_addr, migration_info->self_addr,
				migration_info->peer_port, migration_info->self_port);
	assert(ret == 0);
	zlog_debug(zlog_handoff, "HANDOFF_OUT Removed blocking rule (osd=%d,client=%" PRIu64 ":%d,original_server=%" PRIu64 ":%d)",
		out_ctx->client->to_migrate,
		migration_info->peer_addr, ntohs(migration_info->peer_port),
		migration_info->self_addr, ntohs(migration_info->self_port));
#endif
	// }

	handoff_out_send_done(out_ctx);

	tls_free_client(out_ctx->client);
	free_http_client(out_ctx->client);
	out_ctx->client = NULL;
	socket_serialize__free_unpacked(migration_info, NULL);

	if (handoff_out_queue_is_empty(out_ctx->queue)) {
		if (epoll_ctl(out_ctx->epoll_fd, EPOLL_CTL_DEL, out_ctx->fd, NULL) == -1) {
			zlog_fatal(zlog_handoff, "Failed to remove control connection from epoll (fd=%d): %s", out_ctx->fd, strerror(errno));
			exit(EXIT_FAILURE);
		}
		zlog_debug(zlog_handoff, "Set is_fd_in_epoll to false");
		out_ctx->is_fd_in_epoll = false;
		return;
	} else {
		struct epoll_event event = {0};
		event.data.ptr = out_ctx;
		event.events = EPOLLOUT;
		if (epoll_ctl(out_ctx->epoll_fd, EPOLL_CTL_MOD, out_ctx->fd, &event) == -1) {
			zlog_fatal(zlog_handoff, "Failed to set control socket to send mode (fd=%d): %s", out_ctx->fd, strerror(errno));
			close(out_ctx->fd);
			exit(EXIT_FAILURE);
		}
		handoff_out_send(out_ctx);
	}
}

static void handoff_in_deserialize(struct handoff_in *in_ctx, SocketSerialize *migration_info)
{
	int ret = -1;

	int rfd;
	struct sockaddr_in server_sin, client_sin;
	struct tcp_repair_window new_window;

#ifdef PROFILE
	struct timespec start_time2, end_time2;
	clock_gettime(CLOCK_MONOTONIC, &start_time2);
#endif

	/* restore tcp connection */
	rfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	assert(rfd > 0);
	ret = setsockopt(rfd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));
	assert(ret == 0);
	ret = setsockopt(rfd, SOL_SOCKET, SO_REUSEPORT, &(int){1}, sizeof(int));
	assert(ret == 0);
	ioctl(rfd, FIONBIO, &(int){1});
	ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR, &(int){1}, sizeof(int));
	assert(ret == 0);
	const int qid_snd = TCP_SEND_QUEUE;
	ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR_QUEUE, &qid_snd, sizeof(qid_snd));
	assert(ret == 0);
	ret = setsockopt(rfd, IPPROTO_TCP, TCP_QUEUE_SEQ, &migration_info->seq, sizeof(migration_info->seq));
	assert(ret == 0);
	const int qid_rcv = TCP_RECV_QUEUE;
	ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR_QUEUE, &qid_rcv, sizeof(qid_rcv));
	assert(ret == 0);
	ret = setsockopt(rfd, IPPROTO_TCP, TCP_QUEUE_SEQ, &migration_info->ack, sizeof(migration_info->ack));
	assert(ret == 0);

	server_sin.sin_family = AF_INET;
	server_sin.sin_port = migration_info->self_port;
	server_sin.sin_addr.s_addr = get_my_osd_addr().sin_addr.s_addr;
	client_sin.sin_family = AF_INET;
	client_sin.sin_port = migration_info->peer_port;
	client_sin.sin_addr.s_addr = migration_info->peer_addr;

	ret = bind(rfd, (struct sockaddr *)&server_sin, sizeof(server_sin));
	assert(ret == 0);
	ret = connect(rfd, (struct sockaddr *)&client_sin, sizeof(client_sin));
	assert(ret == 0);

	socklen_t new_ulen = migration_info->unsentq_len;
	socklen_t new_len = migration_info->sendq.len - new_ulen;

	//zlog_debug(zlog_handoff, "restore send q: new_ulen %d new_len %d recvq len %d", new_ulen, new_len, migration_info->recvq.len);
	if (new_len) {
		ret = restore_queue(rfd, TCP_SEND_QUEUE, (const uint8_t *)migration_info->sendq.data, new_len, 1);
		assert(ret == 0);
	}
	if (new_ulen) {
		ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR, &(int){-1}, sizeof(-1));
		assert(ret == 0);
		ret = restore_queue(rfd, TCP_SEND_QUEUE, (const uint8_t *)migration_info->sendq.data + new_len, new_ulen, 0);
		assert(ret == 0);
		ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR, &(int){1}, sizeof(int));
		assert(ret == 0);
	}
	if (migration_info->recvq.len > 0) {
		ret = restore_queue(rfd, TCP_RECV_QUEUE, (const uint8_t *)migration_info->recvq.data, migration_info->recvq.len, 1);
		assert(ret == 0);
	}

	struct tcp_repair_opt opts[4];
	bzero(opts, sizeof(opts));
	opts[0].opt_code = TCPOPT_SACK_PERM;
	opts[0].opt_val = 0;
	opts[1].opt_code = TCPOPT_WINDOW;
	opts[1].opt_val = migration_info->send_wscale + (migration_info->recv_wscale << 16);
	opts[2].opt_code = TCPOPT_TIMESTAMP;
	opts[2].opt_val = 0;
	opts[3].opt_code = TCPOPT_MSS;
	opts[3].opt_val = migration_info->mss;

	ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR_OPTIONS, opts, sizeof(struct tcp_repair_opt) * 4);
	assert(ret == 0);
	ret = setsockopt(rfd, IPPROTO_TCP, TCP_TIMESTAMP, &migration_info->timestamp, sizeof(migration_info->timestamp));
	assert(ret == 0);

	new_window.snd_wl1 = migration_info->snd_wl1;
	new_window.snd_wnd = migration_info->snd_wnd;
	new_window.max_window = migration_info->max_window;
	new_window.rcv_wnd = migration_info->rev_wnd;
	new_window.rcv_wup = migration_info->rev_wup;

	ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR_WINDOW, &new_window, sizeof(new_window));
	assert(ret == 0);

	if (migration_info->ktlsbuf.len != 0) {
		if (migration_info->ktlsbuf.len != 2 * sizeof(struct tls12_crypto_info_aes_gcm_256)) {
			zlog_fatal(zlog_tls, "incorrect ktlsbuf length (%ld)", migration_info->ktlsbuf.len);
			exit(EXIT_FAILURE);
		}
		struct tls12_crypto_info_aes_gcm_256 *crypto_info_send =
			migration_info->ktlsbuf.data;
		struct tls12_crypto_info_aes_gcm_256 *crypto_info_recv =
			migration_info->ktlsbuf.data + sizeof(struct tls12_crypto_info_aes_gcm_256);
		if (setsockopt(rfd, SOL_TCP, TCP_ULP, "tls", sizeof("tls")) < 0) {
			zlog_fatal(zlog_tls, "set ULP tls fail (%s)", strerror(errno));
			exit(EXIT_FAILURE);
		}
		if (setsockopt(rfd, SOL_TLS, TLS_TX, crypto_info_send,
						sizeof(*crypto_info_send)) < 0) {
			zlog_fatal(zlog_tls, "Couldn't set TLS_TX option (%s)", strerror(errno));
			exit(EXIT_FAILURE);
		}
		if (setsockopt(rfd, SOL_TLS, TLS_RX, crypto_info_recv,
						sizeof(*crypto_info_recv)) < 0) {
			zlog_fatal(zlog_tls, "Couldn't set TLS_RX option (%s)", strerror(errno));
			exit(EXIT_FAILURE);
		}
	}

	struct http_client *client = create_http_client(in_ctx->epoll_fd, rfd);
	snprintf(client->bucket_name, MAX_BUCKET_NAME_SIZE, "%s", migration_info->bucket_name);
	snprintf(client->object_name, MAX_OBJECT_NAME_SIZE, "%s", migration_info->object_name);
	//client->uri_str = realloc(client->uri_str, sizeof(char) * (strlen(migration_info->uri_str) + 1));
	snprintf(client->uri_str, strlen(migration_info->uri_str) + 1, "%s", migration_info->uri_str);
	client->object_size = migration_info->object_size;
	client->method = migration_info->method;
	client->bucket_io_ctx = in_ctx->bucket_io_ctx;
	client->data_io_ctx = in_ctx->data_io_ctx;
	if (migration_info->msg_type == HANDOFF_REQUEST) {
		client->from_migrate = osd_ids[in_ctx->osd_arr_index];
	}
	memcpy(client->client_mac, &(migration_info->peer_mac), sizeof(uint8_t) * 6);

	if (migration_info->ktlsbuf.len) {
		client->tls.is_ssl = true;
		client->tls.is_handshake_done = true;
		client->tls.is_ktls_set = true;
	} else {
		client->tls.is_ssl = false;
	}

	client->client_addr = migration_info->peer_addr;
	client->client_port = migration_info->peer_port;

	/* quiting repair mode */
	ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR, &(int){-1}, sizeof(int));
	assert(ret == 0);

	client->fd = rfd;

	zlog_debug(zlog_handoff, "Deserialized: %s (fd=%d)", client->uri_str, client->fd);

	// we only add client into epoll after originaldone received
	in_ctx->client_for_originaldone = client;
}

int handoff_in_listen(int thread_id)
{
	struct sockaddr_in saddr = {0};
	saddr.sin_family = AF_INET;
	saddr.sin_addr.s_addr = INADDR_ANY;
	saddr.sin_port = htons(HANDOFF_CTRL_PORT + thread_id);

	int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (listen_fd == -1) {
		zlog_error(zlog_handoff, "Failed to create incoming handoff listener socket: %s", strerror(errno));
		return -1;
	}

	if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int))) {
		zlog_error(zlog_handoff, "Failed to setsockopt SO_REUSEADDR: %s", strerror(errno));
		return -1;
	}

	if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &(int){1}, sizeof(int))) {
		zlog_error(zlog_handoff, "Failed to setsockopt SO_REUSEPORT: %s", strerror(errno));
		return -1;
	}

	if (bind(listen_fd, (struct sockaddr *)&saddr, sizeof(saddr)) < 0) {
		zlog_error(zlog_handoff, "Failed to bind socket: %s", strerror(errno));
		return -1;
	}

	if (listen(listen_fd, SOMAXCONN) < 0) {
		zlog_error(zlog_handoff, "Failed to listen (fd=%d): %s", listen_fd, strerror(errno));
		return -1;
	}

	set_socket_non_blocking(listen_fd);

	return listen_fd;
}

// no need to change epoll since we will need to wait for new migration request
// anyways
static void handoff_in_recv_done(struct handoff_in *in_ctx) {

	zlog_debug(zlog_handoff, "HANDOFF_IN Recevied incoming migration message (osd=%d)", osd_ids[in_ctx->osd_arr_index]);

	struct http_client *client = in_ctx->client_for_originaldone;

	struct epoll_event event = {};
	event.data.ptr = client;
	event.events = EPOLLIN | EPOLLRDHUP;
	int ret = epoll_ctl(client->epoll_fd, EPOLL_CTL_ADD, client->fd, &event);
	assert(ret == 0);

	// check if HTTP GET
	char datetime_str[256];
	get_datetime_str(datetime_str, 256);

	init_object_get_request(client);
	complete_get_request(client, datetime_str);

	in_ctx->client_for_originaldone = NULL;
}

// handle handoff request - another end will not send another
// request before current request is been acked
// 1. loop until whole request received
// 2. create a new http client, deserialze s3 client,
// create connect, setup ktls
// 3. change mod to epoll out and send back handoff done
void handoff_in_recv(struct handoff_in *in_ctx, bool *ready_to_send, struct http_client **client_to_handoff_again) {
	*ready_to_send = false;
	*client_to_handoff_again = NULL;

	if (in_ctx->recv_protobuf == NULL) {
		int ret = recv(in_ctx->fd, &in_ctx->recv_protobuf_len, sizeof(in_ctx->recv_protobuf_len), 0);
		if ((ret == 0) || (ret == -1 && errno != EAGAIN)) {
			zlog_error(zlog_handoff, "Fail to receive incoming handoff protobuf message length: %s", strerror(errno));
			handoff_in_disconnect(in_ctx);
			return;
		}
		if (ret == -1 && errno == EAGAIN) {
			return;
		}
		if (ret != sizeof(in_ctx->recv_protobuf_len)) {
			zlog_fatal(zlog_handoff, "unable to handle the case TCP recv not equal to 4 bytes on header");
			exit(EXIT_FAILURE);
		}

		// received original done
		if (in_ctx->recv_protobuf_len == UINT32_MAX) {
			ready_to_send = false; // nothing to send...
			// this can be NULL for handoff_reset or handoff_back_rehandoff
			if (in_ctx->client_for_originaldone != NULL)
				handoff_in_recv_done(in_ctx);
			if (in_ctx->client_to_handoff_again) {
				*client_to_handoff_again = in_ctx->client_to_handoff_again;
				in_ctx->client_to_handoff_again = NULL;
			}
			in_ctx->recv_protobuf_len = 0;
			in_ctx->wait_for_originaldone = false;
			return;
		}

		if (in_ctx->wait_for_originaldone) {
			// we are waiting for original done
			// but we didn't receive it, but a new migration
			zlog_fatal(zlog_handoff, "HANDOFF_IN new migration comes before original server done (osd=%d)",	osd_ids[in_ctx->osd_arr_index]);
			exit(EXIT_FAILURE);
		}

		in_ctx->recv_protobuf_len = ntohl(in_ctx->recv_protobuf_len);
		if (in_ctx->recv_protobuf_len == 0) {
			zlog_fatal(zlog_handoff, "in_ctx->recv_protobuf_len is zero");
			exit(EXIT_FAILURE);
		}
		in_ctx->recv_protobuf = malloc(in_ctx->recv_protobuf_len);
	}

	int ret = recv(in_ctx->fd, in_ctx->recv_protobuf + in_ctx->recv_protobuf_received,
		in_ctx->recv_protobuf_len - in_ctx->recv_protobuf_received, 0);
	if ((ret == 0) || (ret == -1 && errno != EAGAIN)) {
		zlog_error(zlog_handoff, "Fail to receive protobuf message: %s", strerror(errno));
		handoff_in_disconnect(in_ctx);
		return;
	}
	if (ret == -1 && errno == EAGAIN) {
		return;
	}

	SocketSerialize *migration_info = socket_serialize__unpack(NULL,
		in_ctx->recv_protobuf_len, in_ctx->recv_protobuf);

	if (in_ctx->recv_protobuf != NULL) {
		free(in_ctx->recv_protobuf);
		in_ctx->recv_protobuf = NULL;
	}
	in_ctx->recv_protobuf_len = 0;
	in_ctx->recv_protobuf_received = 0;

	if (migration_info->msg_type == HANDOFF_REQUEST) {
		handoff_in_deserialize(in_ctx, migration_info);
		zlog_debug(zlog_handoff, "HANDOFF_IN HANDOFF_REQUEST Deserialize connection");
		// apply src IP modification
		ret = apply_redirection(get_my_osd_addr().sin_addr.s_addr, migration_info->peer_addr,
					migration_info->self_port, migration_info->peer_port,
					migration_info->self_addr, my_mac, migration_info->peer_addr, (uint8_t *)&migration_info->peer_mac,
					migration_info->self_port, migration_info->peer_port, false, false);
		assert(ret == 0);
		zlog_debug(zlog_handoff, "HANDOFF_IN HANDOFF_REQUEST Applied source IP modification");
	} else if (migration_info->msg_type == HANDOFF_BACK_REQUEST) {
		if (migration_info->acting_primary_osd_id == get_my_osd_id()) {
			zlog_debug(zlog_handoff, "HANDOFF_IN HANDOFF_BACK_REQUEST Migration target is current node, deserialize");
			handoff_in_deserialize(in_ctx, migration_info);
		} else {
			zlog_debug(zlog_handoff, "HANDOFF_IN HANDOFF_BACK_REQUEST Migration target is not current node, rehandoff to target (osd=%d)",
				migration_info->acting_primary_osd_id);

			handoff_out_serialize_rehandoff(&in_ctx->client_to_handoff_again, migration_info);
		}
		// we are safe to remove previous redir to fake server there since we
		// either have a working fd or blocked incoming packets
#ifdef USE_TC
		ret = remove_redirection(migration_info->peer_addr, get_my_osd_addr().sin_addr.s_addr,
					migration_info->peer_port, migration_info->self_port);
#else
		ret = remove_redirection_ebpf(migration_info->peer_addr, get_my_osd_addr().sin_addr.s_addr,
					migration_info->peer_port, migration_info->self_port);
#endif
		assert(ret == 0);
		zlog_debug(zlog_handoff, "HANDOFF_IN HANDOFF_BACK_REQUEST Remove redirection rule from client to fake server (osd=%d,client=%" PRIu64":%d,original_server=%" PRIu64":%d)",
					osd_ids[in_ctx->osd_arr_index],
					migration_info->peer_addr, ntohs(migration_info->peer_port),
					migration_info->self_addr, ntohs(migration_info->self_port));

	} else if (migration_info->msg_type == HANDOFF_RESET_REQUEST) {
#ifdef USE_TC
		ret = remove_redirection(migration_info->peer_addr, get_my_osd_addr().sin_addr.s_addr,
					migration_info->peer_port, migration_info->self_port);
#else
		ret = remove_redirection_ebpf(migration_info->peer_addr, get_my_osd_addr().sin_addr.s_addr,
					migration_info->peer_port, migration_info->self_port);
#endif
		assert(ret == 0);
		zlog_debug(zlog_handoff, "HANDOFF_IN HANDOFF_RESET_REQUEST Remove redirection rule from client to fake server (osd=%d,client=%" PRIu64":%d,orignal_server=%" PRIu64":%d)",
				osd_ids[in_ctx->osd_arr_index],
				migration_info->peer_addr, ntohs(migration_info->peer_port),
				migration_info->self_addr, ntohs(migration_info->self_port));
	} else {
		zlog_fatal(zlog_handoff, "Can only handle HANDOFF_REQUEST or HANDOFF_BACK_REQUEST msg");
		exit(EXIT_FAILURE);
	}

	// build response proto_buf
	SocketSerialize migration_info_resp = SOCKET_SERIALIZE__INIT;
	migration_info_resp.msg_type = HANDOFF_DONE;

	migration_info_resp.self_addr = migration_info->self_addr;
	migration_info_resp.peer_addr = migration_info->peer_addr;

	// encode self mac in response for orginal server to perform redirection
	memcpy(&(migration_info_resp.peer_mac), my_mac, sizeof(uint8_t) * 6);

	migration_info_resp.self_port = migration_info->self_port;
	migration_info_resp.peer_port = migration_info->peer_port;

	// no longer need
	socket_serialize__free_unpacked(migration_info, NULL);

	int proto_len = socket_serialize__get_packed_size(&migration_info_resp);
	uint32_t net_proto_len = htonl(proto_len);
	uint8_t *proto_buf = malloc(sizeof(net_proto_len) + proto_len);
	socket_serialize__pack(&migration_info_resp, proto_buf + sizeof(net_proto_len));
	// add length of proto_buf at the begin
	memcpy(proto_buf, &net_proto_len, sizeof(net_proto_len));

	// change epoll to epollout
	in_ctx->send_protobuf = proto_buf;
	in_ctx->send_protobuf_len = sizeof(net_proto_len) + proto_len;
	in_ctx->send_protobuf_sent = 0;
	struct epoll_event event = {0};
	event.data.ptr = in_ctx;
	event.events = EPOLLOUT | EPOLLRDHUP;
	if (epoll_ctl(in_ctx->epoll_fd, EPOLL_CTL_MOD, in_ctx->fd, &event) == -1) {
		zlog_fatal(zlog_handoff, "Failed to change socket fd=%d to send mode: %s", in_ctx->fd, strerror(errno));
		exit(EXIT_FAILURE);
	}

	*ready_to_send = true;
}

void handoff_in_send(struct handoff_in *in_ctx) {
	int ret = send(in_ctx->fd, in_ctx->send_protobuf + in_ctx->send_protobuf_sent,
		in_ctx->send_protobuf_len - in_ctx->send_protobuf_sent, 0);
	if ((ret == 0) || (ret == -1 && errno != EAGAIN)) {
		zlog_fatal(zlog_handoff, "Failed to send protobuf len fd=%d: %s", in_ctx->fd, strerror(errno));
		handoff_in_disconnect(in_ctx);
		return;
	}
	if (ret == -1 && errno == EAGAIN) {
		return;
	}

	in_ctx->send_protobuf_sent += ret;

	if (in_ctx->send_protobuf_sent < in_ctx->send_protobuf_len) {
		zlog_debug(zlog_handoff, "HANDOFF_IN sent response to migration request %d/%d (osd=%d)",
			in_ctx->send_protobuf_sent, in_ctx->send_protobuf_len, osd_ids[in_ctx->osd_arr_index]);
		return;
	}
	zlog_debug(zlog_handoff, "HANDOFF_IN all response to migration request sent (osd=%d)", osd_ids[in_ctx->osd_arr_index]);

	// send done
	if (in_ctx->send_protobuf != NULL) {
		free(in_ctx->send_protobuf);
		in_ctx->send_protobuf = NULL;
	}
	in_ctx->send_protobuf_len = 0;
	in_ctx->send_protobuf_sent = 0;
	in_ctx->wait_for_originaldone = true;
	struct epoll_event event = {0};
	event.data.ptr = in_ctx;
	event.events = EPOLLIN | EPOLLRDHUP;
	if (epoll_ctl(in_ctx->epoll_fd, EPOLL_CTL_MOD, in_ctx->fd, &event) == -1) {
		zlog_fatal(zlog_handoff, "Fail to set socket back to receive mode (fd=%d): %s", in_ctx->fd, strerror(errno));
		exit(EXIT_FAILURE);
	}
}

void handoff_in_disconnect(struct handoff_in *in_ctx)
{
	zlog_info(zlog_handoff, "Disconnecting incoming handoff control socket (fd=%d)", in_ctx->fd);
	close(in_ctx->fd);
	in_ctx->fd = 0;
}
