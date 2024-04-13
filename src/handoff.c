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

#include "proto/socket_serialize.pb-c.h"

#include "handoff.h"
#include "object_store.h"
#include "http_client.h"
#include "osd_mapping.h"
#include "forward.h"

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
       handoff_out_issue     |     handoff_in_recv
               |             |            |
               |             |            |
               v             |            v
     handoff_out_serialize   |  handoff_in_deserialize
               |             |            |
               |             |            |
               v             |            v
       handoff_out_send------+     handoff_in_send
                                          |
               +--------------------------+
               v
       handoff_out_recv
               |
               |
               v
    handoff_out_apply_rule

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
    struct handoff_out_req* node = queue->front;
	*client = node->c;

    queue->front = queue->front->next;
    // If front becomes NULL, then change rear also as NULL
    if (queue->front == NULL) {
        queue->rear = NULL;
    }

    free(node);
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

static void handoff_out_serialize(struct http_client *client)
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

	struct sockaddr_in peer_sin;
	bzero(&peer_sin, sizeof(peer_sin));
	peer_sin.sin_family = AF_INET;
	slen = sizeof(peer_sin);
	ret = getpeername(fd, (struct sockaddr *)&peer_sin, &slen);
	assert(ret == 0);

	// apply blocking
	ret = apply_redirection_ebpf(peer_sin.sin_addr.s_addr, self_sin.sin_addr.s_addr,
				peer_sin.sin_port, self_sin.sin_port,
				peer_sin.sin_addr.s_addr, client->client_mac, self_sin.sin_addr.s_addr, my_mac,
				peer_sin.sin_port, self_sin.sin_port, true);
	assert(ret == 0);

	ret = setsockopt(fd, IPPROTO_TCP, TCP_REPAIR, &(int){1}, sizeof(int));
	assert(ret == 0);

// TODO: ktls export
#ifdef WITH_KTLS
	tls_unmake_ktls(fd_state[fd].tls_context, fd);
	assert(ret == 0);
#endif /* WITH_KTLS */

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
			fprintf(stderr, "Couldn't get TLS_TX option (%s)\n", strerror(errno));
			exit(EXIT_FAILURE);
		}

		optlen = sizeof(struct tls12_crypto_info_aes_gcm_256);
		if (getsockopt(fd, SOL_TLS, TLS_RX, crypto_info_recv, &optlen)) {
			fprintf(stderr, "Couldn't get TLS_RX option (%s)\n", strerror(errno));
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
	migration_info.peer_addr = peer_sin.sin_addr.s_addr;
	migration_info.peer_port = peer_sin.sin_port;
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

	memcpy(&(migration_info.peer_mac), client->client_mac, sizeof(uint8_t) * 6);

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
	printf("Thread %d HANDOFF_OUT try connect to osd id %d (ip %s, port %d)\n",
		out_ctx->thread_id, osd_ids[out_ctx->osd_arr_index],
		osd_addr_strs[out_ctx->osd_arr_index],
		ntohs(osd_addrs[out_ctx->osd_arr_index].sin_port));

	out_ctx->fd = socket(AF_INET, SOCK_STREAM, 0);
	if (out_ctx->fd == -1) {
		perror("socket");
		exit(EXIT_FAILURE);
	}

	if (setsockopt(out_ctx->fd, IPPROTO_TCP, TCP_QUICKACK, &(int){1}, sizeof(int)) == -1) {
		perror("setsockopt TCP_QUICKACK");
		exit(EXIT_FAILURE);
	}

	if (setsockopt(out_ctx->fd, IPPROTO_TCP, TCP_NODELAY, &(int){1}, sizeof(int)) == -1) {
		perror("setsockopt TCP_NODELAY");
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
	out_ctx->reconnect_count++;
	if (out_ctx->reconnect_count > MAX_HANDOFF_OUT_RECONNECT) {
		fprintf(stderr, "Thread %d HANDOFF_OUT try RE-connect too many times (osd id %d, ip %s, port %d, reconnect count %d)\n",
			out_ctx->thread_id, osd_ids[out_ctx->osd_arr_index],
			osd_addr_strs[out_ctx->osd_arr_index],
			ntohs(osd_addrs[out_ctx->osd_arr_index].sin_port),
			out_ctx->reconnect_count);
		exit(EXIT_FAILURE);
	}

	printf("Thread %d HANDOFF_OUT try RE-connect (osd id %d, ip %s, port %d, reconnect count %d)\n",
		out_ctx->thread_id, osd_ids[out_ctx->osd_arr_index],
		osd_addr_strs[out_ctx->osd_arr_index],
		ntohs(osd_addrs[out_ctx->osd_arr_index].sin_port),
		out_ctx->reconnect_count);

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
	struct handoff_out *out_ctx, int osd_arr_index, int thread_id)
{
	out_ctx->osd_arr_index = osd_arr_index;
	out_ctx->thread_id = thread_id;

	// serialize state, we don't delete client until handoff_out is complete
	handoff_out_serialize(client);
	printf("Thread %d HANDOFF_OUT serialized client on conn %d\n", thread_id, client->fd);

	// enqueue this handoff request
	if (!out_ctx->queue) {
		out_ctx->queue = handoff_out_queue_create();
	}
	handoff_out_enqueue(out_ctx->queue, client);
	printf("Thread %d HANDOFF_OUT enqueued migration work to osd id %d for s3 client conn %d\n",
		thread_id, osd_ids[out_ctx->osd_arr_index], client->fd);

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
				exit(EXIT_FAILURE);
			}
			out_ctx->is_fd_in_epoll = false;
			return;
		}
		handoff_out_dequeue(out_ctx->queue, (void **)&out_ctx->client);
	}

	printf("Thread %d HANDOFF_OUT dequeued migration work to osd id %d for s3 client conn %d, now start send\n",
		out_ctx->thread_id, out_ctx->client->to_migrate, out_ctx->client->fd);

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

	out_ctx->reconnect_count = 0;
	out_ctx->client->proto_buf_sent += ret;

	// send done
	if (out_ctx->client->proto_buf_len == out_ctx->client->proto_buf_sent) {
		printf("Thread %d HANDOFF_OUT migration request to osd id %d for s3 client conn %d sent out, wait for handoff_done response\n",
			out_ctx->thread_id, out_ctx->client->to_migrate, out_ctx->client->fd);

		if (out_ctx->recv_protobuf != NULL) {
			free(out_ctx->recv_protobuf);
		}
		out_ctx->recv_protobuf_len = 0;
		out_ctx->recv_protobuf_received = 0;
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
	if (out_ctx->recv_protobuf == NULL) {
		int ret = recv(out_ctx->fd, &out_ctx->recv_protobuf_len, sizeof(out_ctx->recv_protobuf_len), 0);
		printf("ret = %d received recv protobuf len %d\n", ret, out_ctx->recv_protobuf_len);
		if ((ret == 0) || (ret == -1 && errno != EAGAIN)) {
			perror("handoff_out_recv recv1");
			handoff_out_reconnect(out_ctx);
			return;
		}
		if (ret == -1 && errno == EAGAIN) {
			return;
		}
		printf("before recv protobuf len %d\n", out_ctx->recv_protobuf_len);
		if (ret != sizeof(out_ctx->recv_protobuf_len)) {
			fprintf(stderr, "%s: unable to handle the case TCP recv not equal to 4 bytes on header\n", __func__);
			exit(EXIT_FAILURE);
		}
		out_ctx->recv_protobuf_len = ntohl(out_ctx->recv_protobuf_len);
		printf("after recv protobuf len %d\n", out_ctx->recv_protobuf_len);
		if (out_ctx->recv_protobuf_len == 0) {
			fprintf(stderr, "%s: out_ctx->recv_protobuf_len is zero\n", __func__);
			exit(EXIT_FAILURE);
		}
		out_ctx->recv_protobuf = malloc(out_ctx->recv_protobuf_len);
	}

	int ret = recv(out_ctx->fd, out_ctx->recv_protobuf + out_ctx->recv_protobuf_received,
		out_ctx->recv_protobuf_len - out_ctx->recv_protobuf_received, 0);
	if ((ret == 0) || (ret == -1 && errno != EAGAIN)) {
		perror("handoff_out_recv recv2");
		handoff_out_reconnect(out_ctx);
		return;
	}
	if (ret == -1 && errno == EAGAIN) {
		return;
	}

	out_ctx->reconnect_count = 0;
	out_ctx->recv_protobuf_received += ret;
	if (out_ctx->recv_protobuf_received < out_ctx->recv_protobuf_len)
		return;

	printf("Thread %d HANDOFF_OUT migration request to osd id %d for s3 client conn %d handoff_done response received\n",
		out_ctx->thread_id, out_ctx->client->to_migrate, out_ctx->client->fd);

	SocketSerialize *migration_info = socket_serialize__unpack(NULL,
		out_ctx->recv_protobuf_len, out_ctx->recv_protobuf);
	if (migration_info == NULL) {
		fprintf(stderr, "%s: unable to unpack recv protobuf\n", __func__);
		exit(EXIT_FAILURE);
	}
	if (migration_info->msg_type != HANDOFF_DONE) {
		fprintf(stderr, "%s: can only handle HANDOFF_DONE msg\n", __func__);
		exit(EXIT_FAILURE);
	}

	free(out_ctx->recv_protobuf);
	out_ctx->recv_protobuf = NULL;
	out_ctx->recv_protobuf_len = 0;
	out_ctx->recv_protobuf_received = 0;

	// TODO!!!: insert rule
	printf("Thread %d HANDOFF_OUT migration request to osd id %d for s3 client conn %d insert redir rule\n",
		out_ctx->thread_id, out_ctx->client->to_migrate, out_ctx->client->fd);

	free_http_client(out_ctx->client);
	out_ctx->client = NULL;
	socket_serialize__free_unpacked(migration_info, NULL);

	struct epoll_event event = {0};
	event.data.ptr = out_ctx;
	event.events = EPOLLOUT;
	if (epoll_ctl(out_ctx->epoll_fd, EPOLL_CTL_MOD, out_ctx->fd, &event) == -1) {
		perror("epoll_ctl");
		close(out_ctx->fd);
		exit(EXIT_FAILURE);
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
			fprintf(stderr, "incorrect ktlsbuf length (%ld)", migration_info->ktlsbuf.len);
			exit(EXIT_FAILURE);
		}

		struct tls12_crypto_info_aes_gcm_256 *crypto_info_send =
			migration_info->ktlsbuf.data;
		struct tls12_crypto_info_aes_gcm_256 *crypto_info_recv =
			migration_info->ktlsbuf.data + sizeof(struct tls12_crypto_info_aes_gcm_256);

		if (setsockopt(rfd, SOL_TCP, TCP_ULP, "tls", sizeof("tls")) < 0) {
			fprintf(stderr, "set ULP tls fail (%s)\n", strerror(errno));
			exit(EXIT_FAILURE);
		}

		if (setsockopt(rfd, SOL_TLS, TLS_TX, crypto_info_send,
						sizeof(*crypto_info_send)) < 0) {
			fprintf(stderr, "Couldn't set TLS_TX option (%s)\n", strerror(errno));
			exit(EXIT_FAILURE);
		}

		if (setsockopt(rfd, SOL_TLS, TLS_RX, crypto_info_recv,
						sizeof(*crypto_info_recv)) < 0) {
			fprintf(stderr, "Couldn't set TLS_RX option (%s)\n", strerror(errno));
			exit(EXIT_FAILURE);
		}
	}

	struct http_client *client = create_http_client(in_ctx->epoll_fd, rfd);
	snprintf(client->bucket_name, MAX_BUCKET_NAME_SIZE, "%s", migration_info->bucket_name);
	snprintf(client->object_name, MAX_OBJECT_NAME_SIZE, "%s", migration_info->object_name);
	client->uri_str = realloc(client->uri_str, sizeof(char) * (strlen(migration_info->uri_str) + 1));
	snprintf(client->uri_str, strlen(migration_info->uri_str) + 1, "%s", migration_info->uri_str);
	client->object_size = migration_info->object_size;
	client->method = migration_info->method;
	client->bucket_io_ctx = in_ctx->bucket_io_ctx;
	client->data_io_ctx = in_ctx->data_io_ctx;
	client->from_migrate = true;

	if (migration_info->ktlsbuf.len) {
		client->tls.is_ssl = true;
		client->tls.is_handshake_done = true;
		client->tls.is_ktls_set = true;
	} else {
		client->tls.is_ssl = false;
	}

	// Extract the MAC address from the reply
	memcpy(client->client_mac, &(migration_info->peer_mac), sizeof(uint8_t) * 6);
	printf("migration peer mac is ");
	print_mac_address(client->client_mac);

	printf("deserialized: %s\n", client->uri_str);

	// apply src IP modification
	ret = apply_redirection(get_my_osd_addr().sin_addr.s_addr, migration_info->peer_addr,
				migration_info->self_port, migration_info->peer_port,
				migration_info->self_addr, my_mac, migration_info->peer_addr, client->client_mac,
				migration_info->self_port, migration_info->peer_port, false, false);
	assert(ret == 0);

	/* quiting repair mode */
	ret = setsockopt(rfd, IPPROTO_TCP, TCP_REPAIR, &(int){-1}, sizeof(int));
	assert(ret == 0);
	struct epoll_event event = {};
	client->fd = rfd;
	event.data.ptr = client;
	event.events = EPOLLIN;
	ret = epoll_ctl(client->epoll_fd, EPOLL_CTL_ADD, client->fd, &event);
	assert(ret == 0);

	// check if HTTP GET
        char datetime_str[256];
        get_datetime_str(datetime_str, 256);

	init_object_get_request(client);
	complete_get_request(client, datetime_str);
}

void handoff_in_disconnect(struct handoff_in *in_ctx)
{
	printf("Thread %d HANDOFF_IN disconnect conn %d (epoll_fd %d, osd arr index %d, osd id %d)\n",
		in_ctx->thread_id, in_ctx->fd, in_ctx->epoll_fd, in_ctx->osd_arr_index, osd_ids[in_ctx->osd_arr_index]);
	// main thread will handle actual close and new accept comein
	if (epoll_ctl(in_ctx->epoll_fd, EPOLL_CTL_DEL, in_ctx->fd, NULL) == -1) {
		// maybe already closed by main
		if (errno != EBADF) {
			perror("epoll_ctl");
			exit(EXIT_FAILURE);
		}
	}
	in_ctx->fd = 0;
}

// handle handoff request - another end will not send another
// request before current request is been acked
// 1. loop until whole request received
// 2. create a new http client, deserialze s3 client,
// create connect, setup ktls
// 3. change mod to epoll out and send back handoff done
void handoff_in_recv(struct handoff_in *in_ctx) {
	if (in_ctx->recv_protobuf == NULL) {
		int ret = recv(in_ctx->fd, &in_ctx->recv_protobuf_len, sizeof(in_ctx->recv_protobuf_len), 0);
		if ((ret == 0) || (ret == -1 && errno != EAGAIN)) {
			perror("handoff_in_recv recv");
			handoff_in_disconnect(in_ctx);
			return;
		}
		if (ret == -1 && errno == EAGAIN) {
			return;
		}
		if (ret != sizeof(in_ctx->recv_protobuf_len)) {
			fprintf(stderr, "%s: unable to handle the case TCP recv not equal to 4 bytes on header\n", __func__);
			exit(EXIT_FAILURE);
		}
		in_ctx->recv_protobuf_len = ntohl(in_ctx->recv_protobuf_len);
		if (in_ctx->recv_protobuf_len == 0) {
			fprintf(stderr, "%s: in_ctx->recv_protobuf_len is zero\n", __func__);
			exit(EXIT_FAILURE);
		}
		in_ctx->recv_protobuf = malloc(in_ctx->recv_protobuf_len);
	}

	int ret = recv(in_ctx->fd, in_ctx->recv_protobuf + in_ctx->recv_protobuf_received,
		in_ctx->recv_protobuf_len - in_ctx->recv_protobuf_received, 0);
	if ((ret == 0) || (ret == -1 && errno != EAGAIN)) {
		perror("handoff_in_recv recv");
		handoff_in_disconnect(in_ctx);
		return;
	}
	if (ret == -1 && errno == EAGAIN) {
		return;
	}

	SocketSerialize *migration_info = socket_serialize__unpack(NULL,
		in_ctx->recv_protobuf_len, in_ctx->recv_protobuf);
	if (migration_info == NULL) {
		fprintf(stderr, "%s: unable to unpack recv protobuf\n", __func__);
		exit(EXIT_FAILURE);
	}
	if (migration_info->msg_type != HANDOFF_REQUEST) {
		fprintf(stderr, "%s: can only handle HANDOFF_REQUEST msg\n", __func__);
		exit(EXIT_FAILURE);
	}

	if (in_ctx->recv_protobuf != NULL) {
		free(in_ctx->recv_protobuf);
		in_ctx->recv_protobuf = NULL;
	}
	in_ctx->recv_protobuf_len = 0;
	in_ctx->recv_protobuf_received = 0;

	// do deserialize
	handoff_in_deserialize(in_ctx, migration_info);

	// build response proto_buf
	SocketSerialize migration_info_resp = SOCKET_SERIALIZE__INIT;
	migration_info_resp.msg_type = HANDOFF_DONE;
	migration_info_resp.self_addr = migration_info->self_addr;
	migration_info_resp.peer_addr = migration_info->peer_addr;
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
	hexdump("protobuf packed", proto_buf, sizeof(net_proto_len) + proto_len);

	// change epoll to epollout
	in_ctx->send_protobuf = proto_buf;
	in_ctx->send_protobuf_len = sizeof(net_proto_len) + proto_len;
	in_ctx->send_protobuf_sent = 0;
	struct epoll_event event = {0};
	event.data.ptr = in_ctx;
	event.events = EPOLLOUT;
	if (epoll_ctl(in_ctx->epoll_fd, EPOLL_CTL_MOD, in_ctx->fd, &event) == -1) {
		perror("epoll_ctl");
		exit(EXIT_FAILURE);
	}
}

void handoff_in_send(struct handoff_in *in_ctx) {
	int ret = send(in_ctx->fd, in_ctx->send_protobuf + in_ctx->send_protobuf_sent,
		in_ctx->send_protobuf_len - in_ctx->send_protobuf_sent, 0);
	if ((ret == 0) || (ret == -1 && errno != EAGAIN)) {
		perror("handoff_in_send send");
		handoff_in_disconnect(in_ctx);
		return;
	}
	if (ret == -1 && errno == EAGAIN) {
		return;
	}

	in_ctx->send_protobuf_sent += ret;

	// send done
	if (in_ctx->send_protobuf_sent == in_ctx->send_protobuf_len) {
		if (in_ctx->send_protobuf != NULL) {
			free(in_ctx->send_protobuf);
			in_ctx->send_protobuf = NULL;
		}
		in_ctx->send_protobuf_len = 0;
		in_ctx->send_protobuf_sent = 0;
		struct epoll_event event = {0};
		event.data.ptr = in_ctx;
		event.events = EPOLLIN;
		if (epoll_ctl(in_ctx->epoll_fd, EPOLL_CTL_MOD, in_ctx->fd, &event) == -1) {
			perror("epoll_ctl");
			exit(EXIT_FAILURE);
		}
	}
}
