/* for repqair */
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/ioctl.h>
#include <linux/sockios.h>
#include <errno.h>
#include <assert.h>
#include <sys/epoll.h>
/* for repqair */

#include "conn_migration.h"

#define USE_TFM 1
#define TFM_DESC 1
#define TLS_REEXPORTABLE 1
#define WITH_KTLS 1

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
/* for repair */


/* for repair */
struct tcp_info_sub {
        uint8_t tcpi_state;
        uint8_t tcpi_ca_state;
        uint8_t tcpi_retransmits;
        uint8_t tcpi_probes;
        uint8_t tcpi_backoff;
        uint8_t tcpi_options;
        uint8_t tcpi_snd_wscale : 4;
        uint8_t tcpi_rcv_wscale : 4;
} info;


static void
reuseaddr(int fd)
{
	int ret;
	ret = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));
	assert(ret == 0);
	ret = setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &(int){1}, sizeof(int));
	assert(ret == 0);
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

void conn_migration(struct http_client *client, int target_osd_id)
{
	/* to serialize */
	int ret;
	struct sockaddr_in sin = {.sin_family = AF_INET};
	struct sockaddr_in sin2;
	int sndq_len, unsndq_len, rcvq_len;
	uint32_t mss, ts;
	socklen_t olen_mss = sizeof(mss);
	socklen_t olen_ts = sizeof(ts);
	struct tcp_repair_window window;
	uint32_t seqno_send, seqno_recv;
	const int qid_snd = TCP_SEND_QUEUE;
	const int qid_rcv = TCP_RECV_QUEUE;
	socklen_t ulen;
	char *sndbuf, *rcvbuf;
	const int peek = MSG_PEEK | MSG_DONTWAIT;
	struct tcp_repair_opt opts[4];
	const int dopt = -1;
	const int on = 1;
	socklen_t slen;
	ssize_t len;

	ret = setsockopt(client->fd, IPPROTO_TCP, TCP_REPAIR, &on, sizeof(on));
	assert(ret == 0);
	slen = sizeof(info);
	ret = getsockopt(client->fd, IPPROTO_TCP, TCP_INFO, &info, &slen);
	assert(ret == 0);
	assert(info.tcpi_state == TCP_ESTABLISHED);
	ret = ioctl(client->fd, SIOCOUTQ, &sndq_len);
	assert(ret == 0);
	ret = ioctl(client->fd, SIOCOUTQNSD, &unsndq_len);
	assert(ret == 0);
	ret = ioctl(client->fd, SIOCINQ, &rcvq_len);
	assert(ret == 0);

	ret = getsockopt(client->fd, IPPROTO_TCP, TCP_MAXSEG, &mss, &olen_mss);
	assert(ret == 0);
	ret = getsockopt(client->fd, IPPROTO_TCP, TCP_TIMESTAMP, &ts, &olen_ts);
	assert(ret == 0);
	/* window scale in info */

	slen = sizeof(window);
	ret = getsockopt(client->fd, IPPROTO_TCP, TCP_REPAIR_WINDOW, &window, &slen);
	assert(ret == 0);

	bzero(&sin, sizeof(sin));
	sin.sin_family = AF_INET;
	slen = sizeof(sin);
	ret = getsockname(client->fd, (struct sockaddr *)&sin, &slen);
	assert(ret == 0);
	bzero(&sin2, sizeof(sin2));
	sin2.sin_family = AF_INET;
	slen = sizeof(sin2);
	ret = getpeername(client->fd, (struct sockaddr *)&sin2, &slen);
	assert(ret == 0);

	ret = setsockopt(client->fd, IPPROTO_TCP, TCP_REPAIR_QUEUE, &qid_snd, sizeof(qid_snd));
	assert(ret == 0);
	slen = sizeof(seqno_send);
	ret = getsockopt(client->fd, IPPROTO_TCP, TCP_QUEUE_SEQ, &seqno_send, &slen);
	assert(ret == 0);
	if (sndq_len) {
		sndbuf = calloc(1, sndq_len + 1);
		assert(sndbuf != NULL);
		ret = recv(client->fd, sndbuf, sndq_len + 1, peek);
		assert(ret == sndqlen);
	}

	ret = setsockopt(client->fd, IPPROTO_TCP, TCP_REPAIR_QUEUE, &qid_rcv, sizeof(qid_rcv));
	assert(ret == 0);
	slen = sizeof(seqno_recv);
	ret = getsockopt(client->fd, IPPROTO_TCP, TCP_QUEUE_SEQ, &seqno_recv, &slen);
	assert(ret == 0);
	if (rcvq_len) {
		rcvbuf = calloc(1, rcvq_len + 1);
		assert(recvbuf != NULL);
		ret = recv(client->fd, rcvbuf, rcvq_len + 1, peek);
		assert(ret == rcvq_len);
	}

	ret = epoll_ctl(client->epoll_fd, EPOLL_CTL_DEL, client->fd, NULL);
	assert(ret == 0);
	close(client->fd);
	printf("Serialized fd=%d\n", client->fd);
	/* done with serialize */

	/* start to deserialize */
	client->fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	assert (client->fd > 0);
	reuseaddr(client->fd);

	ret = setsockopt(client->fd, IPPROTO_TCP, TCP_REPAIR, &on, sizeof(on));
	assert(ret == 0);

	ret = setsockopt(client->fd, IPPROTO_TCP, TCP_REPAIR_QUEUE, &qid_snd, sizeof(qid_snd));
	assert(ret == 0);
	ret = setsockopt(client->fd, IPPROTO_TCP, TCP_QUEUE_SEQ, &seqno_send, sizeof(seqno_send));
	assert(ret == 0);
	ret = setsockopt(client->fd, IPPROTO_TCP, TCP_REPAIR_QUEUE, &qid_rcv, sizeof(qid_rcv));
	assert(ret == 0);
	ret = setsockopt(client->fd, IPPROTO_TCP, TCP_QUEUE_SEQ, &seqno_recv, sizeof(seqno_recv));
	assert(ret == 0);
	ret = bind(client->fd, (struct sockaddr *)&sin, sizeof(sin));
	assert(ret == 0);
	ret = connect(client->fd, (struct sockaddr *)&sin2, sizeof(sin2));
	assert(ret == 0);

	ulen = unsndq_len;
	len = sndq_len - ulen;
	if (len) {
		ret = restore_queue(client->fd, TCP_SEND_QUEUE, (const uint8_t *)sndbuf, len, 1);
		assert(ret == 0);
	}
	if (ulen) {
		ret = setsockopt(client->fd, IPPROTO_TCP, TCP_REPAIR, &dopt, sizeof(dopt));
		assert(ret == 0);
		ret = restore_queue(client->fd, TCP_SEND_QUEUE, (const uint8_t *)sndbuf + len, ulen, 0);
		assert(ret == 0);
		ret = setsockopt(client->fd, IPPROTO_TCP, TCP_REPAIR, &on, sizeof(on));
		assert(ret == 0);

	}
	if (rcvq_len > 0) {
		ret = restore_queue(client->fd, TCP_RECV_QUEUE, (const uint8_t *)rcvbuf, rcvq_len, 1);
		assert(ret == 0);
	}

	bzero(opts, sizeof(opts));
        opts[0].opt_code = TCPOPT_SACK_PERM;
        opts[0].opt_val = 0;
        opts[1].opt_code = TCPOPT_WINDOW;
        opts[1].opt_val = info.tcpi_snd_wscale + (info.tcpi_rcv_wscale << 16);
        opts[2].opt_code = TCPOPT_TIMESTAMP;
        opts[2].opt_val = 0;
        opts[3].opt_code = TCPOPT_MSS;
        opts[3].opt_val = mss;

	ret = setsockopt(client->fd, IPPROTO_TCP, TCP_REPAIR_OPTIONS, opts, sizeof(struct tcp_repair_opt) * 4);
	assert(ret == 0);
	ret = setsockopt(client->fd, IPPROTO_TCP, TCP_TIMESTAMP, &ts, sizeof(ts));
	assert(ret == 0);
	ret = setsockopt(client->fd, IPPROTO_TCP, TCP_REPAIR_WINDOW, &window, sizeof(window));
	assert(ret == 0);
	ret = setsockopt(client->fd, IPPROTO_TCP, TCP_REPAIR, &dopt, sizeof(dopt));
	assert(ret == 0);
	printf("Deserialized fd=%d\n", client->fd);
	/* to serialize */

	struct epoll_event event = {};
	event.data.ptr = client;

	event.events = EPOLLIN;
	ret = epoll_ctl(client->epoll_fd, EPOLL_CTL_ADD, client->fd, &event);
	assert(ret == 0);

	printf("added back to epoll fd=%d\n", client->fd);

}
