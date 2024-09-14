#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <assert.h>
#include "queue.h"
#include "forward.h"

zlog_category_t *zlog_queue;

#include <time.h>
// https://stackoverflow.com/questions/68804469/subtract-two-timespec-objects-find-difference-in-time-or-duration
static double diff_timespec(const struct timespec *time1, const struct timespec *time0)
{
	return (time1->tv_sec - time0->tv_sec) + (time1->tv_nsec - time0->tv_nsec) / 1000000000.0;
}

void rule_queue_init(rule_queue_t *q, int q_size)
{
	q->size= q_size;
	q->count = 0;
	q->head = 0;
	q->tail = 0;
	q->buffer = (rule_args_t *)malloc(q->size * sizeof(struct rule_args));
	pthread_mutex_init(&q->lock, NULL);
	pthread_cond_init(&q->not_empty, NULL);
	pthread_cond_init(&q->not_full, NULL);
}

void rule_queue_destroy(rule_queue_t *q)
{
	free(q->buffer);
	pthread_mutex_destroy(&q->lock);
	pthread_cond_destroy(&q->not_empty);
	pthread_cond_destroy(&q->not_full);
}

bool rule_enqueue(rule_queue_t *q, rule_args_t *arg)
{
//clock_gettime(CLOCK_MONOTONIC, &start_time);
	pthread_mutex_lock(&q->lock);
	if (q->count == q->size)
	{
		pthread_mutex_unlock(&q->lock);
		return false;
	}
	else
	{
		memcpy(&q->buffer[q->tail], arg, sizeof(rule_args_t));
		q->tail = (q->tail + 1) % q->size;
		q->count++;
		pthread_cond_signal(&q->not_empty);
		zlog_debug(zlog_queue, "q->tail %d, q->count %d", q->tail, q->count);
		pthread_mutex_unlock(&q->lock);
		//clock_gettime(CLOCK_MONOTONIC, &end_time);
		//zlog_debug(zlog_queue, "between lock and unlock the queue: %.9lf", diff_timespec(&end_time, &start_time));
		return true;
	}
}

bool rule_in_queue(uint32_t src_ip, uint32_t dst_ip, uint16_t src_port, uint16_t dst_port, rule_queue_t *q)
{
	pthread_mutex_lock(&q->lock);
	for (int i = 0; i < q->count; i++) {
		if (q->buffer[i].src_ip == src_ip && q->buffer[i].dst_ip == dst_ip &&
			q->buffer[i].src_port == src_port && q->buffer[i].dst_port == dst_port) {

			q->buffer[i].skip = true;
			zlog_debug(zlog_queue, "Marking flow (%d) to skip insertion", ntohs(q->buffer[i].src_port));
			pthread_mutex_unlock(&q->lock);
			return true;
		}
	}
	pthread_mutex_unlock(&q->lock);
	return false;
}

void *rule_q_consumer(void *queue)
{
	zlog_info(zlog_queue, "TC-worker queue started");
	rule_queue_t *q = (rule_queue_t *)queue;

	for (;;)
	{
		rule_args_t arg;
		pthread_mutex_lock(&q->lock);
		while (q->count == 0)
		{
			pthread_cond_wait(&q->not_empty, &q->lock);
		}

		memcpy(&arg, &(q->buffer[q->head]), sizeof(rule_args_t));
		q->head = (q->head + 1) % q->size;
		q->count--;
		pthread_cond_signal(&q->not_full);
		pthread_mutex_unlock(&q->lock);

		if (arg.skip)
		{
			zlog_debug(zlog_queue, "skipped, client port %d", arg.src_port);
			continue;
		}
		else
		{
			int ret = apply_redirection(
				arg.src_ip, arg.dst_ip,
				arg.src_port, arg.dst_port,
				arg.new_src_ip, arg.new_src_mac,
				arg.new_dst_ip, arg.new_dst_mac,
				arg.new_src_port, arg.new_dst_port,
				arg.block, arg.hw_offload);
			assert(ret == 0);
			ret = remove_redirection_ebpf(arg.src_ip, arg.dst_ip,
						arg.src_port, arg.dst_port);
			assert(ret == 0);
			zlog_debug(zlog_queue, "Applied redirection rule (%d,%d)", ntohs(arg.src_port), ntohs(arg.dst_port));
		}

//		switch (arg.op)
//		{
//			case RULE_INSERT:
//				ast(apply_redirection_str(
//					arg.src_ip, arg.dst_ip,
//					arg.src_port, arg.dst_port,
//					arg.new_src_ip, arg.new_src_mac,
//					arg.new_dst_ip, arg.new_dst_mac,
//					arg.new_src_port, arg.new_dst_port,
//					arg.block, arg.hw_offload) == 0, "apply_redirection_str", NULL);
////printf("inserted\n");
//				break;
//			case RULE_REMOVE:
//				remove_redirection_str(
//					arg.src_ip, arg.dst_ip,
//					arg.src_port, arg.dst_port);
////printf("removed\n");
//				break;
//		}
	}

	zlog_debug(zlog_queue, "Stopping TC-rule consumer thread");
	return NULL;
}
