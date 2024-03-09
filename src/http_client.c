#include <assert.h>
#include <time.h>

#include <sys/uio.h>

#include "http_client.h"
#include "object_store.h"

rados_t cluster;
_Thread_local rados_ioctx_t bucket_io_ctx;
_Thread_local rados_ioctx_t data_io_ctx;

__thread struct http_client *http_clients[MAX_HTTP_CLIENTS] = { NULL };
size_t BUF_SIZE = sizeof(char) * 1024;
char *random_buffer;

void reset_http_client(struct http_client *client)
{
	for (size_t i = 0; i < client->num_fields; i++) {
		free(client->header_fields[i]);
		free(client->header_values[i]);
	}

	uriFreeUriMembersA(&(client->uri));
	client->num_fields = 0;
	client->expect = NONE;

	if (client->uri_str) free(client->uri_str);
	client->uri_str = NULL;

	if (client->object_name) free(client->object_name);
	client->object_name = NULL;

	if (client->bucket_name) free(client->bucket_name);
	client->bucket_name = NULL;

	if (client->response) free(client->response);
	client->response = NULL;
	client->response_size = 0;

	if (client->data_payload) free(client->data_payload);
	client->data_payload = NULL;
	client->data_payload_size = 0;

	client->bucket_name = NULL;
	client->chunked_upload = false;
	client->object_size = 0;
	client->object_offset = 0;
	client->http_payload_size = 0;
	client->parsing = false;
	client->deleting = false;

	client->current_chunk_size = 0;
	client->current_chunk_offset = 0;
}

void free_http_client(struct http_client *client)
{
	llhttp_finish(&(client->parser));
	reset_http_client(client);
	free(client);
}

int on_header_field_cb(llhttp_t *parser, const char *at, size_t length)
{
	struct http_client *client = (struct http_client*)parser->data;
	client->header_fields[client->num_fields] = strndup(at, length);
	return 0;
}

int on_header_value_cb(llhttp_t *parser, const char *at, size_t length)
{
	struct http_client *client = (struct http_client*)parser->data;
	client->header_values[client->num_fields] = strndup(at, length);

	if (strncmp(at, "100-continue", length) == 0) {
		client->expect = CONTINUE;
	}

	client->num_fields++;

	return 0;
}

int on_body_cb(llhttp_t *parser, const char *at, size_t length)
{
	struct http_client *client = (struct http_client*)parser->data;
	if (client->method == HTTP_PUT) {
		//printf("on body, turn on parsing, at(%ld): %.88s\n", length, at);
		//client->parsing = true;
		struct timespec t0, t1;
		clock_gettime(CLOCK_MONOTONIC, &t0);
		put_object(client, at, length);
		clock_gettime(CLOCK_MONOTONIC, &t1);
		//printf("put_object:\t\t%f s\n", elapsed_time(t1, t0));
	}
	else if (client->method == HTTP_POST) {
		if (client->deleting == true) {
			delete_objects(client, at, length);
		}

	}
	return 0;
}

int on_url_cb(llhttp_t *parser, const char *at, size_t length)
{
	const char * errorPos;
	struct http_client *client = (struct http_client*)parser->data;
	if (client->uri_str) { free(client->uri_str); client->uri_str = NULL; }
	client->uri_str = strndup(at, length);

	if (uriParseSingleUriA(&(client->uri), client->uri_str, &errorPos) != URI_SUCCESS) {
		fprintf(stderr, "Parse uri fail: %s\n", errorPos);
		return -1;
	}

	return 0;
}

int on_chunk_header(llhttp_t *parser)
{
	return 0;
}

int on_message_complete_cb(llhttp_t* parser)
{
	int ret = 0;

	struct http_client *client = (struct http_client*)parser->data;

	char datetime_str[64];
	get_datetime_str(datetime_str, 64);

	if (client->method == HTTP_HEAD) {
		// if head
		complete_head_request(client, datetime_str);
	}
	else if (client->method == HTTP_PUT) {
		// if put
		//struct timespec t0, t1;
		//clock_gettime(CLOCK_MONOTONIC, &t0);
		complete_put_request(client, datetime_str);
		//clock_gettime(CLOCK_MONOTONIC, &t1);
	}
	else if (client->method == HTTP_POST) {
		// if post
		complete_post_request(client, datetime_str);
	}
	else if (client->method == HTTP_GET) {
		//struct timespec t0, t1;
		//clock_gettime(CLOCK_MONOTONIC, &t0);
		complete_get_request(client, datetime_str);
		//clock_gettime(CLOCK_MONOTONIC, &t1);
		//printf("complete GET request: %f s\n", elapsed_time(t1, t0));
	}
	else if (client->method == HTTP_DELETE) {
		complete_delete_request(client, datetime_str);
	}
	else {
		// DEBUG
		client->response_size = snprintf(NULL, 0, "%s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, datetime_str) + 1;
		client->response = malloc(client->response_size);
		snprintf(client->response, client->response_size, "%s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, datetime_str);
	}

	return 0;
}

void send_response(struct http_client *client)
{
	struct iovec iov[3];
	size_t iov_count = 1;

	iov[0].iov_base = client->response;
	iov[0].iov_len = strlen(client->response);

	if (client->data_payload != NULL) {
		iov[1].iov_base = client->data_payload;
		iov[1].iov_len = client->data_payload_size;
		iov_count++;
	}

	//struct timespec t0, t1;
	//clock_gettime(CLOCK_MONOTONIC, &t0);
	int ret = writev(client->fd, iov, iov_count);
	if (ret == -1) perror("writev");
	//clock_gettime(CLOCK_MONOTONIC, &t1);
	//printf("writev: %f s\n", elapsed_time(t1, t0));
}

void aio_ack_callback(rados_completion_t comp, void *arg) {
}

void aio_commit_callback(rados_completion_t comp, void *arg) {
	struct http_client *client = (struct HTTP_Client*)arg;
	send_response(client);
	reset_http_client(client);
}

int on_reset_cb(llhttp_t *parser)
{
	return 0;
}

struct http_client *create_http_client(int fd)
{
	struct http_client *client = (struct http_client*)calloc(1, sizeof(struct http_client));

	llhttp_settings_init(&(client->settings));
	llhttp_init(&(client->parser), HTTP_BOTH, &(client->settings));

	client->settings.on_message_complete = on_message_complete_cb;
	client->settings.on_header_field = on_header_field_cb;
	client->settings.on_header_value = on_header_value_cb;
	client->settings.on_headers_complete = on_headers_complete_cb;
	client->settings.on_url = on_url_cb;
	client->settings.on_reset = on_reset_cb;
	client->settings.on_body = on_body_cb;

	reset_http_client(client);
	client->fd = fd;
	client->parser.data = client;
	client->outstanding_aio_count = 0;

	client->bucket_io_ctx = &bucket_io_ctx;
	client->data_io_ctx = &data_io_ctx;

	client->write_op = rados_create_write_op();
	rados_aio_create_completion((void*)client, aio_ack_callback, aio_commit_callback, &(client->aio_completion));

	return client;
}

int on_headers_complete_cb(llhttp_t* parser)
{
	struct http_client *client = (struct http_client*)parser->data;

	client->method = llhttp_get_method(parser);

	// process URI
	if (client->uri.pathHead != NULL) {
		size_t bucket_name_len = client->uri.pathHead->text.afterLast - client->uri.pathHead->text.first;
		if (bucket_name_len > 0) {
			if (client->bucket_name) { free(client->bucket_name); client->bucket_name = NULL; }
			client->bucket_name = strndup(client->uri.pathHead->text.first, bucket_name_len);
			unescapeHtml(client->bucket_name);

			if (client->uri.pathHead->next != NULL && client->uri.pathHead->next->text.afterLast - client->uri.pathHead->next->text.first > 0) {
				// calculate total length
				size_t object_name_len = 0;
				size_t num_segments = 0;
				size_t off = 0;
				UriPathSegmentA *segment;

				for (segment = client->uri.pathHead->next, num_segments = 0; segment != NULL; segment = segment->next, num_segments++) {
					object_name_len += segment->text.afterLast - segment->text.first;
				}

				// object scope exists
				if (num_segments > 0) {
					object_name_len += num_segments;
					if (client->object_name) { free(client->object_name); client->object_name = NULL; }
					client->object_name = malloc(sizeof(char) * object_name_len);
					for (segment = client->uri.pathHead->next, off = 0; segment != NULL; segment = segment->next) {
						size_t len = segment->text.afterLast - segment->text.first;
						strncpy(client->object_name + off, segment->text.first, len);
						off += len;
						*(client->object_name + off++) = '/';
					}
					*(client->object_name + object_name_len - 1) = 0;
					unescapeHtml(client->object_name);
				}
			}
		}
	}

	if (client->method == HTTP_PUT) {
		init_object_put_request(client);
	}
	else if (client->method == HTTP_POST) {
		UriQueryListA *queryList;
		int itemCount;

		if (uriDissectQueryMallocA(&queryList, &itemCount, client->uri.query.first, client->uri.query.afterLast) == URI_SUCCESS) {
			// go through list of queries
			for (struct UriQueryListStructA *query = queryList; query != NULL; query = query->next) {
				// DeleteObjects
				if (strcasecmp(query->key, "delete") == 0 && client->bucket_name != NULL) {
					init_objects_delete_request(client);
				}
			}
			uriFreeQueryListA(queryList);
		}
	}

	if (client->expect == CONTINUE) {
		// if expects continue
		send(client->fd, HTTP_CONTINUE_HDR, strlen(HTTP_CONTINUE_HDR), 0);
	}

	return 0;
}


