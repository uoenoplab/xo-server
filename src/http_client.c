#include <assert.h>
#include <time.h>

#include <errno.h>
#include <sys/epoll.h>
#include <sys/uio.h>

#include "http_client.h"
#include "object_store.h"

void send_client_data(struct http_client *client)
{
	struct iovec iov[2];
	size_t iov_count = 0;

	if (client->response_sent != client->response_size) {
		iov[iov_count].iov_base = client->response + client->response_sent;
		iov[iov_count].iov_len = client->response_size - client->response_sent;
		iov_count++;
	}

	if (client->data_payload_sent != client->data_payload_size) {
		iov[iov_count].iov_base = client->data_payload + client->data_payload_sent;
		iov[iov_count].iov_len = client->data_payload_size - client->data_payload_sent;
		iov_count++;
	}

	ssize_t ret = writev(client->fd, iov, iov_count);
	if (ret != EAGAIN) {
		// response is not sent
		if (client->response_sent != client->response_size) {
			size_t response_left = client->response_size - client->response_sent;
			if (ret > response_left) {
				client->response_sent = client->response_size;
				ret -= response_left;
			}
			else {
				client->response_sent += ret;
				ret = 0;
			}
		}

		client->data_payload_sent += ret;
		//printf("writev called %ld/%ld %ld/%ld\n", client->response_sent, client->response_size, client->data_payload_sent, client->data_payload_size);
	}

	if (client->response_size == client->response_sent && client->data_payload_size == client->data_payload_sent) {
		struct epoll_event event = {};
		event.data.ptr = client;
		event.events = EPOLLIN;
		epoll_ctl(client->epoll_fd, EPOLL_CTL_MOD, client->fd, &event);
		reset_http_client(client);
	}
}

void reset_http_client(struct http_client *client)
{
//	for (size_t i = 0; i < client->num_fields; i++) {
//		if (client->header_fields[i]) free(client->header_fields[i]);
//		if (client->header_values[i]) free(client->header_values[i]);
//	}

	client->num_fields = 0;
	client->expect = NONE;

	client->uri_str_len = 0;

	if (client->object_name) {
		free(client->object_name);
		client->object_name = NULL;
	}

	if (client->bucket_name) {
		free(client->bucket_name);
		client->bucket_name = NULL;
	}

	if (client->put_buf)  {
		free(client->put_buf);
		client->put_buf = NULL;
		client->object_offset = 0;
	}

	if (client->response) {
		free(client->response);
		client->response = NULL;
		client->response_size = 0;
		client->response_sent = 0;
	}

	if (client->data_payload) {
		free(client->data_payload);
		client->data_payload = NULL;
		client->data_payload_size = 0;
		client->data_payload_sent = 0;
	}

	client->prval = 0;
	client->object_size = 0;
	client->parsing = false;
	client->deleting = false;
	client->chunked_upload = false;
}

void free_http_client(struct http_client *client)
{
	llhttp_finish(&(client->parser));
	reset_http_client(client);

//	free(client->header_fields);
//	free(client->header_values);
	//free(client->uri_str);

	rados_aio_release(client->aio_head_read_completion);
	rados_aio_release(client->aio_completion);

	rados_release_read_op(client->read_op);
	rados_release_write_op(client->write_op);

	free(client);
}

static int on_header_field_cb(llhttp_t *parser, const char *at, size_t length)
{
	struct http_client *client = (struct http_client*)parser->data;
//	client->header_fields[client->num_fields] = malloc(length + 1);
	memcpy(client->header_fields[client->num_fields], at, length);
	client->header_fields[client->num_fields][length] = 0;
	return 0;
}

static int on_header_value_cb(llhttp_t *parser, const char *at, size_t length)
{
	struct http_client *client = (struct http_client*)parser->data;
//	client->header_values[client->num_fields] = malloc(length + 1);
	memcpy(client->header_values[client->num_fields], at, length);
	client->header_values[client->num_fields][length] = 0;

	if (strncmp(at, "100-continue", length) == 0) {
		client->expect = CONTINUE;
	}

	client->num_fields++;

	return 0;
}

static int on_body_cb(llhttp_t *parser, const char *at, size_t length)
{
	struct http_client *client = (struct http_client*)parser->data;
	if (client->method == HTTP_PUT) {
		put_object(client, at, length);
	}
	else if (client->method == HTTP_POST) {
		if (client->deleting == true) {
			delete_objects(client, at, length);
		}

	}
	return 0;
}

static int on_url_cb(llhttp_t *parser, const char *at, size_t length)
{
	struct http_client *client = (struct http_client*)parser->data;

	//client->uri_str = realloc(client->uri_str, client->uri_str_len + length);
	memcpy(client->uri_str + client->uri_str_len, at, length);
	client->uri_str_len += length;

	return 0;
}

static int on_url_complete_cb(llhttp_t* parser)
{
	const char * errorPos;
	struct http_client *client = (struct http_client*)parser->data;
	int ret = -1;

	UriUriA uri;
	//client->uri_str = realloc(client->uri_str, (client->uri_str_len + 1));
	//assert(client->uri_str != NULL);
	client->uri_str[client->uri_str_len] = 0;
	//printf("to parse uri %d-%d %s\n", client->uri_str_len, strlen(client->uri_str), client->uri_str);
	if ((ret = uriParseSingleUriA(&uri, client->uri_str, &errorPos)) != URI_SUCCESS) {
		fprintf(stderr, "Parse uri fail: %s\n", errorPos);
		return -1;
	}

	if (uri.pathHead != NULL) {
		size_t bucket_name_len = uri.pathHead->text.afterLast - uri.pathHead->text.first;
		if (bucket_name_len > 0) {
			client->bucket_name = malloc(bucket_name_len + 1);
			memcpy(client->bucket_name, uri.pathHead->text.first, bucket_name_len);
			client->bucket_name[bucket_name_len] = 0;
			//unescapeHtml(client->bucket_name);

			if (uri.pathHead->next != NULL && uri.pathHead->next->text.afterLast - uri.pathHead->next->text.first > 0) {
				// calculate total length
				size_t object_name_len = 0;
				size_t num_segments = 0;
				size_t off = 0;
				UriPathSegmentA *segment;

				for (segment = uri.pathHead->next, num_segments = 0; segment != NULL; segment = segment->next, num_segments++) {
					object_name_len += segment->text.afterLast - segment->text.first;
				}

				// object scope exists
				if (num_segments > 0) {
					object_name_len += num_segments;
					client->object_name = malloc(sizeof(char) * object_name_len);
					for (segment = uri.pathHead->next, off = 0; segment != NULL; segment = segment->next) {
						size_t len = segment->text.afterLast - segment->text.first;
						strncpy(client->object_name + off, segment->text.first, len);
						off += len;
						*(client->object_name + off++) = '/';
					}
					*(client->object_name + object_name_len - 1) = 0;
					//unescapeHtml(client->object_name);
				}
			}
		}
	}

	if (ret == URI_SUCCESS)
		uriFreeUriMembersA(&uri);

	return 0;
}

static int on_chunk_header(llhttp_t *parser)
{
	return 0;
}

static int on_message_complete_cb(llhttp_t* parser)
{
	int ret = 0;

	struct http_client *client = (struct http_client*)parser->data;

	char datetime_str[64];
	get_datetime_str(datetime_str, 64);

	if (client->method == HTTP_HEAD) {
		complete_head_request(client, datetime_str);
	}
	else if (client->method == HTTP_PUT) {
		complete_put_request(client, datetime_str);
	}
	else if (client->method == HTTP_POST) {
		complete_post_request(client, datetime_str);
	}
	else if (client->method == HTTP_GET) {
		complete_get_request(client, datetime_str);
	}
	else if (client->method == HTTP_DELETE) {
		complete_delete_request(client, datetime_str);
	}
	else {
		// DEBUG
		client->response_size = snprintf(NULL, 0, "%s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, datetime_str) + 1;
		client->response = malloc(client->response_size);
		snprintf(client->response, client->response_size, "%s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, datetime_str);
		client->response--;
		send_response(client);
	}

	return 0;
}

void send_response(struct http_client *client)
{
	struct epoll_event event = {};
	event.data.ptr = client;
	event.events = EPOLLOUT;
	epoll_ctl(client->epoll_fd, EPOLL_CTL_MOD, client->fd, &event);
}

void aio_ack_callback(rados_completion_t comp, void *arg) {
	struct http_client *client = (struct http_client*)arg;
	send_response(client);
}

void aio_commit_callback(rados_completion_t comp, void *arg) {
}

static int on_reset_cb(llhttp_t *parser)
{
	return 0;
}

static int on_headers_complete_cb(llhttp_t* parser)
{
	struct http_client *client = (struct http_client*)parser->data;

	client->method = llhttp_get_method(parser);

	if (client->method == HTTP_PUT) {
		init_object_put_request(client);
	}
	else if (client->method == HTTP_GET) {
		init_object_get_request(client);
	}
	else if (client->method == HTTP_POST) {
		UriQueryListA *queryList;
		int itemCount;

		UriUriA uri;
		char errorPos;
		int ret = -1;

		if ((ret = uriParseSingleUriA(&uri, client->uri_str, &errorPos)) != URI_SUCCESS) {
			fprintf(stderr, "Parse uri fail: %c\n", errorPos);
			return -1;
		}

		if (uriDissectQueryMallocA(&queryList, &itemCount, uri.query.first, uri.query.afterLast) == URI_SUCCESS) {
			// go through list of queries
			for (struct UriQueryListStructA *query = queryList; query != NULL; query = query->next) {
				// DeleteObjects
				if (strcasecmp(query->key, "delete") == 0 && client->bucket_name != NULL) {
					init_objects_delete_request(client);
				}
			}
			uriFreeQueryListA(queryList);
		}

		if (ret == URI_SUCCESS)
			uriFreeUriMembersA(&uri);
	}

	if (client->expect == CONTINUE) {
		// if expects continue
		send(client->fd, HTTP_CONTINUE_HDR, strlen(HTTP_CONTINUE_HDR), 0);
	}

	return 0;
}

struct http_client *create_http_client(int epoll_fd, int fd, rados_ioctx_t *bucket_io_ctx, rados_ioctx_t *data_io_ctx)
{
	struct http_client *client = (struct http_client*)calloc(1, sizeof(struct http_client));

	llhttp_settings_init(&(client->settings));
	llhttp_init(&(client->parser), HTTP_BOTH, &(client->settings));

	client->settings.on_message_complete = on_message_complete_cb;
	client->settings.on_header_field = on_header_field_cb;
	client->settings.on_header_value = on_header_value_cb;
	client->settings.on_headers_complete = on_headers_complete_cb;
	client->settings.on_url = on_url_cb;
	client->settings.on_url_complete = on_url_complete_cb;
	client->settings.on_reset = on_reset_cb;
	client->settings.on_body = on_body_cb;

	reset_http_client(client);

	client->epoll_fd = epoll_fd;
	client->fd = fd;
	client->parser.data = client;

	//client->uri_str = malloc(0);
//	client->header_fields = (char**)malloc(sizeof(char*) * MAX_FIELDS);
//	client->header_values = (char**)malloc(sizeof(char*) * MAX_FIELDS);

	client->bucket_io_ctx = bucket_io_ctx;
	client->data_io_ctx = data_io_ctx;

	client->prval = 0;
	client->write_op = rados_create_write_op();
	client->read_op = rados_create_read_op();

	rados_aio_create_completion((void*)client, aio_ack_callback, aio_commit_callback, &(client->aio_completion));
	rados_aio_create_completion((void*)client, NULL, NULL, &(client->aio_head_read_completion));

	return client;
}
