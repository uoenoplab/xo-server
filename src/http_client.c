#include <assert.h>
#include <time.h>
#include <string.h>

#include <errno.h>
#include <sys/epoll.h>
#include <sys/uio.h>

#include "http_client.h"
#include "object_store.h"
#include "osd_mapping.h"
#include "handoff.h"

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
		//printf("writev fd=%d called %ld/%ld %ld/%ld\n", client->fd, client->response_sent, client->response_size, client->data_payload_sent, client->data_payload_size);
	}

	if (client->response_size == client->response_sent && client->data_payload_size == client->data_payload_sent) {
		struct epoll_event event = {};
		event.data.ptr = client;
		//event.data.u32 = client->epoll_data_u32;
		event.events = EPOLLIN;
		epoll_ctl(client->epoll_fd, EPOLL_CTL_MOD, client->fd, &event);
	}
}

static int on_header_field_cb(llhttp_t *parser, const char *at, size_t length)
{
	struct http_client *client = (struct http_client*)parser->data;
	if (client->header_value_parsed > 0) {
		client->header_fields[client->num_fields][client->header_field_parsed] = '\0';
		client->num_fields++;
		client->header_field_parsed = 0;
		client->header_value_parsed = 0;
	}

	assert(client->header_field_parsed + length < MAX_FIELDS_SIZE);
	memcpy(&(client->header_fields[client->num_fields][client->header_field_parsed]), at, length);
	client->header_field_parsed += length;
	return 0;
}

static int on_header_value_cb(llhttp_t *parser, const char *at, size_t length)
{
	struct http_client *client = (struct http_client*)parser->data;

	if (client->header_value_parsed == 0)
		client->header_fields[client->num_fields][client->header_field_parsed] = '\0';

	memcpy(&(client->header_values[client->num_fields][client->header_value_parsed]), at, length);
	client->header_value_parsed += length;

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

	client->uri_str = realloc(client->uri_str, client->uri_str_len + length);
	assert(client->uri_str != NULL);
	memcpy(client->uri_str + client->uri_str_len, at, length);
	client->uri_str_len += length;

	return 0;
}

static int on_url_complete_cb(llhttp_t* parser)
{
	struct http_client *client = (struct http_client*)parser->data;

	char errorPos;
	int ret;
	UriUriA uri;

	client->uri_str = realloc(client->uri_str, client->uri_str_len + 1);
	assert(client->uri_str != NULL);
	client->uri_str[client->uri_str_len] = '\0';

	// if url bigger than something, return 414 Too long request

	char *tmp_url = malloc(sizeof(char) * (client->uri_str_len + 1));
	assert(tmp_url != NULL);
	memcpy(tmp_url, client->uri_str, client->uri_str_len);
	tmp_url[client->uri_str_len] = '\0';

	char *rest = tmp_url;
	char *token = strtok_r(rest, "/", &rest);
	size_t token_len = 0;

	if (token != NULL) {
		const char *bucket_name_end = strstr(token, "?");
		if (bucket_name_end != NULL) {
			token_len = bucket_name_end - token;
			assert(token_len < MAX_BUCKET_NAME_SIZE);

			memcpy(client->bucket_name, token, token_len);
			client->bucket_name[token_len] = '\0';
		}
		else {
			token_len = strlen(token);
			assert(token_len < MAX_BUCKET_NAME_SIZE);
			snprintf(client->bucket_name, MAX_BUCKET_NAME_SIZE, "%s", token);
		}

		token = strtok_r(NULL, "/", &rest);
		if (token != NULL) {
			token_len = strlen(token);
			assert(token_len < MAX_OBJECT_NAME_SIZE);
			snprintf(client->object_name, MAX_OBJECT_NAME_SIZE, "%s", token);
		}
	}

	free(tmp_url);

//	if ((ret = uriParseSingleUriA(&uri, client->uri_str, &errorPos)) != URI_SUCCESS) {
//		 fprintf(stderr, "Parse uri fail: %d\n", errorPos);
//		 return -1;
//	}
//
//	if (uri.pathHead != NULL) {
//		size_t bucket_name_len = uri.pathHead->text.afterLast - uri.pathHead->text.first;
//		if (bucket_name_len > 0) {
//			//client->bucket_name = malloc(bucket_name_len + 1);
//			// TODO if bucket name > 64 char return error
//			memcpy(client->bucket_name, uri.pathHead->text.first, bucket_name_len);
//			client->bucket_name[bucket_name_len] = '\0';
//			unescapeHtml(client->bucket_name);
//
//			if (uri.pathHead->next != NULL && uri.pathHead->next->text.afterLast - uri.pathHead->next->text.first > 0) {
//				// calculate total length
//				size_t object_name_len = 0;
//				size_t num_segments = 0;
//				size_t off = 0;
//				UriPathSegmentA *segment;
//
//				for (segment = uri.pathHead->next, num_segments = 0; segment != NULL; segment = segment->next, num_segments++) {
//					object_name_len += segment->text.afterLast - segment->text.first;
//				}
//
//				// object scope exists
//				if (num_segments > 0) {
//					object_name_len += num_segments;
//					object_name_len -=1;
//					// TODO if object name > 1024 char return error
//					//client->object_name = malloc(sizeof(char) * object_name_len + 1);
//					for (segment = uri.pathHead->next, off = 0; segment != NULL; segment = segment->next) {
//						size_t len = segment->text.afterLast - segment->text.first;
//						memcpy(client->object_name + off, segment->text.first, len);
//						off += len;
//						*(client->object_name + off++) = '/';
//					}
//					client->object_name[object_name_len] = '\0';
//					unescapeHtml(client->object_name);
//				}
//			}
//		}
//		uriFreeUriMembersA(&uri);
//	}
//
//	//if (ret == URI_SUCCESS)
//	//	uriFreeUriMembersA(&uri);

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

	char datetime_str[256];
	get_datetime_str(datetime_str, 256);

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
		assert(client->response_size <= MAX_RESPONSE_SIZE);
		snprintf(client->response, client->response_size, "%s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, datetime_str);
		client->response_size--;
		send_response(client);
	}

	return 0;
}

void send_response(struct http_client *client)
{
	struct epoll_event event = {};
	event.data.ptr = client;
	//event.data.u32 = client->epoll_data_u32;
	event.events = EPOLLOUT;
	int ret = epoll_ctl(client->epoll_fd, EPOLL_CTL_MOD, client->fd, &event);
	assert(ret == 0);
}

//void aio_ack_callback(rados_completion_t comp, void *arg) {
//}
//
//void aio_commit_callback(rados_completion_t comp, void *arg) {
//	struct http_client *client = (struct http_client*)arg;
//	send_response(client);
//}

static int on_reset_cb(llhttp_t *parser)
{
	struct http_client *client = (struct http_client*)parser->data;
	reset_http_client(client);

	return 0;
}

static int on_headers_complete_cb(llhttp_t* parser)
{
	int ret = 0;
	struct http_client *client = (struct http_client*)parser->data;

	client->method = llhttp_get_method(parser);

	// finish off the header fields
	if (client->header_value_parsed > 0) {
		client->header_values[client->num_fields][client->header_value_parsed] = '\0';
		client->num_fields++;
	}

	for (size_t i = 0; i < client->num_fields; i++) {
		if (strcmp(client->header_values[i], "100-continue") == 0) {
			client->expect = CONTINUE;
		}
	}

	if (client->method == HTTP_PUT) {
		init_object_put_request(client);
	}
	else if (client->method == HTTP_GET) {
		/* retrieve obj from OSD */
		if (strlen(client->bucket_name) > 0 && strlen(client->object_name) > 0) {
			/* check if we want to migrate */
			int acting_primary_osd_id = -1;
			ret = rados_get_object_osd_position(client->data_io_ctx, client->object_name, &acting_primary_osd_id);
			assert(ret == 0);
			printf("/%s/%s in osd.%d\n", client->bucket_name, client->object_name, acting_primary_osd_id);
			//if (get_my_osd_id() == acting_primary_osd_id) {
			//	handle_handoff_out_send(client, acting_primary_osd_id);
			//}
		}

		init_object_get_request(client);
	}
	else if (client->method == HTTP_POST) {
		UriQueryListA *queryList;
		int itemCount;

		UriUriA uri;
		const char *errorPos;
		int ret = -1;

		char *url = malloc(client->uri_str_len + 1);
		memcpy(url, client->uri_str, client->uri_str_len);
		url[client->uri_str_len] = '\0';

		if ((ret = uriParseSingleUriA(&uri, url, &errorPos)) != URI_SUCCESS) {
			fprintf(stderr, "Parse uri fail: %s\n", errorPos);
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
		free(url);
	}

	if (client->expect == CONTINUE) {
		// if expects continue
		send(client->fd, HTTP_CONTINUE_HDR, strlen(HTTP_CONTINUE_HDR), 0);
	}

	return 0;
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
	memset(client->bucket_name, 0, 64);
	memset(client->object_name, 0, 1025);

//	if (client->put_buf != NULL)  {
//		free(client->put_buf);
//		client->put_buf = NULL;
		client->object_offset = 0;
//	}

	client->response_size = 0;
	client->response_sent = 0;
	//if (client->response != NULL) {
	//	free(client->response);
	//	client->response = NULL;
	//	client->response_size = 0;
	//	client->response_sent = 0;
	//}

	//memset(client->data_payload, 0, 1024*1024*4);
	client->data_payload_size = 0;
	client->data_payload_sent = 0;
	//if (client->data_payload != NULL) {
	//	free(client->data_payload);
	//	client->data_payload = NULL;
	//	client->data_payload_size = 0;
	//	client->data_payload_sent = 0;
	//}

	client->prval = 0;
	client->object_size = 0;
	client->parsing = false;
	client->deleting = false;
	client->chunked_upload = false;

	client->header_field_parsed = 0;
	client->header_value_parsed = 0;

//	llhttp_finish(&(client->parser));
//
//	llhttp_settings_init(&(client->settings));
//	llhttp_init(&(client->parser), HTTP_BOTH, &(client->settings));
}

struct http_client *create_http_client(int epoll_fd, int fd)
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

	client->put_buf = malloc(0);
	client->data_payload = malloc(0);
	//client->response = NULL;
	client->uri_str = malloc(0);
	reset_http_client(client);

	client->epoll_fd = epoll_fd;
	client->epoll_data_u32 = 0;
	client->fd = fd;
	client->parser.data = client;

//	client->header_fields = (char**)malloc(sizeof(char*) * MAX_FIELDS);
//	client->header_values = (char**)malloc(sizeof(char*) * MAX_FIELDS);

	//rados_aio_create_completion((void*)client, aio_commit_callback, NULL, &client->comp);
	client->bucket_io_ctx = -1;
	client->data_io_ctx = -1;

	client->prval = 0;

	client->tls.is_ssl = true;
	client->tls.is_handshake_done = false;
	client->tls.is_ktls_set = false;
	client->tls.ssl = NULL;
	client->tls.rbio = NULL;
	client->tls.wbio = NULL;
	client->tls.client_hello_check_off = false;
	client->tls.is_client_traffic_secret_set = false;
	client->tls.is_server_traffic_secret_set = false;

	return client;
}

void free_http_client(struct http_client *client)
{
	llhttp_finish(&(client->parser));
	reset_http_client(client);

//	free(client->header_fields);
//	free(client->header_values);
	free(client->uri_str);
	free(client->data_payload);
	free(client->put_buf);

	//rados_aio_release(client->comp);

	free(client);
}
