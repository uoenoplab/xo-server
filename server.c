#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <time.h>

#include <rados/librados.h>
#include <uriparser/Uri.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <llhttp.h>

#include "md5.h"

#define PORT 8080
#define MAX_EVENTS 1000
#define MAX_FIELDS 64

#define BUCKET_POOL "bucket_pool"
#define DATA_POOL "data_pool"

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

int object_count = 0;
static char *HTTP_OK_HDR = (char *)"HTTP/1.1 200 OK\r\n"
		 "Connection: keep-alive\r\n"
		 "Server: Apache/2.2.800";

static char *HTTP_NOT_FOUND_HDR = (char *)"HTTP/1.1 404 NOT FOUND\r\n"
		 "Connection: keep-alive\r\n"
		 "Server: Apache/2.2.800";

static char *HTTP_CONTINUE_HDR = (char *)"HTTP/1.1 100 CONTINUE\r\n\r\n";

enum http_expect { CONTINUE, NONE };

struct http_client {
	int fd;

	llhttp_t parser;
	llhttp_settings_t settings;
	enum http_expect expect;
	enum llhttp_method method;

	char *bucket_name;
	char *object_name;
	char *full_object_name;

	char *uri_str;
	UriUriA uri;

	char *header_fields[MAX_FIELDS];
	char *header_values[MAX_FIELDS];
	size_t num_fields;

	rados_ioctx_t bucket_io_ctx;
	rados_ioctx_t data_io_ctx;

	size_t object_size;
	size_t object_offset;

	size_t http_payload_size;
	bool chunked_upload;
	bool parsing;
//	rados_completion_t aio_completion[MAX_AIO_OP];

	size_t current_chunk_size;
	size_t current_chunk_offset;
	RollingMD5Context md5_ctx;
};

struct http_client *http_clients[65536] = { NULL };;
rados_t cluster;
char *client_data_buffer;
size_t BUF_SIZE = sizeof(char) * 1024 * 4096;

void get_datetime_str(char *buf, size_t length)
{
	time_t now = time(0);
	struct tm tm = *gmtime(&now);
	strftime(buf, length, "%a, %d %b %Y %H:%M:%S %Z", &tm);
}

void reset_http_client(struct http_client *client)
{
	for (size_t i = 0; i < client->num_fields; i++) {
		free(client->header_fields[i]);
		free(client->header_values[i]);
	}

	uriFreeUriMembersA(&(client->uri));
	client->num_fields = 0;
	client->expect = NONE;

	client->chunked_upload = false;
	client->object_size = 0;
	client->object_offset = 0;
	client->http_payload_size = 0;
	client->parsing = false;

	client->current_chunk_size = 0;
	client->current_chunk_offset = 0;
}

void free_http_client(struct http_client *client)
{
	llhttp_finish(&(client->parser));
	reset_http_client(client);
	free(client->uri_str);
	free(client->bucket_name);
	free(client->object_name);
	rados_ioctx_destroy(client->bucket_io_ctx);
	rados_ioctx_destroy(client->data_io_ctx);
	free(client);
	printf("free client\n");
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
////	else if (strcmp(client->header_fields[client->num_fields], "X-Amz-Decoded-Content-Length") == 0) {
////		client->data_length = atol(client->header_values[client->num_fields]);
////		printf("set data length %ld\n", client->data_length);
////	}
//	else if (strcmp(client->header_fields[client->num_fields], "Content-Length") == 0) {
//		client->content_length = atol(client->header_values[client->num_fields]);
//	}

	client->num_fields++;

	return 0;
}

void printByteArrayHex(const unsigned char *byteArray, size_t length) {
    for (size_t i = 0; i < length; i++) {
        printf("%02X ", byteArray[i]); // %02X formats the byte as a two-digit hexadecimal number
    }
    printf("\n");
}

void put_object(struct http_client *client, const char *buf, size_t length)
{
	int ret;
	char *ptr = buf;

	//printf("put_object: %s , length=%ld\n", buf, length);
	size_t current_chunk_size = client->current_chunk_size;
	size_t current_chunk_offset = client->current_chunk_offset;
	//printf("calling put object length %ld current_chunk_size %ld current_chunk_offset %ld\n", length, current_chunk_size, current_chunk_offset);

	while (length > 0) {
		//printf("beginning of loop: current_chunk_size=%ld current_chunk_offset=%ld\n", current_chunk_size, current_chunk_offset);
		// 1. we are starting a chunk
		// 2. we are in a middle of a chunk
		// 3. we are working towards the end of a chunk
		if (current_chunk_size == 0 && current_chunk_offset == 0) {
			char *chunk_size_start = ptr;
			//printByteArrayHex(chunk_size_start, 16);
			char *chunk_size_end = memmem(chunk_size_start, length, ";chunk-signature=", strlen(";chunk-signature="));
			char *chunk_size_str = strndup(chunk_size_start, chunk_size_end - chunk_size_start);
			current_chunk_size = strtol(chunk_size_str, NULL, 16);

			length -= chunk_size_end - chunk_size_start;
			//char *chunk_data_start = memmem(chunk_size_end, length, "\r\n", 2) + 2;
			char *chunk_data_start = memmem(chunk_size_end, length, "\r\n", 2);
			printf("chunk_size: %ld ; ptr %.*s ; object size %ld object offset %ld\n", current_chunk_size, chunk_data_start - chunk_size_start, ptr, client->object_size, client->object_offset); 
			if (chunk_data_start == NULL) {
				printf("chunk start is null!!!\n");
				exit(1);
			}
			else {
				chunk_data_start += 2;
			}
			char *chunk_data_end = NULL;
			length -= chunk_data_start - chunk_size_end;
			if (length > current_chunk_size) {
				chunk_data_end = chunk_data_start + current_chunk_size;
			}
			else {
				chunk_data_end = chunk_data_start + length;
			}
			//printByteArrayHex(chunk_data_start, length - (chunk_data_start - buf));
			size_t data_len = (char*)chunk_data_end - (char*)chunk_data_start;
			ret = rados_write(client->data_io_ctx, client->object_name, chunk_data_start, data_len, client->object_offset);
			if (ret) {
				fprintf(stderr, "fail to write to rados\n");
			}
			updateRollingMD5(&(client->md5_ctx), chunk_data_start, data_len);

			client->object_offset += data_len;
			current_chunk_offset += data_len;
			ptr = chunk_data_end;
			length -= data_len;
			//printf("FIRST write to rados object name %s data_len %ld chunk_size %ld chunk_offset %ld length %ld\n", client->object_name, data_len, current_chunk_size, current_chunk_offset, length);

			free(chunk_size_str);
			//printByteArrayHex(ptr, 16);
			//printf("%.*s\n", 88, ptr);
		}
		else if (current_chunk_offset < current_chunk_size) {
			// check if this chunk is ending
			char *chunk_data_start = ptr;
			//printByteArrayHex(chunk_data_start, 16);
			char *chunk_data_end = NULL;
			if (current_chunk_offset + length > current_chunk_size) {
				//printf("second chunk ending\n");
				chunk_data_end = chunk_data_start + (current_chunk_size - current_chunk_offset);
			}
			else {
				// chunk not ending yet
				//printf("second chunk not ending\n");
				chunk_data_end = chunk_data_start + length;
				//chunk_data_end -= 1;
			}

			//printf("chunk_size: %ld ; ptr %s ; object size %ld object offset %ld\n", current_chunk_size, ptr, client->object_size, client->object_offset); 
			size_t data_len = chunk_data_end - chunk_data_start;

			ret = rados_write(client->data_io_ctx, client->object_name, chunk_data_start, data_len, client->object_offset);
			if (ret) {
				fprintf(stderr, "fail to write to rados\n");
			}
			updateRollingMD5(&(client->md5_ctx), chunk_data_start, data_len);

			if (current_chunk_offset + length > current_chunk_size) {
				ptr = chunk_data_end + 2;
			}
			else {
				ptr = chunk_data_end + 2;
			}
			client->object_offset += data_len;
			current_chunk_offset += data_len;
			length -= data_len;
			//printf("SECOND write to rados object name %s data_len %ld chunk_size %ld chunk_offset %ld length %ld\n", client->object_name, data_len, current_chunk_size, current_chunk_offset, length);
			//printByteArrayHex(ptr, 16);
			//printf("%.*s\n", 88, ptr);
		}
		if (current_chunk_offset >= current_chunk_size) {
			//printf("currentl chunk is done, reset\n");
			current_chunk_offset = 0;
			current_chunk_size = 0;
		}

		client->current_chunk_size = current_chunk_size;
		client->current_chunk_offset = current_chunk_offset;

		if (client->object_offset == client->object_size) {
			client->parsing = false;
			llhttp_finish(&(client->parser));
			printf("object size now same as object offset (length of buf %ld): %s\n", length, ptr);
			break;
		}
	}
}

int on_body_cb(llhttp_t *parser, const char *at, size_t length)
{
	struct http_client *client = (struct http_client*)parser->data;
	printf("on body, turn on parsing, at(%ld): %.40s\n", length, at);
	client->parsing = true;
	put_object(client, at, length);
	return 0;
}

int on_url_cb(llhttp_t *parser, const char *at, size_t length)
{
	const char * errorPos;
	struct http_client *client = (struct http_client*)parser->data;
	client->uri_str = strndup(at, length);

	if (uriParseSingleUriA(&(client->uri), client->uri_str, &errorPos) != URI_SUCCESS) {
		fprintf(stderr, "Parse uri fail: %s\n", errorPos);
		return -1;
	}

	return 0;
}

void init_object_put_request(struct http_client *client) {
	for (size_t i = 0; i < client->num_fields; i++) {
		if (strcmp(client->header_fields[i], "Content-Length") == 0) {
			client->http_payload_size = atol(client->header_values[i]);
		}
		else if (strcmp(client->header_fields[i], "X-Amz-Decoded-Content-Length") == 0) {
			client->object_size = atol(client->header_values[i]);
		}
	}

	if (client->http_payload_size != 0 && client->object_size == 0) {
		// non chunked transfer
		client->object_size = client->http_payload_size;
		client->chunked_upload = false;
		//fprintf(stderr, "non chunked upload\n");
	}
	else {
		client->chunked_upload = true;
		//fprintf(stderr, "chunked upload\n");
	}

	initRollingMD5(&(client->md5_ctx));
}

int on_headers_complete_cb(llhttp_t* parser)
{
	struct http_client *client = (struct http_client*)parser->data;

	client->method = llhttp_get_method(parser);
	client->bucket_name = NULL;
	client->object_name = NULL;

	if (client->uri.pathHead != NULL) {
		size_t bucket_name_len = client->uri.pathHead->text.afterLast - client->uri.pathHead->text.first;
		if (bucket_name_len > 0) {
			client->bucket_name = strndup(client->uri.pathHead->text.first, bucket_name_len);

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
					client->object_name = malloc(sizeof(char) * object_name_len);
					for (segment = client->uri.pathHead->next, off = 0; segment != NULL; segment = segment->next) {
						size_t len = segment->text.afterLast - segment->text.first;
						strncpy(client->object_name + off, segment->text.first, len);
						off += len;
						*(client->object_name + off++) = '/';
					}
					*(client->object_name + object_name_len - 1) = 0;
				}
			}
		}
	}

	//printf("bucket: %s object: %s\n", client->bucket_name, client->object_name);
	if (client->method == HTTP_PUT)
		init_object_put_request(client);

	return 0;
}

void complete_head_request(struct http_client *client, char *datetime_str, char *response, size_t response_buf_len)
{
	int ret = 0;
	UriQueryListA *queryList;
	int itemCount;

	xmlDocPtr doc = NULL;
	xmlNodePtr root_node = NULL, node = NULL;/* node pointers */

	xmlChar *xmlbuf;
	int xmlbuf_size;

	memset(response, 0, response_buf_len);

	// if scopped to bucket
	if (client->bucket_name != NULL && client->object_name == NULL) {
		char buf;
		int ret;
		ret = rados_read(client->bucket_io_ctx, client->bucket_name, &buf, 0, 0);
		if (ret != 0) {
			fprintf(stderr, "Bucket %s does not exist\n", client->bucket_name);
			snprintf(response, response_buf_len, "%s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_NOT_FOUND_HDR, datetime_str);
		}
		else {
			snprintf(response, response_buf_len, "%s\r\nX-RGW-Object-Count: 430\r\nX-RGW-Bytes-Used: 1803550720\r\nX-RGW-Quota-User-Size: -1\r\nX-RGW-Quota-User-Objects: -1\r\nX-RGW-Quota-Max-Buckets: 1000\r\nX-RGW-Quota-Bucket-Size: -1\r\nX-RGW-Quota-Bucket-Objects: -1\r\nx-amz-request-id: tx00000d0a0663662aed5bd-00651d6e1a-3771-default\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, datetime_str);
		}
	}
}

void complete_post_request(struct http_client *client, char *datetime_str, char *response, size_t response_buf_len)
{
}

void complete_put_request(struct http_client *client, char *datetime_str, char *response, size_t response_buf_len)
{
	int ret = 0;

	UriQueryListA *queryList;
	int itemCount;

	xmlDocPtr doc = NULL;
	xmlNodePtr root_node = NULL, node = NULL;/* node pointers */

	xmlChar *xmlbuf;
	int xmlbuf_size;

	memset(response, 0, response_buf_len);

	if (client->bucket_name != NULL && client->object_name == NULL) {
		// if scopped to bucket, create bucket
		rados_write_op_t write_op = rados_create_write_op();
		rados_write_op_create(write_op, LIBRADOS_CREATE_IDEMPOTENT, NULL);
		rados_write_op_operate2(write_op, client->bucket_io_ctx, client->bucket_name, NULL, 0);
		rados_release_write_op(write_op);
		snprintf(response, response_buf_len, "%s\r\nx-amz-request-id: tx000009a75d393f1564ec2-0065202454-3771-default\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, datetime_str);
		//printf("REPLY: %s\n", response);
	}
	else if (client->bucket_name != NULL && client->object_name != NULL) {
		// if scopped to object, create object
		char md5_hash[MD5_DIGEST_LENGTH * 2 + 1];
		finalizeRollingMD5(&(client->md5_ctx), md5_hash);

		char metadata[4096];
		snprintf(metadata, 4096, "%s;%s", datetime_str, md5_hash);
		const size_t key_lens = strlen(client->object_name);
		const size_t val_lens = strlen(metadata);
		const char *keys = client->object_name;
		const char *vals = metadata;

		rados_write_op_t write_op = rados_create_write_op();
		rados_write_op_omap_set2(write_op, &keys, &vals, &key_lens, &val_lens, 1);
		rados_write_op_operate2(write_op, client->bucket_io_ctx, client->bucket_name, NULL, 0);
		rados_release_write_op(write_op);

		snprintf(response, response_buf_len, "%s\r\nEtag: %s\r\nx-amz-request-id: tx000009a75d393f1564ec2-0065202454-3771-default\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, md5_hash, datetime_str);
//		printf("REPLY: %s\n", response);

		client->parsing = false;
		object_count++;
		printf("OBJECT COUNT: %d\n", object_count);
	}
}

void complete_get_request(struct http_client *client, char *datetime_str, char *response, size_t response_buf_len)
{
	int ret = 0;

	UriQueryListA *queryList;
	int itemCount;

	xmlDocPtr doc = NULL;
	xmlNodePtr root_node = NULL, node = NULL;/* node pointers */

	xmlChar *xmlbuf;
	int xmlbuf_size;

	memset(response, 0, response_buf_len);

	if (client->bucket_name != NULL && client->object_name == NULL) {
		// if GET bucket service: no objects
		//fprintf(stderr, "GET bucket: %s\n", client->bucket_name);
		// check if bucket exist
		int ret; char buf;
		ret = rados_read(client->bucket_io_ctx, client->bucket_name, &buf, 0, 0);
		if (ret != 0) {
			// 404
			snprintf(response, response_buf_len, "%s\r\nx-amz-request-id: tx000009a75d393f1564ec2-0065202454-3771-default\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_NOT_FOUND_HDR, datetime_str);
		}
		else {
		//	snprintf(response, sizeof(response), "%s\r\nx-amz-request-id: tx000009a75d393f1564ec2-0065202454-3771-default\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, datetime_str);
			if (uriDissectQueryMallocA(&queryList, &itemCount, client->uri.query.first, client->uri.query.afterLast) == URI_SUCCESS) {
				// go through list of queries
				bool fetch_owner = false;
				int list_type = 1;

				doc = xmlNewDoc(BAD_CAST "1.0");
				root_node = xmlNewNode(NULL, BAD_CAST "root");
				xmlDocSetRootElement(doc, root_node);

				for (struct UriQueryListStructA *query = queryList; query != NULL; query = query->next) {
					//fprintf(stdout, "query: (%s,%s)\n", query->key, query->value);
					if (strcmp(query->key, "location") == 0) {
						node = xmlNewChild(root_node, NULL, BAD_CAST "LocationConstraint", "default");
						xmlNewProp(node, BAD_CAST "xmlns", BAD_CAST "http://s3.amazonaws.com/doc/2006-03-01/");
					}
					if (strcmp(query->key, "versioning") == 0){
						node = xmlNewChild(root_node, NULL, BAD_CAST "VersioningConfiguration", NULL);
						xmlNewProp(node, BAD_CAST "xmlns", BAD_CAST "http://s3.amazonaws.com/doc/2006-03-01/");
					}
					if (strcmp(query->key, "fetch-owner") == 0) {
						if (strcmp(query->value, "true") == 0)
							fetch_owner = true;
					}
					if (strcmp(query->key, "list_type") == 0) {
						if (strcmp(query->value, "2") == 0)
							list_type = 2;
					}
				}

				// return list inside bucket
				if (fetch_owner && list_type == 2) {
				}

				// dump XML document
				xmlDocDumpMemoryEnc(doc, &xmlbuf, &xmlbuf_size, "UTF-8");
				snprintf(response, response_buf_len, "%s\r\nContent-Length: %d\r\nDate: %s\r\n\r\n%s", HTTP_OK_HDR, xmlbuf_size, datetime_str, (char*)xmlbuf);

				// cleanup
				xmlFree(xmlbuf);
				xmlFreeDoc(doc);
				xmlCleanupParser();
			}
		}
	}
	else if (client->bucket_name != NULL && client->object_name != NULL) {
		// getting object
	}
}

int on_chunk_header(llhttp_t *parser)
{
	//printf("on chunk header\n");
	return 0;
}

int on_message_complete_cb(llhttp_t* parser)
{
	int ret = 0;
	char datetime_str[64];

	char response[65536];
	char response_payload[65536];

	struct http_client *client = (struct http_client*)parser->data;
	get_datetime_str(datetime_str, 64);

	if (client->expect == CONTINUE) {
		// if expects continue
		snprintf(response, 65536, "HTTP/1.1 100 CONTINUE\r\n\r\n");
	}
	else if (client->method == HTTP_HEAD) {
		// if head
		complete_head_request(client, datetime_str, response, sizeof(response));
	}
	else if (client->method == HTTP_PUT) {
		// if put
		complete_put_request(client, datetime_str, response, sizeof(response));
	}
	else if (client->method == HTTP_POST) {
		// if put
		complete_post_request(client, datetime_str, response, sizeof(response));
	}
	else if (client->method == HTTP_GET) {
		complete_get_request(client, datetime_str, response, sizeof(response));
	}
	else {
		// DEBUG
		snprintf(response, sizeof(response), "%s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, datetime_str);
	}

	//fprintf(stdout, "%s\n", response);
	send(client->fd, response, strlen(response), 0);
	reset_http_client(client);

	return 0;
}

int on_reset_cb(llhttp_t *parser)
{
	struct http_client *client = (struct http_client*)parser->data;
	//fprintf(stderr, "resetting http client for next message\n");
//	reset_http_client(client);
	//sleep(1);
	return 0;
}

struct http_client *create_http_client(int fd)
{
	int err;
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

	err = rados_ioctx_create(cluster, BUCKET_POOL, &(client->bucket_io_ctx));
	if (err < 0) {
		fprintf(stderr, "cannot open rados pool %s: %s\n", BUCKET_POOL, strerror(-err));
		rados_shutdown(cluster);
		exit(1);
	}

	err = rados_ioctx_create(cluster, DATA_POOL, &(client->data_io_ctx));
	if (err < 0) {
		fprintf(stderr, "cannot open rados pool %s: %s\n", DATA_POOL, strerror(-err));
		rados_shutdown(cluster);
		exit(1);
	}

	return client;
}

void handle_new_connection(int epoll_fd, int server_fd)
{
	int new_socket;
	struct sockaddr_in client_addr;
	socklen_t client_len = sizeof(client_addr);

	// Accept a new client connection
	new_socket = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
	if (new_socket == -1) {
		perror("accept");
		return;
	}

	//printf("Accepted connection from %s:%d\n", inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

	// Add the new client socket to the epoll event list
	struct epoll_event event;
	event.events = EPOLLIN | EPOLLET; // Edge-triggered mode
	event.data.fd = new_socket;
	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, new_socket, &event) == -1) {
		perror("epoll_ctl");
		close(new_socket);
	}

	http_clients[event.data.fd] = create_http_client(event.data.fd);
}

void handle_client_disconnect(int epoll_fd, int client_fd)
{
	// Remove the client socket from the epoll event list
	if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, NULL) == -1) {
		perror("epoll_ctl");
	}
	close(client_fd);
	free_http_client(http_clients[client_fd]);
}

void handle_client_data(int epoll_fd, int client_fd)
{
	ssize_t bytes_received;

	while (1) {
		memset(client_data_buffer, 0, BUF_SIZE);
		bytes_received = recv(client_fd, client_data_buffer, BUF_SIZE, 0);
		if (bytes_received <= 0) {
			// Client closed the connection or an error occurred
			//if (bytes_received == 0) {
			//	printf("Client disconnected: %d\n", client_fd);
			//} else {
			//	perror("recv");
			//}
	
			// Remove the client socket from the epoll event list
			handle_client_disconnect(epoll_fd, client_fd); // Handle client disconnection
			http_clients[client_fd] = NULL;
			break;
		}
		struct http_client *client = http_clients[client_fd];
		enum llhttp_errno ret;

		// Echo the received data back to the client
		ret = llhttp_execute(&(client->parser), client_data_buffer, bytes_received);
		if (ret != HPE_OK) {
			fprintf(stderr, "Parse error: %s %s\n", llhttp_errno_name(ret), client->parser.reason);
		}
		//printf("handle_client_data: recv %ld bytes: %.50s\n", bytes_received, client_data_buffer);
		//if (client->parsing) {
		//	printf("pass to put_object(%ld): %.*40\n", bytes_received, client_data_buffer);
		//	put_object(client, client_data_buffer, bytes_received);
		//}
	}
}

int main(int argc, char *argv[])
{
	int server_fd, new_socket, epoll_fd, event_count;
	struct sockaddr_in server_addr, client_addr;
	socklen_t client_len = sizeof(client_addr);
	struct epoll_event event, events[MAX_EVENTS];

	client_data_buffer = malloc(BUF_SIZE);

	int err;

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

	const int enable = 1;
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
	if (listen(server_fd, 5) == -1) {
		perror("listen");
		exit(EXIT_FAILURE);
	}

	// Create an epoll instance
	if ((epoll_fd = epoll_create1(0)) == -1) {
		perror("epoll_create1");
		exit(EXIT_FAILURE);
	}

	// Add the server socket to the epoll event list
	event.events = EPOLLIN;
	event.data.fd = server_fd;
	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event) == -1) {
		perror("epoll_ctl");
		exit(EXIT_FAILURE);
	}

	printf("Server is listening on port %d\n", PORT);

	while (1) {
		// Wait for events using epoll
		event_count = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
		if (event_count == -1) {
			perror("epoll_wait");
			exit(EXIT_FAILURE);
		}

		for (int i = 0; i < event_count; i++) {
			// Handle events using callback functions
			if (events[i].data.fd == server_fd) {
				handle_new_connection(epoll_fd, server_fd);
			} else {
				handle_client_data(epoll_fd, events[i].data.fd);
			}
		}
	}

	free(client_data_buffer);
	close(server_fd);
	close(epoll_fd);
	rados_shutdown(cluster);

	return 0;
}
