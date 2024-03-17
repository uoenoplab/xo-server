#ifndef __HTTP_CLIENT_H__
#define __HTTP_CLIENT_H__

#include <rados/librados.h>
#include <uriparser/Uri.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <llhttp.h>

#include "md5.h"
#include "util.h"

#define MAX_FIELDS 32
#define MAX_HTTP_CLIENTS 1000

#define BUCKET_POOL "bucket_pool"
#define DATA_POOL "data_pool"

enum http_expect { CONTINUE, NONE };

extern size_t BUF_SIZE;

static char *HTTP_OK_HDR = (char *)"HTTP/1.1 200 OK\r\n"
		 "Connection: keep-alive\r\n"
		 "Server: Apache/2.2.800";

static char *HTTP_NOT_FOUND_HDR = (char *)"HTTP/1.1 404 NOT FOUND\r\n"
		 "Connection: keep-alive\r\n"
		 "Server: Apache/2.2.800";

static char *HTTP_CONTINUE_HDR = (char *)"HTTP/1.1 100 CONTINUE\r\n\r\n";

static char *AMZ_REQUEST_ID = (char*)"tx000009a75d393f1564ec2-0065202454-3771-default";

struct http_client {
	int epoll_fd;
	int fd;

	//char *put_buf;
	char put_buf[1024*1024*4];

	int prval;
	rados_xattrs_iter_t iter;

	rados_write_op_t write_op;
	rados_read_op_t read_op;

	rados_completion_t aio_completion;
	rados_completion_t aio_head_read_completion;

	ssize_t data_payload_sent;
	ssize_t data_payload_size;
	char *data_payload;
	//char data_payload[1024*1024*4];

	ssize_t response_sent;
	ssize_t response_size;
	//char *response;
	char response[65536];

	llhttp_t parser;
	llhttp_settings_t settings;
	enum http_expect expect;
	enum llhttp_method method;

	//char *bucket_name;
	//char *object_name;
	char bucket_name[65536];
	char object_name[65536];

	char uri_str[4096];
	size_t uri_str_len;
	//UriUriA uri;

	char header_fields[MAX_FIELDS][4096];
	char header_values[MAX_FIELDS][4096];

	size_t header_field_parsed;
	size_t header_value_parsed;
	//char **header_fields;
	//char **header_values;
	size_t num_fields;

	rados_ioctx_t *bucket_io_ctx;
	rados_ioctx_t *data_io_ctx;

	size_t object_size;
	size_t object_offset;

	size_t http_payload_size;
	bool chunked_upload;
	bool parsing;
	bool deleting;

	size_t current_chunk_size;
	size_t current_chunk_offset;
	RollingMD5Context md5_ctx;

	xmlParserCtxtPtr xml_ctx;
};

struct http_client *create_http_client(int epoll_fd, int fd, rados_ioctx_t *bucket_io_ctx, rados_ioctx_t *data_io_ctx);
void reset_http_client(struct http_client *client);
void free_http_client(struct http_client *client);

//static int on_header_field_cb(llhttp_t *parser, const char *at, size_t length);
//static int on_header_value_cb(llhttp_t *parser, const char *at, size_t length);
//static int on_body_cb(llhttp_t *parser, const char *at, size_t length);
//static int on_url_cb(llhttp_t *parser, const char *at, size_t length);
//
//static int on_headers_complete_cb(llhttp_t* parser);
//static int on_chunk_header(llhttp_t *parser);
//static int on_message_complete_cb(llhttp_t* parser);
//static int on_reset_cb(llhttp_t *parser);

void send_client_data(struct http_client *client);
void send_response(struct http_client *client);
void aio_ack_callback(rados_completion_t comp, void *arg);
void aio_commit_callback(rados_completion_t comp, void *arg);

#endif
