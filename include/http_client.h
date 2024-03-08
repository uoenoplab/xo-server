#ifndef __HTTP_CLIENT_H__
#define __HTTP_CLIENT_H__

#include <rados/librados.h>
#include <uriparser/Uri.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <llhttp.h>

#include "md5.h"
#include "util.h"

#define MAX_AIO_OP 65536
#define MAX_FIELDS 64
#define MAX_HTTP_CLIENTS 65536

#define BUCKET_POOL "bucket_pool"
#define DATA_POOL "data_pool"

enum http_expect { CONTINUE, NONE };

extern size_t BUF_SIZE;
extern char *random_buffer;

static char *HTTP_OK_HDR = (char *)"HTTP/1.1 200 OK\r\n"
		 "Connection: keep-alive\r\n"
		 "Server: Apache/2.2.800";

static char *HTTP_NOT_FOUND_HDR = (char *)"HTTP/1.1 404 NOT FOUND\r\n"
		 "Connection: keep-alive\r\n"
		 "Server: Apache/2.2.800";

static char *HTTP_CONTINUE_HDR = (char *)"HTTP/1.1 100 CONTINUE\r\n\r\n";

static char *AMZ_REQUEST_ID = (char*)"tx000009a75d393f1564ec2-0065202454-3771-default";

struct http_client {
	int fd;

	char *put_buf;
	rados_completion_t aio_completion[MAX_AIO_OP];
	size_t outstanding_aio_count;
	rados_write_op_t write_op;
	rados_read_op_t read_op;

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

extern __thread struct http_client *http_clients[MAX_HTTP_CLIENTS];

extern rados_t cluster;
extern _Thread_local rados_ioctx_t bucket_io_ctx;
extern _Thread_local rados_ioctx_t data_io_ctx;

struct http_client *create_http_client(int fd);
void reset_http_client(struct http_client *client);
void free_http_client(struct http_client *client);

int on_header_field_cb(llhttp_t *parser, const char *at, size_t length);
int on_header_value_cb(llhttp_t *parser, const char *at, size_t length);
int on_body_cb(llhttp_t *parser, const char *at, size_t length);
int on_url_cb(llhttp_t *parser, const char *at, size_t length);

int on_headers_complete_cb(llhttp_t* parser);
int on_chunk_header(llhttp_t *parser);
int on_message_complete_cb(llhttp_t* parser);
int on_reset_cb(llhttp_t *parser);

#endif
