#ifndef __HTTP_CLIENT_H__
#define __HTTP_CLIENT_H__

#include <rados/librados.h>
#include <uriparser/Uri.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <llhttp.h>

#include "util.h"
#include "md5.h"

#define MAX_AIO_OP 65536
#define MAX_FIELDS 64
#define MAX_HTTP_CLIENTS 65536

#define BUCKET_POOL "bucket_pool"
#define DATA_POOL "1mb_data_pool"

enum http_expect { CONTINUE, NONE };

extern size_t BUF_SIZE;
extern char *random_buffer;

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
//	rados_ioctx_t data_io_ctx;

	size_t object_size;
	size_t object_offset;

	size_t http_payload_size;
	bool chunked_upload;
	bool parsing;
	bool deleting;

	rados_completion_t aio_completion[MAX_AIO_OP];
	size_t num_outstanding_aio;

	size_t current_chunk_size;
	size_t current_chunk_offset;
	RollingMD5Context md5_ctx;

	xmlParserCtxtPtr xml_ctx;
};

extern __thread struct http_client *http_clients[MAX_HTTP_CLIENTS];

extern rados_t cluster;
extern rados_ioctx_t bucket_io_ctx;
extern rados_ioctx_t data_io_ctx;

struct http_client *create_http_client(int fd);
void reset_http_client(struct http_client *client);
void free_http_client(struct http_client *client);
int on_header_field_cb(llhttp_t *parser, const char *at, size_t length);
int on_header_value_cb(llhttp_t *parser, const char *at, size_t length);
void delete_objects(struct http_client *client, const char *buf, size_t length);
void put_object(struct http_client *client, const char *buf, size_t length);
int on_body_cb(llhttp_t *parser, const char *at, size_t length);
int on_url_cb(llhttp_t *parser, const char *at, size_t length);
int on_chunk_header(llhttp_t *parser);
int on_message_complete_cb(llhttp_t* parser);
int on_reset_cb(llhttp_t *parser);

void init_object_put_request(struct http_client *client);
void complete_head_request(struct http_client *client, char *datetime_str, char **response, size_t *response_size);
void complete_post_request(struct http_client *client, char *datetime_str, char **response, size_t *response_size, char **data_payload, size_t *data_payload_size);
void complete_delete_request(struct http_client *client, char *datetime_str, char **response, size_t *response_size, char **data_payload, size_t *data_payload_size);
void complete_put_request(struct http_client *client, char *datetime_str, char **response, size_t *response_size);
void complete_get_request(struct http_client *client, char *datetime_str, char **response, size_t *response_size, char **data_payload, size_t *data_payload_size);

#endif
