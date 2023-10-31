#ifndef __HTTP_CLIENT_H__
#define __HTTP_CLIENT_H__

#include "md5.h"

#define MAX_AIO_OP 1000
#define MAX_FIELDS 64

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
	bool deleting;

	rados_completion_t aio_completion[MAX_AIO_OP];
	size_t num_outstanding_aio;

	size_t current_chunk_size;
	size_t current_chunk_offset;
	RollingMD5Context md5_ctx;

	xmlParserCtxtPtr xml_ctx;
};

#endif
