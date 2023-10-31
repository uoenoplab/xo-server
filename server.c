#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <signal.h>
#include <sys/epoll.h>
#include <errno.h>
#include <sys/uio.h>

#include <rados/librados.h>
#include <uriparser/Uri.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <llhttp.h>

#include "http_client.h"
#include "md5.h"
#include "util.h"

#define PORT 8080
#define MAX_EVENTS 1000
#define MAX_HTTP_CLIENTS 65536

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

static char *AMZ_REQUEST_ID = (char*)"tx000009a75d393f1564ec2-0065202454-3771-default";

static bool server_running = true;

struct http_client *http_clients[MAX_HTTP_CLIENTS] = { NULL };
rados_t cluster;
char *client_data_buffer;
size_t BUF_SIZE = sizeof(char) * 1024 * 4096;

void handleCtrlC(int signum) {
	printf("Received Ctrl+C. Stopping the server...\n");
	server_running = false; // Set the flag to stop the server gracefully.
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

	if (client->uri_str) free(client->uri_str);
	client->uri_str = NULL;

	if (client->object_name) free(client->object_name);
	client->object_name = NULL;

	if (client->bucket_name) free(client->bucket_name);
	client->bucket_name = NULL;

	client->chunked_upload = false;
	client->object_size = 0;
	client->object_offset = 0;
	client->http_payload_size = 0;
	client->parsing = false;
	client->deleting = false;
	client->num_outstanding_aio = 0;

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
	//printf("free client\n");
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

void delete_objects(struct http_client *client, const char *buf, size_t length)
{
	//printf("delete\n");
	xmlParseChunk(client->xml_ctx, buf, length, 0);
}

void put_object(struct http_client *client, const char *buf, size_t length)
{
	int ret;
	char *ptr = (char*)buf;
	size_t current_chunk_size = client->current_chunk_size;
	size_t current_chunk_offset = client->current_chunk_offset;

	if (!client->chunked_upload) {
		//printf("length:%ld offset:%ld\n", length, client->object_offset);
		ret = rados_write(client->data_io_ctx, client->object_name, ptr, length, client->object_offset);
		if (ret) {
			perror("rados_write");
			exit(1);
		}

		//rados_aio_create_completion(NULL, NULL, NULL, &client->aio_completion[client->num_outstanding_aio]);
		//ret = rados_aio_write(client->data_io_ctx, client->object_name, client->aio_completion[client->num_outstanding_aio++], ptr, length, client->object_offset);
		//if (ret) {
		//	perror("rados_write");
		//	exit(1);
		//}
		updateRollingMD5(&(client->md5_ctx), ptr, length);
		client->object_offset += length;
		client->current_chunk_size += length;
	}
	else {
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
				char *chunk_data_start = memmem(chunk_size_end, length, "\r\n", 2);
				if (chunk_data_start == NULL) {
					//printf("chunk start is null!!!\n");
					break;
				}
				else {
					chunk_data_start += 2;
				}
				//printf("chunk_size: %ld ; ptr %.*s ; object size %ld object offset %ld\n", current_chunk_size, (int)(chunk_data_start - chunk_size_start), ptr, client->object_size, client->object_offset); 
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
				//rados_aio_create_completion(NULL, NULL, NULL, &client->aio_completion[client->num_outstanding_aio]);
				//ret = rados_aio_write(client->data_io_ctx, client->object_name, client->aio_completion[client->num_outstanding_aio++], chunk_data_start, data_len, client->object_offset);
				if (ret) {
					perror("rados_write");
					exit(1);
				}
				updateRollingMD5(&(client->md5_ctx), chunk_data_start, data_len);
	
				client->object_offset += data_len;
				current_chunk_offset += data_len;
				ptr = chunk_data_end;
				length -= data_len;
	
				free(chunk_size_str);
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
	
				size_t data_len = chunk_data_end - chunk_data_start;
	
				//printf("(cont.) chunk_size: %ld ; ptr %.16s ; object size %ld object offset %ld\n", current_chunk_size, ptr, client->object_size, client->object_offset); 
				ret = rados_write(client->data_io_ctx, client->object_name, chunk_data_start, data_len, client->object_offset);
				//rados_aio_create_completion(NULL, NULL, NULL, &client->aio_completion[client->num_outstanding_aio]);
				//ret = rados_aio_write(client->data_io_ctx, client->object_name, client->aio_completion[client->num_outstanding_aio++], chunk_data_start, data_len, client->object_offset);
				if (ret) {
					perror("rados_write");
					exit(1);
				}
				updateRollingMD5(&(client->md5_ctx), chunk_data_start, data_len);
	
				if (current_chunk_offset + length > current_chunk_size) {
					ptr = chunk_data_end + 2;
				}
				else {
					ptr = chunk_data_end;
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
				//client->parsing = false;
				//llhttp_finish(&(client->parser));
				//printf("object size now same as object offset (length of buf %ld): %s\n", length, ptr);
				break;
			}
		}
	}
}

int on_body_cb(llhttp_t *parser, const char *at, size_t length)
{
	struct http_client *client = (struct http_client*)parser->data;
	if (client->method == HTTP_PUT) {
		//printf("on body, turn on parsing, at(%ld): %.88s\n", length, at);
		//client->parsing = true;
		put_object(client, at, length);
	}
	else if (client->method == HTTP_POST) {
		if (client->deleting == true)
			delete_objects(client, at, length);

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

void init_object_put_request(struct http_client *client) {
	for (size_t i = 0; i < client->num_fields; i++) {
		if (strcasecmp(client->header_fields[i], "Content-Length") == 0) {
			client->http_payload_size = atol(client->header_values[i]);
		}
		else if (strcasecmp(client->header_fields[i], "X-Amz-Decoded-Content-Length") == 0) {
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

void init_objects_delete_request(struct http_client *client) {
	client->xml_ctx = xmlCreatePushParserCtxt(NULL, NULL, NULL, 0, NULL);
	client->deleting = true;
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
					//printf("init delete\n");
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

void complete_head_request(struct http_client *client, char *datetime_str, char **response, size_t *response_size)
{
	int ret = 0;

	// if scopped to bucket
	if (client->bucket_name != NULL && client->object_name == NULL) {
		char buf;
		ret = rados_read(client->bucket_io_ctx, client->bucket_name, &buf, 0, 0);
		if (ret != 0) {
			fprintf(stderr, "Bucket %s does not exist\n", client->bucket_name);
			*response_size = snprintf(NULL, 0, "%s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_NOT_FOUND_HDR, datetime_str) + 1;
			*response = malloc(*response_size);
			snprintf(*response, *response_size, "%s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_NOT_FOUND_HDR, datetime_str);
		}
		else {
			*response_size = snprintf(NULL, 0, "%s\r\nX-RGW-Object-Count: 430\r\nX-RGW-Bytes-Used: 1803550720\r\nX-RGW-Quota-User-Size: -1\r\nX-RGW-Quota-User-Objects: -1\r\nX-RGW-Quota-Max-Buckets: 5000\r\nX-RGW-Quota-Bucket-Size: -1\r\nX-RGW-Quota-Bucket-Objects: -1\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, datetime_str) + 1;
			*response = malloc(*response_size);
			snprintf(*response, *response_size, "%s\r\nX-RGW-Object-Count: 430\r\nX-RGW-Bytes-Used: 1803550720\r\nX-RGW-Quota-User-Size: -1\r\nX-RGW-Quota-User-Objects: -1\r\nX-RGW-Quota-Max-Buckets: 5000\r\nX-RGW-Quota-Bucket-Size: -1\r\nX-RGW-Quota-Bucket-Objects: -1\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, datetime_str);
		}
	}
}

void complete_post_request(struct http_client *client, char *datetime_str, char **response, size_t *response_size, char **data_payload, size_t *data_payload_size)
{
	xmlDocPtr doc = NULL;
	xmlDocPtr response_doc = NULL;
	xmlChar *xmlbuf;
	int xmlbuf_size;

	if (client->deleting) {
		doc = client->xml_ctx->myDoc;
		response_doc = xmlNewDoc(BAD_CAST "1.0");
		xmlNodePtr response_node;

		xmlNodePtr response_root_node = xmlNewNode(NULL, BAD_CAST "DeleteResult");
		xmlNewProp(response_root_node, BAD_CAST "xmlns", BAD_CAST "http://s3.amazonaws.com/doc/2006-03-01/");
		xmlDocSetRootElement(response_doc, response_root_node);

		xmlNodePtr root = xmlDocGetRootElement(doc);
		xmlNodePtr node = root->children;
		xmlNodePtr ptr;

		size_t num_objects = 65536; //xmlChildElementCount(node);
		size_t i;

		rados_write_op_t write_op = rados_create_write_op();
		char *keys[num_objects];
		size_t keys_len[num_objects];

		for (ptr = node, i = 0; ptr != NULL; ptr = ptr->next) {
			if (strcmp((const char*)ptr->name, "Object") == 0) {
				xmlNodePtr key_node = ptr->children;
				while (key_node->type != XML_ELEMENT_NODE) key_node = key_node->next;
				xmlChar* content = xmlNodeListGetString(doc, key_node->children, 1);
				if (content) {
					keys[i] = strdup((const char*)content);
					keys_len[i] = strlen((const char*)content);

					int ret = rados_remove(client->data_io_ctx, (const char*)content);
					if (ret) { perror("rados_remove"); printf("%ld/%ld deleting %s %ld\n", i, num_objects, content, keys_len[i]); }

					response_node = xmlNewChild(response_root_node, NULL, BAD_CAST "Deleted", NULL);
					response_node = xmlNewChild(response_node, NULL, BAD_CAST "Key", content);
					free(content);

					i++;
				}
			}
		}

		rados_write_op_omap_rm_keys2(write_op, (const char**)keys, keys_len, i);
		rados_write_op_operate(write_op, client->bucket_io_ctx, client->bucket_name, NULL, 0);
		rados_release_write_op(write_op);

		// dump XML document
		xmlDocDumpMemoryEnc(response_doc, &xmlbuf, &xmlbuf_size, "UTF-8");
		*response_size = snprintf(NULL, 0, "%s\r\nContent-Length: %d\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, xmlbuf_size, datetime_str) + 1;
		*response = malloc(*response_size);
		snprintf(*response, *response_size, "%s\r\nContent-Length: %d\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, xmlbuf_size, datetime_str);

		*data_payload = malloc(xmlbuf_size);
		memcpy(*data_payload, xmlbuf, xmlbuf_size);
		*data_payload_size = xmlbuf_size;

		xmlFreeParserCtxt(client->xml_ctx);
		xmlCleanupParser();

		xmlFreeDoc(doc);
		xmlFreeDoc(response_doc);

		xmlFree(xmlbuf);
		client->deleting = false;
		for (size_t j = 0; j < i; j++) free(keys[j]);
	}
}

void complete_delete_request(struct http_client *client, char *datetime_str, char **response, size_t *response_size, char **data_payload, size_t *data_payload_size)
{
	int ret = 0;

	if (client->bucket_name != NULL && client->object_name == NULL) {
		// delete bucket
		// check if bucket is empty
		rados_omap_iter_t iter;
		unsigned char pmore; int prval;
		char *object_name, *metadata;
		size_t object_name_len, metadata_len;

		rados_read_op_t read_op = rados_create_read_op();
		rados_read_op_omap_get_vals2(read_op, NULL, NULL, 1, &iter, &pmore, &prval);
		rados_read_op_operate(read_op, client->bucket_io_ctx, client->bucket_name, 0);
		rados_omap_get_next2(iter, &object_name, &metadata, &object_name_len, &metadata_len);

		if (object_name == NULL) {
			ret = rados_remove(client->bucket_io_ctx, client->bucket_name);
			if (ret) { perror("rados_remove"); }

			*response_size = snprintf(NULL, 0, "HTTP/1.1 204 No Content\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", AMZ_REQUEST_ID, datetime_str) + 1;
			*response = malloc(*response_size);
			snprintf(*response, *response_size, "HTTP/1.1 204 No Content\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", AMZ_REQUEST_ID, datetime_str);

			*data_payload = NULL;
			*data_payload_size = 0;
		}
		else {
			// not empty
			// 409 Conflict
			xmlDocPtr doc = NULL;
			xmlNodePtr root_node = NULL;

			xmlChar *xmlbuf;
			int xmlbuf_size;

			doc = xmlNewDoc(BAD_CAST "1.0");
			root_node = xmlNewNode(NULL, BAD_CAST "Error");
			xmlDocSetRootElement(doc, root_node);

			xmlNewChild(root_node, NULL, BAD_CAST "Code", BAD_CAST "BucketNotEmpty");
			xmlNewChild(root_node, NULL, BAD_CAST "BucketName", (const unsigned char*)client->bucket_name);
			xmlNewChild(root_node, NULL, BAD_CAST "HostId", BAD_CAST "facc-default-default");

			// dump response xml
			xmlDocDumpMemoryEnc(doc, &xmlbuf, &xmlbuf_size, "UTF-8");

			*response_size = snprintf(NULL, 0, "HTTP/1.1 409 Conflict\r\nx-amz-request-id: %s\r\nContent-Length: %d\r\nDate: %s\r\n\r\n", AMZ_REQUEST_ID, xmlbuf_size, datetime_str) + 1;
			*response = malloc(*response_size);
			snprintf(*response, *response_size, "HTTP/1.1 409 Conflict\r\nx-amz-request-id: %s\r\nContent-Length: %d\r\nDate: %s\r\n\r\n", AMZ_REQUEST_ID, xmlbuf_size, datetime_str);

			*data_payload = malloc(xmlbuf_size);
			memcpy(*data_payload, xmlbuf, xmlbuf_size);
			*data_payload_size = xmlbuf_size;

			// cleanup
			xmlFree(xmlbuf);
			xmlFreeDoc(doc);
		}

		rados_omap_get_end(iter);
		rados_release_read_op(read_op);
	}
	else if (client->bucket_name != NULL && client->object_name != NULL) {
		// delete object
		ret = rados_remove(client->data_io_ctx, client->object_name);

		const char *key = client->object_name; const size_t keys_len[] = { strlen(key) };
		rados_write_op_t write_op = rados_create_write_op();
		rados_write_op_omap_rm_keys2(write_op, &key, keys_len, 1);
		rados_write_op_operate2(write_op, client->bucket_io_ctx, client->bucket_name, NULL, 0);
		rados_release_write_op(write_op);

		*response_size = snprintf(NULL, 0, "HTTP/1.1 204 No Content\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", AMZ_REQUEST_ID, datetime_str) + 1;
		*response = malloc(*response_size);
		snprintf(*response, *response_size, "HTTP/1.1 204 No Content\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", AMZ_REQUEST_ID, datetime_str);

		*data_payload = NULL;
		*data_payload_size = 0;
	}
}

void complete_put_request(struct http_client *client, char *datetime_str, char **response, size_t *response_size)
{
	if (client->bucket_name != NULL && client->object_name == NULL) {
		// if scopped to bucket, create bucket
		rados_write_op_t write_op = rados_create_write_op();
		rados_write_op_operate2(write_op, client->bucket_io_ctx, client->bucket_name, NULL, 0);
		rados_release_write_op(write_op);

		*response_size = snprintf(NULL, 0, "%s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, datetime_str) + 1;
		*response = malloc(*response_size);
		snprintf(*response, *response_size, "%s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, datetime_str);
	}
	else if (client->bucket_name != NULL && client->object_name != NULL) {
		// if scopped to object, create object
		char metadata[4096];
		char md5_hash[MD5_DIGEST_LENGTH * 2 + 1];

		finalizeRollingMD5(&(client->md5_ctx), md5_hash);

		//for (size_t i = 0; i < client->num_outstanding_aio; i++) {
		//	rados_aio_wait_for_complete(client->aio_completion[i]);
		//	rados_aio_release(client->aio_completion[i]);
		//}

		snprintf(metadata, 4096, "%s;%s;%ld", datetime_str, md5_hash, client->object_size);
		const size_t key_lens = strlen(client->object_name);
		const size_t val_lens = strlen(metadata);
		const char *keys = client->object_name;
		const char *vals = metadata;

		rados_write_op_t write_op = rados_create_write_op();
		rados_write_op_omap_set2(write_op, &keys, &vals, &key_lens, &val_lens, 1);
		rados_write_op_operate2(write_op, client->bucket_io_ctx, client->bucket_name, NULL, 0);
		rados_release_write_op(write_op);

		*response_size = snprintf(NULL, 0, "%s\r\nEtag: %s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, md5_hash, AMZ_REQUEST_ID, datetime_str) + 1;
		*response = malloc(*response_size);
		snprintf(*response, *response_size, "%s\r\nEtag: %s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, md5_hash, AMZ_REQUEST_ID, datetime_str);

		object_count++;
	}
}

void complete_get_request(struct http_client *client, char *datetime_str, char **response, size_t *response_size, char **data_payload, size_t *data_payload_size)
{
	int ret = 0;

	UriQueryListA *queryList;
	int itemCount;

	xmlDocPtr doc = NULL;
	xmlNodePtr root_node = NULL, node = NULL;/* node pointers */

	xmlChar *xmlbuf;
	int xmlbuf_size;

	if (client->bucket_name != NULL && client->object_name == NULL) {
		// if GET bucket service: no objects
		// check if bucket exist
		int ret; char buf;
		ret = rados_read(client->bucket_io_ctx, client->bucket_name, &buf, 0, 0);
		if (ret != 0) {
			// 404
			printf("bucket %s not found!\n", client->bucket_name),
			*response_size = snprintf(NULL, 0, "%s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_NOT_FOUND_HDR, AMZ_REQUEST_ID, datetime_str) + 1;
			*response = malloc(*response_size);
			snprintf(*response, *response_size, "%s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_NOT_FOUND_HDR, AMZ_REQUEST_ID, datetime_str);

			*data_payload = NULL;
			*data_payload_size = 0;
		}
		else {
			// go through list of queries
			bool fetch_owner = false;
			char *prefix = NULL;
			char *encoding_type = NULL;
			int list_type = 1;
			bool versioning = false;
			bool location = false;
			char *continue_from = NULL;

			doc = xmlNewDoc(BAD_CAST "1.0");

			if (uriDissectQueryMallocA(&queryList, &itemCount, client->uri.query.first, client->uri.query.afterLast) == URI_SUCCESS) {
				for (struct UriQueryListStructA *query = queryList; query != NULL; query = query->next) {
					//fprintf(stdout, "query: (%s,%s)\n", query->key, query->value);
					if (strcmp(query->key, "location") == 0) {
						location = true;
					}
					if (strcmp(query->key, "versioning") == 0){
						versioning = true;
					}
					if (strcmp(query->key, "fetch-owner") == 0) {
						if (strcmp(query->value, "true") == 0)
							fetch_owner = true;
					}
					if (strcmp(query->key, "list-type") == 0) {
						if (strcmp(query->value, "2") == 0)
							list_type = 2;
					}
					if (strcmp(query->key, "prefix") == 0) {
						prefix = strdup(query->value);
					}
					if (strcmp(query->key, "encoding-type") == 0) {
						encoding_type = strdup(query->value);
					}
					if (strcmp(query->key, "continuation-token") == 0) {
						continue_from = strdup(query->value);
					}
				}
				uriFreeQueryListA(queryList);
			}

			// return list inside bucket
			//if (fetch_owner && list_type == 2) {
			rados_omap_iter_t iter;
			unsigned char pmore; int prval;
			rados_read_op_t read_op = rados_create_read_op();

			rados_read_op_omap_get_vals2(read_op, prefix, continue_from, 5000, &iter, &pmore, &prval);
			rados_read_op_operate(read_op, client->bucket_io_ctx, client->bucket_name, 0);

			root_node = xmlNewNode(NULL, BAD_CAST "ListBucketResult");
			xmlNewProp(root_node, BAD_CAST "xmlns", BAD_CAST "http://s3.amazonaws.com/doc/2006-03-01/");
			xmlDocSetRootElement(doc, root_node);

			node = xmlNewChild(root_node, NULL, BAD_CAST "EncodingType", BAD_CAST encoding_type);
			node = xmlNewChild(root_node, NULL, BAD_CAST "Name", BAD_CAST client->bucket_name);
			if (prefix)
				node = xmlNewChild(root_node, NULL, BAD_CAST "Prefix", BAD_CAST prefix);
			if (encoding_type)
				node = xmlNewChild(root_node, NULL, BAD_CAST "EncodingType", BAD_CAST encoding_type);

			// for loop
			char *object_name, *metadata;
			char *last_obj_name = NULL;
			size_t object_name_len, metadata_len;
			while(rados_omap_get_next2(iter, &object_name, &metadata, &object_name_len, &metadata_len) == 0 && \
					(object_name != NULL && metadata != NULL && object_name_len != 0 && metadata_len != 0)) {
				char last_modified_datetime_str[4096];
				metadata[metadata_len] = 0;
				char *data = strdup(metadata);
				if (last_obj_name != NULL) { free(last_obj_name); last_obj_name = NULL; }
				if (pmore) last_obj_name = strdup(object_name);

				xmlNodePtr content_node = xmlNewChild(root_node, NULL, BAD_CAST "Contents", NULL);
				node = xmlNewChild(content_node, NULL, BAD_CAST "Key", BAD_CAST object_name);

				char *token = strtok(data, ";");
				convertToISODateTime(token, last_modified_datetime_str);
				node = xmlNewChild(content_node, NULL, BAD_CAST "LastModified", BAD_CAST last_modified_datetime_str);
				token = strtok(NULL, ";");
				node = xmlNewChild(content_node, NULL, BAD_CAST "Etag", BAD_CAST token);
				token = strtok(NULL, ";");
				node = xmlNewChild(content_node, NULL, BAD_CAST "Size", BAD_CAST token);
				node = xmlNewChild(content_node, NULL, BAD_CAST "StorageClass", BAD_CAST "STANDARD");

				xmlNodePtr owner_node = xmlNewChild(content_node, NULL, BAD_CAST "Owner", NULL);
				node = xmlNewChild(owner_node, NULL, BAD_CAST "ID", BAD_CAST "admin");
				node = xmlNewChild(owner_node, NULL, BAD_CAST "DisplayName", BAD_CAST "admin");
				node = xmlNewChild(content_node, NULL, BAD_CAST "Type", BAD_CAST "Nomal");

				free(data);
			}

			if (pmore) {
				// cont. token should not be object name, but...
				node = xmlNewChild(root_node, NULL, BAD_CAST "NextContinuationToken", BAD_CAST last_obj_name);
				node = xmlNewChild(root_node, NULL, BAD_CAST "IsTruncated", BAD_CAST "true");
			}
			else {
				node = xmlNewChild(root_node, NULL, BAD_CAST "IsTruncated", BAD_CAST "false");
			}

			if (continue_from)
				node = xmlNewChild(root_node, NULL, BAD_CAST "ContinuationToken", BAD_CAST continue_from);

			if (last_obj_name != NULL) free(last_obj_name);
			rados_release_read_op(read_op);

			if (location) {
				node = xmlNewChild(root_node, NULL, BAD_CAST "LocationConstraint", BAD_CAST "default");
				xmlNewProp(node, BAD_CAST "xmlns", BAD_CAST "http://s3.amazonaws.com/doc/2006-03-01/");
			}
			if (versioning) {
				node = xmlNewChild(root_node, NULL, BAD_CAST "VersioningConfiguration", NULL);
				xmlNewProp(node, BAD_CAST "xmlns", BAD_CAST "http://s3.amazonaws.com/doc/2006-03-01/");
			}

			// dump XML document
			xmlDocDumpMemoryEnc(doc, &xmlbuf, &xmlbuf_size, "UTF-8");
			*response_size = snprintf(NULL, 0, "%s\r\nContent-Length: %d\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, xmlbuf_size, datetime_str) + 1;
			*response = malloc(*response_size);
			snprintf(*response, *response_size, "%s\r\nContent-Length: %d\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, xmlbuf_size, datetime_str);

			*data_payload = malloc(xmlbuf_size);
			memcpy(*data_payload, xmlbuf, xmlbuf_size);
			*data_payload_size = xmlbuf_size;

			// cleanup
			xmlFree(xmlbuf);
			xmlFreeDoc(doc);
			xmlCleanupParser();
			if (prefix) free(prefix);
			if (encoding_type) free(encoding_type);
			if (continue_from) free(continue_from);
			//}
		}
	}
	else if (client->bucket_name != NULL && client->object_name != NULL) {
		// getting object
		rados_omap_iter_t iter;
		int omap_prval;

		char *object_name, *metadata;
		size_t object_name_len, metadata_len;

		char last_modified_datetime_str[4096];
		char *etag;

		rados_read_op_t read_op = rados_create_read_op();
		rados_read_op_omap_get_vals2(read_op, NULL, client->object_name, 1, &iter, NULL, &omap_prval);
		rados_read_op_operate(read_op, client->bucket_io_ctx, client->bucket_name, 0);
		rados_omap_get_next2(iter, &object_name, &metadata, &object_name_len, &metadata_len);

		if (object_name == NULL || metadata == NULL) {
			printf("object %s not found\n", client->object_name);
			*response_size = snprintf(NULL, 0, "%s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_NOT_FOUND_HDR, AMZ_REQUEST_ID, datetime_str) + 1;
			*response = malloc(*response_size);
			snprintf(*response, *response_size, "%s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_NOT_FOUND_HDR, AMZ_REQUEST_ID, datetime_str);

			*data_payload = NULL;
			*data_payload_size = 0;
		}
		else {
			//metadata[metadata_len] = 0;
			char *data = strdup(metadata);

			char *token = strtok(data, ";");
			strncpy(last_modified_datetime_str, token, 4096);
			//convertToISODateTime(token, last_modified_datetime_str);
			token = strtok(NULL, ";");
			etag = strdup(token);
			token = strtok(NULL, ";");
			client->object_size = atol(token);
			//printf("got metadata: %s %s %ld\n", last_modified_datetime_str, etag, client->object_size);

			*data_payload_size = client->object_size;
			*data_payload = malloc(client->object_size);
			ret = rados_read(client->data_io_ctx, client->object_name, *data_payload, client->object_size, 0);

			*response_size = snprintf(NULL, 0, "%s\r\nx-amz-request-id: %s\r\nContent-Length: %ld\r\nEtag: %s\r\nLast-Modified: %s\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, client->object_size, etag, last_modified_datetime_str, datetime_str) + 1;
			*response = malloc(*response_size);
			snprintf(*response, *response_size, "%s\r\nx-amz-request-id: %s\r\nContent-Length: %ld\r\nEtag: %s\r\nLast-Modified: %s\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, client->object_size, etag, last_modified_datetime_str, datetime_str);

			free(etag);
			free(data);
		}

		rados_release_read_op(read_op);
	}
}

int on_chunk_header(llhttp_t *parser)
{
	return 0;
}

int on_message_complete_cb(llhttp_t* parser)
{
	int ret = 0;

	size_t response_size = 0;
	char *response = NULL;

	char *data_payload = NULL;
	size_t data_payload_size = 0;

	struct iovec iov[3];
	size_t iov_count = 1;

	struct http_client *client = (struct http_client*)parser->data;

	char datetime_str[64];
	get_datetime_str(datetime_str, 64);

	if (client->method == HTTP_HEAD) {
		// if head
		complete_head_request(client, datetime_str, &response, &response_size);
	}
	else if (client->method == HTTP_PUT) {
		// if put
		complete_put_request(client, datetime_str, &response, &response_size);
	}
	else if (client->method == HTTP_POST) {
		// if post
		complete_post_request(client, datetime_str, &response, &response_size, &data_payload, &data_payload_size);
	}
	else if (client->method == HTTP_GET) {
		complete_get_request(client, datetime_str, &response, &response_size, &data_payload, &data_payload_size);
	}
	else if (client->method == HTTP_DELETE) {
		complete_delete_request(client, datetime_str, &response, &response_size, &data_payload, &data_payload_size);
	}
	else {
		// DEBUG
		response_size = snprintf(NULL, 0, "%s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, datetime_str) + 1;
		response = malloc(response_size);
		snprintf(response, response_size, "%s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, datetime_str);
	}

	iov[0].iov_base = response;
	iov[0].iov_len = strlen(response);

	if (data_payload != NULL) {
		iov[1].iov_base = data_payload;
		iov[1].iov_len = data_payload_size;
		iov_count++;
	}

	ret = writev(client->fd, iov, iov_count);
	if (ret == -1) perror("writev");

	free(response);
	if (data_payload != NULL) free(data_payload);
	reset_http_client(client);

	return 0;
}

int on_reset_cb(llhttp_t *parser)
{
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

	printf("Accepted connection (%d) from %s:%d\n", new_socket, inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

	// Add the new client socket to the epoll event list
	struct epoll_event event;
	//event.events = EPOLLIN | EPOLLET; // Edge-triggered mode
	event.events = EPOLLIN; // Edge-triggered mode
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

	memset(client_data_buffer, 0, BUF_SIZE);
	bytes_received = recv(client_fd, client_data_buffer, BUF_SIZE, 0);
	if (bytes_received <= 0) {
		// Client closed the connection or an error occurred
		if (bytes_received == 0) {
			printf("Client disconnected: %d\n", client_fd);
		} else {
			perror("recv");
		}

		// Remove the client socket from the epoll event list
		handle_client_disconnect(epoll_fd, client_fd); // Handle client disconnection
		http_clients[client_fd] = NULL;
		//break;
		return;
	}

	struct http_client *client = http_clients[client_fd];
	enum llhttp_errno ret;

	// Echo the received data back to the client
	ret = llhttp_execute(&(client->parser), client_data_buffer, bytes_received);
	if (ret != HPE_OK) {
		fprintf(stderr, "Parse error: %s %s\n", llhttp_errno_name(ret), client->parser.reason);
	}
}

int main(int argc, char *argv[])
{
	int server_fd, epoll_fd, event_count;
	struct sockaddr_in server_addr, client_addr;
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

	signal(SIGINT, handleCtrlC);
	printf("Server is listening on port %d\n", PORT);

	while (server_running) {
		// Wait for events using epoll
		event_count = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
		if (event_count == -1) {
			if (errno == EINTR) {
				continue;
			}
			else {
				perror("epoll_wait");
				exit(EXIT_FAILURE);
			}
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

	for (size_t i = 0; i < MAX_HTTP_CLIENTS; i++) {
		if (http_clients[i] != NULL) free_http_client(http_clients[i]);
	}
	free(client_data_buffer);
	close(server_fd);
	close(epoll_fd);
	rados_shutdown(cluster);
	printf("Server terminated!\n");

	return 0;
}
