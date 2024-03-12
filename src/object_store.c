#include <errno.h>
#include <assert.h>
#include <string.h>

#include <time.h>

#include "object_store.h"

#define FIRST_READ_SIZE sizeof(char) * 1024 * 4096

void delete_objects(struct http_client *client, const char *buf, size_t length)
{
	xmlParseChunk(client->xml_ctx, buf, length, 0);
}

void put_object(struct http_client *client, const char *buf, size_t length)
{
	char *ptr = (char*)buf;

	if (!client->chunked_upload && client->object_name != NULL) {
		memcpy(&client->put_buf[client->object_offset], ptr, length);
		updateRollingMD5(&(client->md5_ctx), ptr, length);

		client->object_offset += length;
	}
	else {
		printf("Chunked upload not supported\n");
	}
}

void aio_head_read_callback(rados_completion_t comp, void *arg) {
	int ret = 0;
	struct http_client *client = (struct http_client*)arg;
	char datetime_str[64];
	get_datetime_str(datetime_str, 64);

	if (client->prval != 0) {
		printf("object %s not found\n", client->object_name);
		client->response_size = snprintf(NULL, 0, "%s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_NOT_FOUND_HDR, AMZ_REQUEST_ID, datetime_str) + 1;
		client->response = malloc(client->response_size);
		snprintf(client->response, client->response_size, "%s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_NOT_FOUND_HDR, AMZ_REQUEST_ID, datetime_str);
		client->response_size--;

		client->data_payload = NULL;
		client->data_payload_size = 0;

		send_response(client);
	}
	else {
		const char *attr_name = NULL;
		const char *attr_val = NULL;
		size_t attr_val_len = -1;

		size_t full_object_size = 0;

		char *etag = NULL;
		char *last_modified_datetime_str = NULL;
		while (rados_getxattrs_next(client->iter, &attr_name, &attr_val, &attr_val_len) == 0 && (attr_name != NULL & attr_val != NULL)) {
			if (strcmp(attr_name, "size") == 0) {
				full_object_size = atol(attr_val);
			}
			else if (strcmp(attr_name, "etag") == 0) {
				etag = strdup(attr_val);
			}
			else if (strcmp(attr_name, "last_modified") == 0) {
				last_modified_datetime_str = strdup(attr_val);
			}
		}
		rados_getxattrs_end(client->iter);

		client->response_size = snprintf(NULL, 0, "%s\r\nx-amz-request-id: %s\r\nContent-Length: %ld\r\nEtag: \"%s\"\r\nLast-Modified: %s\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, full_object_size, etag, last_modified_datetime_str, datetime_str) + 1;
		client->response = malloc(client->response_size);
		snprintf(client->response, client->response_size, "%s\r\nx-amz-request-id: %s\r\nContent-Length: %ld\r\nEtag: \"%s\"\r\nLast-Modified: %s\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, full_object_size, etag, last_modified_datetime_str, datetime_str);
		client->response_size--;

		if (client->object_size != full_object_size) {
			size_t tail_size = full_object_size - client->object_size;
			size_t bytes_read = client->object_size;
			client->object_size = full_object_size;
			client->data_payload = realloc(client->data_payload, client->object_size);
			client->data_payload_size = client->object_size;

			rados_aio_release(client->aio_completion);
			rados_aio_create_completion((void*)client, aio_ack_callback, aio_commit_callback, &(client->aio_completion));
			ret = rados_aio_read(*(client->data_io_ctx), client->aio_completion, client->object_name, client->data_payload + bytes_read, tail_size, bytes_read);
			assert(ret == 0);
		}
		else {
			client->data_payload_size = client->object_size;
			send_response(client);
		}
	}
}

void init_object_get_request(struct http_client *client)
{
	// getting object
	int prval; // we only need to know if getxattrs is successful
	int ret;

	if (client->bucket_name != NULL && client->object_name != NULL) {
		client->data_payload = malloc(FIRST_READ_SIZE);

		rados_release_read_op(client->read_op);
		client->read_op = rados_create_read_op();
		rados_aio_release(client->aio_head_read_completion);
		rados_aio_create_completion((void*)client, aio_head_read_callback, NULL, &(client->aio_head_read_completion));

		rados_read_op_assert_exists(client->read_op);
		rados_read_op_getxattrs(client->read_op, &(client->iter), &(client->prval));
		rados_read_op_read(client->read_op, 0, FIRST_READ_SIZE, client->data_payload, &(client->object_size), &prval);
		ret = rados_aio_read_op_operate(client->read_op, *(client->data_io_ctx), client->aio_head_read_completion, client->object_name, LIBRADOS_OPERATION_NOFLAG);
		assert(ret == 0);
	}
}

void init_objects_delete_request(struct http_client *client) {
	client->xml_ctx = xmlCreatePushParserCtxt(NULL, NULL, NULL, 0, NULL);
	client->deleting = true;
}

void complete_head_request(struct http_client *client, const char *datetime_str)
{
	int ret = 0;

	// if scopped to bucket
	if (client->bucket_name != NULL && client->object_name == NULL) {
		char buf;
		ret = rados_read(*(client->bucket_io_ctx), client->bucket_name, &buf, 0, 0);
		if (ret != 0) {
			fprintf(stderr, "Bucket %s does not exist\n", client->bucket_name);
			client->response_size = snprintf(NULL, 0, "%s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_NOT_FOUND_HDR, datetime_str) + 1;
			client->response = malloc(client->response_size);
			snprintf(client->response, client->response_size, "%s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_NOT_FOUND_HDR, datetime_str);
			client->response_size--;
		}
		else {
			client->response_size = snprintf(NULL, 0, "%s\r\nX-RGW-Object-Count: 430\r\nX-RGW-Bytes-Used: 1803550720\r\nX-RGW-Quota-User-Size: -1\r\nX-RGW-Quota-User-Objects: -1\r\nX-RGW-Quota-Max-Buckets: 5000\r\nX-RGW-Quota-Bucket-Size: -1\r\nX-RGW-Quota-Bucket-Objects: -1\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, datetime_str) + 1;
			client->response = malloc(client->response_size);
			snprintf(client->response, client->response_size, "%s\r\nX-RGW-Object-Count: 430\r\nX-RGW-Bytes-Used: 1803550720\r\nX-RGW-Quota-User-Size: -1\r\nX-RGW-Quota-User-Objects: -1\r\nX-RGW-Quota-Max-Buckets: 5000\r\nX-RGW-Quota-Bucket-Size: -1\r\nX-RGW-Quota-Bucket-Objects: -1\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, datetime_str);
			client->response_size--;
		}
		send_response(client);
	}
}

void complete_post_request(struct http_client *client, const char *datetime_str)
{
	int ret = 0;

	xmlDocPtr doc = NULL;
	xmlDocPtr response_doc = NULL;
	xmlChar *xmlbuf;
	int xmlbuf_size;

	if (client->deleting) {
		doc = client->xml_ctx->myDoc;
		if (doc != NULL) {
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

			rados_release_write_op(client->write_op);
			client->write_op = rados_create_write_op();
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

						int ret = rados_remove(*(client->data_io_ctx), (const char*)content);
						if (ret) { perror("rados_remove"); printf("%ld/%ld deleting %s %ld\n", i, num_objects, content, keys_len[i]); }

						response_node = xmlNewChild(response_root_node, NULL, BAD_CAST "Deleted", NULL);
						response_node = xmlNewChild(response_node, NULL, BAD_CAST "Key", content);
						free(content);

						i++;
					}
				}
			}

			rados_write_op_omap_rm_keys2(client->write_op, (const char**)keys, keys_len, i);
			ret = rados_write_op_operate(client->write_op, *(client->bucket_io_ctx), client->bucket_name, NULL, 0);
			assert(ret == 0);

			// dump XML document
			xmlDocDumpMemoryEnc(response_doc, &xmlbuf, &xmlbuf_size, "UTF-8");
			client->data_payload = malloc(xmlbuf_size);
			memcpy(client->data_payload, xmlbuf, xmlbuf_size);
			client->data_payload_size = xmlbuf_size;

			xmlFreeParserCtxt(client->xml_ctx);
			xmlCleanupParser();

			xmlFreeDoc(doc);
			xmlFreeDoc(response_doc);

			xmlFree(xmlbuf);

			for (size_t j = 0; j < i; j++) free(keys[j]);
		}
		else {
			xmlbuf_size = 0;
			client->data_payload_size = 0;
			client->data_payload = NULL;
		}

		client->response_size = snprintf(NULL, 0, "%s\r\nContent-Length: %d\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, xmlbuf_size, datetime_str) + 1;
		client->response = malloc(client->response_size);
		snprintf(client->response, client->response_size, "%s\r\nContent-Length: %d\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, xmlbuf_size, datetime_str);
		client->response_size--;

		client->deleting = false;

		//send_response(client);
		send(client->fd, client->response, client->response_size, 0);
		send(client->fd, client->data_payload, client->data_payload_size, 0);
	}
}

void complete_delete_request(struct http_client *client, const char *datetime_str)
{
	int ret = 0;

	if (client->bucket_name != NULL && client->object_name == NULL) {
		// delete bucket
		// check if bucket is empty
		rados_omap_iter_t iter;
		unsigned char pmore; int prval;
		char *object_name = NULL, *metadata;
		size_t object_name_len, metadata_len;

		rados_read_op_t read_op = rados_create_read_op();
		rados_read_op_omap_get_vals2(read_op, NULL, NULL, 1, &iter, &pmore, &prval);
		rados_read_op_operate(read_op, *(client->bucket_io_ctx), client->bucket_name, 0);
		rados_omap_get_next2(iter, &object_name, &metadata, &object_name_len, &metadata_len);

		if (object_name == NULL) {
			ret = rados_remove(*(client->bucket_io_ctx), client->bucket_name);
			if (ret) { perror("rados_remove"); }

			client->response_size = snprintf(NULL, 0, "HTTP/1.1 204 No Content\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", AMZ_REQUEST_ID, datetime_str) + 1;
			client->response = malloc(client->response_size);
			snprintf(client->response, client->response_size, "HTTP/1.1 204 No Content\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", AMZ_REQUEST_ID, datetime_str);
			client->response_size--;

			client->data_payload = NULL;
			client->data_payload_size = 0;
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

			client->response_size = snprintf(NULL, 0, "HTTP/1.1 409 Conflict\r\nx-amz-request-id: %s\r\nContent-Length: %d\r\nDate: %s\r\n\r\n", AMZ_REQUEST_ID, xmlbuf_size, datetime_str) + 1;
			client->response = malloc(client->response_size);
			snprintf(client->response, client->response_size, "HTTP/1.1 409 Conflict\r\nx-amz-request-id: %s\r\nContent-Length: %d\r\nDate: %s\r\n\r\n", AMZ_REQUEST_ID, xmlbuf_size, datetime_str);
			client->response_size--;

			client->data_payload = malloc(xmlbuf_size);
			memcpy(client->data_payload, xmlbuf, xmlbuf_size);
			client->data_payload_size = xmlbuf_size;

			// cleanup
			xmlFree(xmlbuf);
			xmlFreeDoc(doc);
		}

		rados_omap_get_end(iter);
		rados_release_read_op(read_op);

		send_response(client);
	}
	else if (client->bucket_name != NULL && client->object_name != NULL) {
		// delete object
		ret = rados_remove(*(client->data_io_ctx), client->object_name);
		assert(ret == 0);

		const char *key = client->object_name; const size_t keys_len[] = { strlen(key) };
		rados_release_write_op(client->write_op);
		client->write_op = rados_create_write_op();
		rados_write_op_omap_rm_keys2(client->write_op, &key, keys_len, 1);
		ret = rados_write_op_operate2(client->write_op, *(client->bucket_io_ctx), client->bucket_name, NULL, 0);
		assert(ret == 0);

		client->response_size = snprintf(NULL, 0, "HTTP/1.1 204 No Content\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", AMZ_REQUEST_ID, datetime_str) + 1;
		client->response = malloc(client->response_size);
		snprintf(client->response, client->response_size, "HTTP/1.1 204 No Content\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", AMZ_REQUEST_ID, datetime_str);

		client->data_payload = NULL;
		client->data_payload_size = 0;

		send_response(client);
	}

}

void complete_put_request(struct http_client *client, const char *datetime_str)
{
	int ret = 0;

	if (client->bucket_name != NULL && client->object_name == NULL) {
		// if scopped to bucket, create bucket
		rados_release_write_op(client->write_op);
		client->write_op = rados_create_write_op();
		rados_write_op_create(client->write_op, LIBRADOS_CREATE_EXCLUSIVE, NULL);
		ret = rados_write_op_operate2(client->write_op, *(client->bucket_io_ctx), client->bucket_name, NULL, 0);
		if (ret) {
			// bucket already exist
			client->response_size = snprintf(NULL, 0, "HTTP/1.1 400 Bad Request\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", AMZ_REQUEST_ID, datetime_str) + 1;
			client->response = malloc(client->response_size);
			snprintf(client->response, client->response_size, "HTTP/1.1 400 Bad Request\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", AMZ_REQUEST_ID, datetime_str);
			client->response_size--;
			// add BucketAlreadyExists to payload
		}
		else {
			client->response_size = snprintf(NULL, 0, "%s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, datetime_str) + 1;
			client->response = malloc(client->response_size);
			snprintf(client->response, client->response_size, "%s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, datetime_str);
			client->response_size--;
		}

		send_response(client);
	}
	else if (client->bucket_name != NULL && client->object_name != NULL) {
		// if scopped to object, create object
		char metadata[4096];
		char md5_hash[MD5_DIGEST_LENGTH * 2 + 1];
		finalizeRollingMD5(&(client->md5_ctx), md5_hash);

		rados_write_op_setxattr(client->write_op, "etag", md5_hash, strlen(md5_hash) + 1);
		rados_write_op_setxattr(client->write_op, "last_modified", datetime_str, strlen(datetime_str) + 1);
		rados_write_op_write(client->write_op, client->put_buf, client->object_size, 0);
		rados_write_op_set_alloc_hint2(client->write_op, client->object_size, client->object_size, LIBRADOS_ALLOC_HINT_FLAG_SEQUENTIAL_WRITE);

		snprintf(metadata, 4096, "%s;%s;%ld", datetime_str, md5_hash, client->object_size);
		const size_t key_lens = strlen(client->object_name);
		const size_t val_lens = strlen(metadata);
		const char *keys = client->object_name;
		const char *vals = metadata;

		client->response_size = snprintf(NULL, 0, "%s\r\nEtag: \"%s\"\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, md5_hash, AMZ_REQUEST_ID, datetime_str) + 1;
		client->response = malloc(client->response_size);
		snprintf(client->response, client->response_size, "%s\r\nEtag: \"%s\"\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, md5_hash, AMZ_REQUEST_ID, datetime_str);
		client->response_size--;

		/* store data and register callback */
		rados_aio_release(client->aio_completion);
		rados_aio_create_completion((void*)client, aio_ack_callback, aio_commit_callback, &(client->aio_completion));
		ret = rados_aio_write_op_operate2(client->write_op, *(client->data_io_ctx), client->aio_completion, client->object_name, NULL, 0);
		assert(ret == 0);

		/* register object to bucket */
		rados_release_write_op(client->write_op);
		client->write_op = rados_create_write_op();
		rados_write_op_omap_set2(client->write_op, &keys, &vals, &key_lens, &val_lens, 1);
		ret = rados_write_op_operate2(client->write_op, *(client->bucket_io_ctx), client->bucket_name, NULL, 0);
		assert(ret == 0);
	}
}

void complete_get_request(struct http_client *client, const char *datetime_str)
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
		int ret = 1;

		rados_omap_iter_t iter;
		unsigned char pmore; int prval;
		char *continue_from = NULL;
		char *prefix = NULL;

		client->read_op = rados_create_read_op();
		rados_read_op_assert_exists(client->read_op);
		rados_read_op_omap_get_vals2(client->read_op, prefix, continue_from, 5000, &iter, &pmore, &prval);
		ret = rados_read_op_operate(client->read_op, *(client->bucket_io_ctx), client->bucket_name, 0);

		if (ret != 0 || prval != 0) {
			// 404
			printf("bucket %s not found!\n", client->bucket_name),
			client->response_size = snprintf(NULL, 0, "%s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_NOT_FOUND_HDR, AMZ_REQUEST_ID, datetime_str) + 1;
			client->response = malloc(client->response_size);
			snprintf(client->response, client->response_size, "%s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_NOT_FOUND_HDR, AMZ_REQUEST_ID, datetime_str);
			client->response_size--;

			client->data_payload = NULL;
			client->data_payload_size = 0;
		}
		else {
			// go through list of queries
			bool fetch_owner = false;
			char *encoding_type = NULL;
			int list_type = 1;
			bool versioning = false;
			bool location = false;

			doc = xmlNewDoc(BAD_CAST "1.0");

			if (uriDissectQueryMallocA(&queryList, &itemCount, client->uri.query.first, client->uri.query.afterLast) == URI_SUCCESS) {
				for (struct UriQueryListStructA *query = queryList; query != NULL; query = query->next) {
					fprintf(stdout, "query: (%s,%s)\n", query->key, query->value);
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
			rados_omap_get_end(iter);

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
			client->response_size = snprintf(NULL, 0, "%s\r\nContent-Length: %d\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, xmlbuf_size, datetime_str) + 1;
			client->response = malloc(client->response_size);
			snprintf(client->response, client->response_size, "%s\r\nContent-Length: %d\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, xmlbuf_size, datetime_str);
			client->response_size--; // we don't send the null

			client->data_payload = malloc(xmlbuf_size);
			memcpy(client->data_payload, xmlbuf, xmlbuf_size);
			client->data_payload_size = xmlbuf_size;

			// cleanup
			xmlFree(xmlbuf);
			xmlFreeDoc(doc);
			xmlCleanupParser();
			if (encoding_type) free(encoding_type);
			//}
		}
		if (prefix) free(prefix);
		if (continue_from) free(continue_from);
		rados_release_read_op(client->read_op);

		//send_response(client);
		printf("response: %ld %ld\ndata: %ld %ld\n", strlen(client->response), client->response_size, strlen(client->data_payload), client->data_payload_size);
		send(client->fd, client->response, client->response_size, 0);
		send(client->fd, client->data_payload, client->data_payload_size, 0);
	}
	else if (client->bucket_name != NULL && client->object_name != NULL) {
	}
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
	}

	if (client->object_name != NULL) {
		client->put_buf = malloc(client->object_size);
		initRollingMD5(&(client->md5_ctx));
		size_t obj_size_str_len = snprintf(NULL, 0, "%ld", client->object_size) + 1;
		char *obj_size_str = malloc(obj_size_str_len);
		snprintf(obj_size_str, obj_size_str_len, "%ld", client->object_size) * sizeof(char);

		rados_release_write_op(client->write_op);
		client->write_op = rados_create_write_op();
		rados_write_op_create(client->write_op, LIBRADOS_CREATE_EXCLUSIVE, NULL);
		rados_write_op_setxattr(client->write_op, "size", obj_size_str, obj_size_str_len);
	}
}
