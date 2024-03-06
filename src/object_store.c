#include <errno.h>
#include <assert.h>
#include <string.h>

#include <time.h>

#include "object_store.h"

int object_count = 0;

void delete_objects(struct http_client *client, const char *buf, size_t length)
{
	xmlParseChunk(client->xml_ctx, buf, length, 0);
}

void put_object(struct http_client *client, const char *buf, size_t length)
{
	int ret;
	char *ptr = (char*)buf;

	if (!client->chunked_upload && client->object_name != NULL) {
		memcpy(&client->put_buf[client->object_offset], buf, length);
		updateRollingMD5(&(client->md5_ctx), ptr, length);

		client->object_offset += length;
		client->outstanding_aio_count++;
	}
	else {
		printf("Chunked upload not supported\n");
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
		rados_set_alloc_hint2(*(client->data_io_ctx), client->object_name, client->object_size, client->object_size, LIBRADOS_ALLOC_HINT_FLAG_SEQUENTIAL_WRITE);
		initRollingMD5(&(client->md5_ctx));
	}
}

void init_objects_delete_request(struct http_client *client) {
	client->xml_ctx = xmlCreatePushParserCtxt(NULL, NULL, NULL, 0, NULL);
	client->deleting = true;
}

void complete_head_request(struct http_client *client, char *datetime_str, char **response, size_t *response_size)
{
	int ret = 0;

	// if scopped to bucket
	if (client->bucket_name != NULL && client->object_name == NULL) {
		char buf;
		ret = rados_read(*(client->bucket_io_ctx), client->bucket_name, &buf, 0, 0);
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

						int ret = rados_remove(*(client->data_io_ctx), (const char*)content);
						if (ret) { perror("rados_remove"); printf("%ld/%ld deleting %s %ld\n", i, num_objects, content, keys_len[i]); }

						response_node = xmlNewChild(response_root_node, NULL, BAD_CAST "Deleted", NULL);
						response_node = xmlNewChild(response_node, NULL, BAD_CAST "Key", content);
						free(content);

						i++;
					}
				}
			}

			rados_write_op_omap_rm_keys2(write_op, (const char**)keys, keys_len, i);
			//ret = rados_write_op_operate(write_op, client->bucket_io_ctx(client->bucket_io_ctx), client->bucket_name, NULL, 0);
			ret = rados_write_op_operate(write_op, *(client->bucket_io_ctx), client->bucket_name, NULL, 0);
			assert(ret == 0);
			rados_release_write_op(write_op);

			// dump XML document
			xmlDocDumpMemoryEnc(response_doc, &xmlbuf, &xmlbuf_size, "UTF-8");
			*data_payload = malloc(xmlbuf_size);
			memcpy(*data_payload, xmlbuf, xmlbuf_size);
			*data_payload_size = xmlbuf_size;

			xmlFreeParserCtxt(client->xml_ctx);
			xmlCleanupParser();

			xmlFreeDoc(doc);
			xmlFreeDoc(response_doc);

			xmlFree(xmlbuf);

			for (size_t j = 0; j < i; j++) free(keys[j]);
		}
		else {
			xmlbuf_size = 0;
			*data_payload_size = 0;
			*data_payload = NULL;
		}

		*response_size = snprintf(NULL, 0, "%s\r\nContent-Length: %d\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, xmlbuf_size, datetime_str) + 1;
		*response = malloc(*response_size);
		snprintf(*response, *response_size, "%s\r\nContent-Length: %d\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, xmlbuf_size, datetime_str);

		client->deleting = false;
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
		char *object_name = NULL, *metadata;
		size_t object_name_len, metadata_len;

		rados_read_op_t read_op = rados_create_read_op();
		rados_read_op_omap_get_vals2(read_op, NULL, NULL, 1, &iter, &pmore, &prval);
		rados_read_op_operate(read_op, *(client->bucket_io_ctx), client->bucket_name, 0);
		rados_omap_get_next2(iter, &object_name, &metadata, &object_name_len, &metadata_len);

		if (object_name == NULL) {
			ret = rados_remove(*(client->bucket_io_ctx), client->bucket_name);
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
		ret = rados_remove(*(client->data_io_ctx), client->object_name);
		assert(ret == 0);

		const char *key = client->object_name; const size_t keys_len[] = { strlen(key) };
		rados_write_op_t write_op = rados_create_write_op();
		rados_write_op_omap_rm_keys2(write_op, &key, keys_len, 1);
		ret = rados_write_op_operate2(write_op, *(client->bucket_io_ctx), client->bucket_name, NULL, 0);
		assert(ret == 0);
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
	int ret = 0;

	if (client->bucket_name != NULL && client->object_name == NULL) {
		// if scopped to bucket, create bucket
		rados_write_op_t write_op = rados_create_write_op();
		rados_write_op_create(write_op, LIBRADOS_CREATE_EXCLUSIVE, NULL);
		ret = rados_write_op_operate2(write_op, *(client->bucket_io_ctx), client->bucket_name, NULL, 0);
		if (ret) {
			// bucket already exist
			*response_size = snprintf(NULL, 0, "HTTP/1.1 400 Bad Request\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", AMZ_REQUEST_ID, datetime_str) + 1;
			*response = malloc(*response_size);
			snprintf(*response, *response_size, "HTTP/1.1 400 Bad Request\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", AMZ_REQUEST_ID, datetime_str);
			// add BucketAlreadyExists to payload
		}
		else {
			*response_size = snprintf(NULL, 0, "%s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, datetime_str) + 1;
			*response = malloc(*response_size);
			snprintf(*response, *response_size, "%s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, datetime_str);
		}
		rados_release_write_op(write_op);
	}
	else if (client->bucket_name != NULL && client->object_name != NULL) {
		// if scopped to object, create object
		char metadata[4096];
		char md5_hash[MD5_DIGEST_LENGTH * 2 + 1];
		finalizeRollingMD5(&(client->md5_ctx), md5_hash);

		ret = rados_write(client->data_io_ctx, client->object_name, client->put_buf, client->object_size, 0);

		snprintf(metadata, 4096, "%s;%s;%ld", datetime_str, md5_hash, client->object_size);
		const size_t key_lens = strlen(client->object_name);
		const size_t val_lens = strlen(metadata);
		const char *keys = client->object_name;
		const char *vals = metadata;

		/* register object to bucket */
		rados_write_op_t write_op = rados_create_write_op();
		rados_write_op_omap_set2(write_op, &keys, &vals, &key_lens, &val_lens, 1);
		ret = rados_write_op_operate2(write_op, *(client->bucket_io_ctx), client->bucket_name, NULL, 0);
		assert(ret == 0);
		rados_release_write_op(write_op);

		//*response_size = snprintf(NULL, 0, "%s\r\nEtag: %s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, md5_hash, AMZ_REQUEST_ID, datetime_str) + 1;
		*response_size = snprintf(NULL, 0, "%s\r\nEtag: %s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, "xxx", AMZ_REQUEST_ID, datetime_str) + 1;
		*response = malloc(*response_size);
		//snprintf(*response, *response_size, "%s\r\nEtag: %s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, md5_hash, AMZ_REQUEST_ID, datetime_str);
		snprintf(*response, *response_size, "%s\r\nEtag: %s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, "xxx", AMZ_REQUEST_ID, datetime_str);

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
		int ret = 1; char buf;
		ret = rados_read(*(client->bucket_io_ctx), client->bucket_name, &buf, 0, 0);
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
			rados_read_op_operate(read_op, *(client->bucket_io_ctx), client->bucket_name, 0);

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

//		const char *last_modified_datetime_str = "Thu, 25 Jan 2024 15:33:44 GMT";
//		const char *etag = "1463144381b8de6c51b5653a7567e233";
//		*data_payload_size = 1 * 1024 * 1024 * sizeof(char);
//		*data_payload = (char*)malloc(*data_payload_size);
//		ret = rados_read(*(client->data_io_ctx), client->object_name, *data_payload, *data_payload_size, 0);
//		*data_payload_size = ret;
//		client->object_size = *data_payload_size;
		//printf("rados read object %s for %ld\n", client->object_name, *data_payload_size);

//		*response_size = snprintf(NULL, 0, "%s\r\nx-amz-request-id: %s\r\nContent-Length: %ld\r\nEtag: %s\r\nLast-Modified: %s\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, client->object_size, etag, last_modified_datetime_str, datetime_str) + 1;
//		*response = malloc(*response_size);
//		snprintf(*response, *response_size, "%s\r\nx-amz-request-id: %s\r\nContent-Length: %ld\r\nEtag: %s\r\nLast-Modified: %s\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, client->object_size, etag, last_modified_datetime_str, datetime_str);

//		rados_read_op_t read_op = rados_create_read_op();
//		rados_read_op_omap_get_vals2(read_op, NULL, client->object_name, 1, &iter, NULL, &omap_prval);
//		rados_read_op_operate(read_op, *(client->bucket_io_ctx), client->bucket_name, 0);
//		rados_omap_get_next2(iter, &object_name, &metadata, &object_name_len, &metadata_len);

		client->object_size = 1048576;
		*data_payload_size = client->object_size;
		*data_payload = malloc(client->object_size);
		ret = rados_read(*(client->data_io_ctx), client->object_name, *data_payload, client->object_size, 0);

		if (ret != client->object_size) {
			printf("object %s not found\n", client->object_name);
			*response_size = snprintf(NULL, 0, "%s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_NOT_FOUND_HDR, AMZ_REQUEST_ID, datetime_str) + 1;
			*response = malloc(*response_size);
			snprintf(*response, *response_size, "%s\r\nx-amz-request-id: %s\r\nContent-Length: 0\r\nDate: %s\r\n\r\n", HTTP_NOT_FOUND_HDR, AMZ_REQUEST_ID, datetime_str);

			*data_payload = NULL;
			*data_payload_size = 0;
		}
		else {
			////metadata[metadata_len] = 0;
			//char last_modified_datetime_str[4096];
			//char *data = strdup(metadata);

			//char *token = strtok(data, ";");
			//strncpy(last_modified_datetime_str, token, 4095);
			////convertToISODateTime(token, last_modified_datetime_str);
			//token = strtok(NULL, ";");
			//char *etag = strdup(token);
			//token = strtok(NULL, ";");
			//client->object_size = atol(token);
			//printf("got metadata: %s %s %ld\n", last_modified_datetime_str, etag, client->object_size);

			//*data_payload_size = client->object_size;
			//*data_payload = malloc(client->object_size);
			//ret = rados_read(*(client->data_io_ctx), client->object_name, *data_payload, client->object_size, 0);

			char *etag = "xxx";
			char *last_modified_datetime_str = "yyy";
			*response_size = snprintf(NULL, 0, "%s\r\nx-amz-request-id: %s\r\nContent-Length: %ld\r\nEtag: %s\r\nLast-Modified: %s\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, client->object_size, etag, last_modified_datetime_str, datetime_str) + 1;
			*response = malloc(*response_size);
			snprintf(*response, *response_size, "%s\r\nx-amz-request-id: %s\r\nContent-Length: %ld\r\nEtag: %s\r\nLast-Modified: %s\r\nDate: %s\r\n\r\n", HTTP_OK_HDR, AMZ_REQUEST_ID, client->object_size, etag, last_modified_datetime_str, datetime_str);

			//free(etag);
			//free(data);
		}

		//rados_release_read_op(read_op);
	}
}
