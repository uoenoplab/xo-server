#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <time.h>
#include <assert.h>

#include <rados/librados.h>
#include <llhttp.h>

#define PORT 8080
#define MAX_EVENTS 1000
#define MAX_BUFFER_SIZE 65536

#define MAX_FIELDS 64
#define MAX_AIO_OP 65536

rados_t cluster;

const char *poolname = "testpool";

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

	char *header_fields[MAX_FIELDS];
	char *header_values[MAX_FIELDS];
	size_t num_fields;

	rados_ioctx_t io;
	rados_completion_t aio_completion[MAX_AIO_OP];
	size_t rados_chunk_count;
	size_t rados_offset;

	size_t content_length;
	size_t http_chunk_length;
	size_t http_chunk_offset;
};

void get_datetime_str(char *buf, size_t length)
{
	time_t now = time(0);
	struct tm tm = *gmtime(&now);
	strftime(buf, length, "%a, %d %b %Y %H:%M:%S %Z", &tm);
}

void free_http_client(struct http_client *client)
{
	for (size_t i = 0; i < client->num_fields; i++) {
		free(client->header_fields[i]);
		free(client->header_values[i]);
	}

	free(client->bucket_name);
	free(client->object_name);
	rados_ioctx_destroy(client->io);
	free(client);
}

int on_message_complete_cb(llhttp_t* parser)
{
	int ret = 0;
	char response[65536];
	char datetime_str[1024];

	struct http_client *client = parser->data;
	get_datetime_str(datetime_str, 1024);

	if (llhttp_get_method(parser) == HTTP_PUT) {
		for (size_t i = 0; i < client->rados_chunk_count; i++) {
			// in-memeory, not on disk yet
			rados_aio_wait_for_complete(client->aio_completion[i]);
			rados_aio_release(client->aio_completion[i]);
		}
		ret = rados_setxattr(client->io, client->full_object_name, "Last-Modified", datetime_str, strlen(datetime_str)+1);
//Etag: 
		sprintf(response, "HTTP/1.1 200 OK\r\n\
Accept-Ranges: bytes\r\n\
x-amz-request-id: tx000004943788b34314aec-0064faf661-2ad76-default\r\n\
Date: %s\r\n\
Content-Length: 0\r\n\
Connection: Keep-Alive\r\n\r\n", datetime_str);
		//printf("PUT Response:\n%s\n", response);
		send(client->fd, response, strlen(response), 0);
	}
	else if (llhttp_get_method(parser) == HTTP_GET) {
		// handle actions
		if (client->object_name != NULL && client->object_name[0] == '?') {
			char payload[32768];
			char header[32768];

			char *uri = strndup(&client->object_name[1], strlen(&client->object_name[1]));
			char *token = strtok(uri, "&");

			while (token != NULL) {
				if (strncmp(token, "location", 8) == 0) {
					snprintf(payload, 65536, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">default</LocationConstraint>");
					break;
				}
				else if (strncmp(token, "versioning", 9) == 0) {
					snprintf(payload, 65536, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></VersioningConfiguration>");
					break;
				}
				token = strtok(NULL, "&");
			}

			snprintf(response, 65536, "HTTP/1.1 200 OK\r\n\
x-amz-request-id: tx000004943788b34314aec-0064faf661-2ad76-default\r\n\
Content-Type: application/xml\r\n\
Date: %s\r\n\
Content-Length: %ld\r\n\
Connection: Keep-Alive\r\n\r\n%s", datetime_str, strlen(payload), payload);
			send(client->fd, response, strlen(response), 0);

			free(uri);
		}
		else {
			size_t content_length;
			ret = rados_stat(client->io, client->full_object_name, &content_length, NULL);
			if (ret < 0) {
				// object not found
				snprintf(response, 65536, "HTTP/1.1 200 OK\r\n\
x-amz-request-id: tx000004943788b34314aec-0064faf661-2ad76-default\r\n\
Content-Type: application/xml\r\n\
Date: %s\r\n\
Content-Length: 134\r\n\
Connection: Keep-Alive\r\n\r\n\
<?xml version=\"1.0\" encoding=\"UTF-8\"?><LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">default</LocationConstraint>", datetime_str);
				//printf("GET NOT FOUND Response:\n%s\n", response);
				send(client->fd, response, strlen(response), 0);
			}
			else {
				char last_modified_time[1024];
				ret = rados_getxattr(client->io, client->full_object_name, "Last-Modified", last_modified_time, 1024);
				snprintf(response, 65536, "HTTP/1.1 200 OK\r\n\
x-amz-request-id: tx000004943788b34314aec-0064faf661-2ad76-default\r\n\
Date: %s\r\n\
Last-Modified: %s\r\n\
Content-Length: %ld\r\n\
Connection: Keep-Alive\r\n\r\n", last_modified_time, datetime_str, content_length);
				//printf("GET Response:\n%s\n", response);
				send(client->fd, response, strlen(response), 0);
	
				char buf[MAX_BUFFER_SIZE];
				while (client->rados_offset < content_length) {
					ret = rados_read(client->io, client->full_object_name, buf, MAX_BUFFER_SIZE, client->rados_offset);
					assert(ret >= 0);
					send(client->fd, buf, ret, 0);
					client->rados_offset += ret;
				}
			}
		}
	}
	else if (llhttp_get_method(parser) == HTTP_HEAD) {
		sprintf(response, "HTTP/1.1 200 OK\r\n\
X-RGW-Object-Count: 0\r\n\
X-RGW-Bytes-Used: 0\r\n\
X-RGW-Quota-User-Size: -1\r\n\
X-RGW-Quota-User-Objects: -1\r\n\
X-RGW-Quota-Max-Buckets: 1000\r\n\
X-RGW-Quota-Bucket-Size: -1\r\n\
X-RGW-Quota-Bucket-Objects: -1\r\n\
x-amz-request-id: tx0000015f281b0f0776853-0065002e5d-2beb9-default\r\n\
Content-Length: 0\r\n\
Date: %s\r\n\
Connection: Keep-Alive\r\n\r\n", datetime_str);
		//printf("HEAD Response:\n%s\n", response);
		send(client->fd, response, strlen(response), 0);
	}

	client->num_fields = 0;
	client->rados_offset = 0;
	client->rados_chunk_count = 0;

	client->content_length = 0;
	client->http_chunk_length = 0;
	client->http_chunk_offset = 0;

	return 0;
}

int on_headers_complete_cb(llhttp_t *parser)
{
	int ret = 0;
	struct http_client *client = (struct http_client*)parser->data;

	client->method = llhttp_get_method(parser);
	if (client->method == HTTP_PUT || client->method == HTTP_GET) {
		ret = rados_ioctx_create(cluster, poolname, &client->io);
		assert(ret == 0);
	}

	if (client->expect == CONTINUE) {
		const char *response = "HTTP/1.1 100 CONTINUE\r\n\r\n";
		send(client->fd, response, strlen(response), 0);
	}

	return 0;
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
//	else if (strcmp(client->header_fields[client->num_fields], "X-Amz-Decoded-Content-Length") == 0) {
//		client->data_length = atol(client->header_values[client->num_fields]);
//		printf("set data length %ld\n", client->data_length);
//	}
	else if (strcmp(client->header_fields[client->num_fields], "Content-Length") == 0) {
		client->content_length = atol(client->header_values[client->num_fields]);
	}

	client->num_fields++;

	return 0;
}

int on_url_cb(llhttp_t *parser, const char *at, size_t length)
{
	struct http_client *client = (struct http_client*)parser->data;
	client->object_name = NULL;
	client->bucket_name = NULL;

	char *uri = strndup(at, length);
	if (strlen(uri) > 1) {
		const char *bucket_name_start = strchr(uri, '/') + 1;
		char *bucket_name_end = strchr(bucket_name_start, '/');

		client->bucket_name = strndup(bucket_name_start, bucket_name_end - bucket_name_start);
		if (bucket_name_end != NULL && strlen(bucket_name_end) > 1) {
			const char *object_name_start = bucket_name_end + 1;
			client->object_name = strdup(object_name_start);
			size_t full_object_name_len = strlen(client->object_name) + strlen(client->bucket_name) + 2;
			client->full_object_name = malloc(sizeof(char) * full_object_name_len);
			snprintf(client->full_object_name, full_object_name_len, "%s/%s", client->bucket_name, client->object_name);
		}
	}

	free(uri);
//	fprintf(stdout, "bucket name: %s ; object name: %s\n", client->bucket_name, client->object_name);

	return 0;
}

int async_write_object(struct http_client *client, char *buffer, size_t length)
{
	int ret;

	// create an aio completion
	ret = rados_aio_create_completion(NULL, NULL, NULL, &client->aio_completion[client->rados_chunk_count]);
	assert(ret >= 0);

	// write the buffer
	//printf("PUT: %s length=%ld, chunk_size=%ld client->offset=%ld chunk_count=%ld \n", client->full_object_name, length, length, client->rados_offset, client->rados_chunk_count);
	ret = rados_aio_write(client->io, client->full_object_name, client->aio_completion[client->rados_chunk_count++], buffer, length, client->rados_offset);
	client->rados_offset += length;

	return ret;
}

int on_body_cb(llhttp_t *parser, const char *at, size_t length)
{
	int ret = 0;
	struct http_client *client = (struct http_client*)parser->data;

	if (llhttp_get_method(parser) == HTTP_PUT) {
		size_t tcp_stream_length = length;
		size_t tcp_stream_offset = 0;

		char *data_payload = at;
		size_t data_payload_size = client->http_chunk_length - client->http_chunk_offset;

		// while this tcp pkt is not consumed.
		while (tcp_stream_offset < tcp_stream_length) {
			// check if this is a start of an http chunk
			if (client->http_chunk_offset >= client->http_chunk_length) {
				// check the http chunk size
				const char *chunk_size_str_start = data_payload;
				const char *chunk_size_str_end = strstr(data_payload, ";chunk-signature=");
				if (chunk_size_str_end != NULL) {
					char *chunk_size_str = strndup(data_payload, chunk_size_str_end - data_payload);
					client->http_chunk_length = strtol(chunk_size_str, NULL, 16);;
					client->http_chunk_offset = 0;

					data_payload = strstr(chunk_size_str_end, "\r\n");
					if (data_payload != NULL) {
						data_payload += 2;
						tcp_stream_offset += data_payload - chunk_size_str_start;
					}
				}
				else {
					client->http_chunk_length = client->content_length;
					client->http_chunk_offset = 0;
				}
			}

			if ((client->http_chunk_length - client->http_chunk_offset) > (tcp_stream_length - tcp_stream_offset)) {
				data_payload_size = tcp_stream_length - tcp_stream_offset;
			}
			else {
				data_payload_size = client->http_chunk_length - client->http_chunk_offset;
			}

			//printf("tcp_stream_length: %ld , tcp_stream_offset: %ld , http_chunk_length: %ld , http_chunk_offset: %ld, data_payload_size: %ld\n", tcp_stream_length, tcp_stream_offset, client->http_chunk_length, client->http_chunk_offset, data_payload_size);

			// start to parse chunk
			async_write_object(client, data_payload, data_payload_size);

			// process the chunk
			data_payload += data_payload_size;

			// check if payload is ending
			const char *data_payload_end = strstr(data_payload, "\r\n");
			if (data_payload_end == data_payload) {
				tcp_stream_offset += 2;
				data_payload += 2;
			}

			tcp_stream_offset += data_payload_size;
			client->http_chunk_offset += data_payload_size;
		}
	}

	return 0;
}

struct http_client *create_http_client(int fd)
{
	struct http_client *client = (struct http_client*)calloc(1, sizeof(struct http_client));

	llhttp_settings_init(&(client->settings));
	llhttp_init(&(client->parser), HTTP_BOTH, &(client->settings));

	client->settings.on_message_complete = on_message_complete_cb;
	client->settings.on_headers_complete = on_headers_complete_cb;
	client->settings.on_header_field = on_header_field_cb;
	client->settings.on_header_value = on_header_value_cb;
	client->settings.on_url = on_url_cb;
	client->settings.on_body = on_body_cb;

	client->fd = fd;
	client->num_fields = 0;
	client->parser.data = client;
	client->expect = NONE;
	client->rados_chunk_count = 0;
	client->rados_offset = 0;
	//client->data_length = -1;
	client->content_length = 0;
	client->http_chunk_length = 0;
	client->http_chunk_offset = 0;

	return client;
}

void handle_request(struct http_client *client)
{
	// recv
	int fd = client->fd;
	char buffer[MAX_BUFFER_SIZE];
	memset(buffer, 0, MAX_BUFFER_SIZE);

	ssize_t bytes_read = recv(fd, &buffer, 1, MSG_PEEK);
	if (bytes_read == 0) {
		close(fd);
		free_http_client(client);
		return;
	}

	while ((bytes_read = recv(fd, buffer, MAX_BUFFER_SIZE, 0)) > 0) {
		//printf("fd=%d bytes_read (%ld): %10s\n", fd, bytes_read, buffer);
		enum llhttp_errno err = llhttp_execute(&(client->parser), buffer, bytes_read);
		if (err != HPE_OK) {
			fprintf(stderr, "Parse error: %s %s\n", llhttp_errno_name(err), client->parser.reason);
		}
	}
}

void init_rados(const char *cluster_name, const char *user_name, int flags, int argc, const char **argv)
{
	/* Initialize the cluster handle with the "ceph" cluster name and the "client.admin" user */
	int err;
	err = rados_create2(&cluster, cluster_name, user_name, flags);

	if (err < 0) {
		fprintf(stderr, "%s: Couldn't create the cluster handle! %s\n", argv[0], strerror(-err));
		exit(EXIT_FAILURE);
	}

	/* Read a Ceph configuration file to configure the cluster handle. */
	err = rados_conf_read_file(cluster, "/etc/ceph/ceph.conf");
	if (err < 0) {
		fprintf(stderr, "%s: cannot read config file: %s\n", argv[0], strerror(-err));
		exit(EXIT_FAILURE);
	}

	/* Read command line arguments */
	err = rados_conf_parse_argv(cluster, argc, argv);
	if (err < 0) {
		fprintf(stderr, "%s: cannot parse command line arguments: %s\n", argv[0], strerror(-err));
		exit(EXIT_FAILURE);
	}

	/* Connect to the cluster */
	err = rados_connect(cluster);
	if (err < 0) {
		fprintf(stderr, "%s: cannot connect to cluster: %s\n", argv[0], strerror(-err));
		exit(EXIT_FAILURE);
	}
}

void server()
{
	int server_fd, client_fd;
	struct sockaddr_in server_addr, client_addr;
	socklen_t client_len = sizeof(client_addr);
	int epoll_fd, event_count;
	struct epoll_event events[MAX_EVENTS];

	// Create socket
	if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
		perror("Socket creation failed");
		exit(EXIT_FAILURE);
	}

	// Set socket options
	int opt = 1;
	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
		perror("Setsockopt failed");
		exit(EXIT_FAILURE);
	}

	// Bind
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = INADDR_ANY;
	server_addr.sin_port = htons(PORT);
	if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
		perror("Bind failed");
		exit(EXIT_FAILURE);
	}

	// Listen
	if (listen(server_fd, 5) < 0) {
		perror("Listen failed");
		exit(EXIT_FAILURE);
	}

	// Create epoll instance
	epoll_fd = epoll_create1(0);
	if (epoll_fd == -1) {
		perror("Epoll creation failed");
		exit(EXIT_FAILURE);
	}

	// Add server socket to epoll
	struct epoll_event event;
	event.events = EPOLLIN;
	event.data.fd = server_fd;
	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event) == -1) {
		perror("Epoll_ctl failed");
		exit(EXIT_FAILURE);
	}

	printf("Server started on port %d...\n", PORT);

	while (1) {
		event_count = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);

		for (int i = 0; i < event_count; i++) {
			if (events[i].data.fd == server_fd) {
				// Accept new connections
				client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
				if (client_fd == -1) {
					perror("Accept failed");
					exit(EXIT_FAILURE);
				}

				// Set client socket to non-blocking
				int flags = fcntl(client_fd, F_GETFL, 0);
				flags |= O_NONBLOCK;
				fcntl(client_fd, F_SETFL, flags);

				event.events = EPOLLIN | EPOLLET; // Edge-triggered

				// Create new parser
				event.data.ptr = create_http_client(client_fd);

				if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &event) == -1) {
					perror("Epoll_ctl failed");
					exit(EXIT_FAILURE);
				}
			} else {
				// Handle client data
				handle_request(events[i].data.ptr);
			}
		}
	}

	close(server_fd);
}

int main(int argc, char *argv[])
{
	init_rados("ceph", "client.admin", 0, argc, (const char**)argv);
	server();
	return 0;
}
