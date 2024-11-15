// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

// static io_context_t ctx;

static const char HTTP_404_HEADER[] = "HTTP/1.0 404 Not Found\r\nContent-Length: 0\r\n\r\n";

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the reply header. */
	struct stat statbuf;

	fstat(conn->fd, &statbuf);

	conn->file_size = statbuf.st_size;
	sprintf(conn->send_buffer, "HTTP/1.0 200 OK\r\nContent-Length: %ld\r\n\r\n", conn->file_size);
	conn->send_len = strlen(conn->send_buffer);
	conn->state = STATE_HEADER_SENT;
}

static void connection_prepare_send_404(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the 404 header. */
	conn->file_size = 0;
	sprintf(conn->send_buffer, HTTP_404_HEADER);
	conn->send_len = strlen(HTTP_404_HEADER);
	conn->state = STATE_404_SENT;
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* TODO: Get resource type depending on request path/filename. Filename should
	 * point to the static or dynamic folder.
	 */
	if (strstr(conn->filename, AWS_REL_STATIC_FOLDER) != NULL)
		return RESOURCE_TYPE_STATIC;
	else
		return RESOURCE_TYPE_DYNAMIC;
}


struct connection *connection_create(int sockfd)
{
	/* TODO: Initialize connection structure on given socket. */
	struct connection *conn = malloc(sizeof(struct connection));

	DIE(conn == NULL, "malloc");

	memset(conn, 0, sizeof(struct connection));
	memset(conn->filename, 0, BUFSIZ);
	memset(conn->recv_buffer, 0, BUFSIZ);
	memset(conn->send_buffer, 0, BUFSIZ);
	memset(conn->request_path, 0, BUFSIZ);

	conn->sockfd = sockfd;

	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	/* TODO: Start asynchronous operation (read from file).
	 * Use io_submit(2) & friends for reading data asynchronously.
	 */
	conn->eventfd = eventfd(0, 0);
	conn->piocb[0] = &conn->iocb;
	io_set_eventfd(conn->piocb[0], conn->eventfd);
	io_setup(1, &conn->ctx);
}

void connection_remove(struct connection *conn)
{
	/* TODO: Remove connection handler. */
	if (conn->fd > 0)
		close(conn->fd);
	close(conn->sockfd);
}

void handle_new_connection(void)
{
	/* TODO: Handle a new connection request on the server socket. */
	int sockfd;
	socklen_t addrlen = sizeof(struct sockaddr_in);
	struct sockaddr_in addr;
	struct connection *conn;
	int rc;
	int flags;

	/* TODO: Accept new connection. */
	sockfd = accept(listenfd, (struct sockaddr *) &addr, &addrlen);
	DIE(sockfd < 0, "accept");

	/* TODO: Set socket to be non-blocking. */
	flags = fcntl(sockfd, F_GETFL);
	DIE(flags < 0, "fcntl");
	rc = fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);
	DIE(rc < 0, "fcntl");

	/* TODO: Instantiate new connection handler. */
	conn = connection_create(sockfd);

	/* TODO: Add socket to epoll. */
	rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);
	DIE(rc < 0, "w_epoll_add_in");

	/* TODO: Initialize HTTP_REQUEST parser. */
	http_parser_init(&conn->request_parser, HTTP_REQUEST);
}

void receive_data(struct connection *conn)
{
	/* TODO: Receive message on socket.
	 * Store message in recv_buffer in struct connection.
	 */
	ssize_t bytes = 0;
	size_t total_bytes_read = 0;

	do {
		bytes = recv(conn->sockfd, conn->recv_buffer + total_bytes_read, BUFSIZ - total_bytes_read, 0);

		if (bytes > 0)
			total_bytes_read += bytes;
		else if (bytes == -1)
			break;
	} while (bytes != 0);

	conn->recv_len = total_bytes_read;
	conn->state = STATE_REQUEST_RECEIVED;
}
int connection_open_file(struct connection *conn)
{
	/* TODO: Open file and update connection fields. */

	return -1;
}

void connection_complete_async_io(struct connection *conn)
{
	/* TODO: Complete asynchronous operation; operation returns successfully.
	 * Prepare socket for sending.
	 */
	io_destroy(conn->ctx);
}

int parse_header(struct connection *conn)
{
	/* TODO: Parse the HTTP header and extract the file path. */
	/* Use mostly null settings except for on_path callback. */
	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0
	};

	memset(conn->request_path, 0, BUFSIZ);
	http_parser_execute(&conn->request_parser, &settings_on_path, conn->recv_buffer, conn->recv_len);

	sprintf(conn->filename, ".");
	strcat(conn->filename, conn->request_path);
	conn->fd = open(conn->filename, O_RDWR);

	return 0;
}

enum connection_state connection_send_static(struct connection *conn)
{
	/* TODO: Send static data using sendfile(2). */
	size_t total_bytes_sent = 0;
	ssize_t bytes = 0;

	while (total_bytes_sent < conn->file_size) {
		off_t file_offset = total_bytes_sent;

		bytes = sendfile(conn->sockfd, conn->fd, &file_offset, conn->file_size - total_bytes_sent);

		if (bytes > 0)
			total_bytes_sent += bytes;
	}

	return STATE_DATA_SENT;
}

int connection_send_data(struct connection *conn)
{
	/* May be used as a helper function. */
	/* TODO: Send as much data as possible from the connection send buffer.
	 * Returns the number of bytes sent or -1 if an error occurred
	 */
	ssize_t bytes = 0;
	size_t total_bytes_sent = 0;

	do {
		bytes = send(conn->sockfd, conn->send_buffer + total_bytes_sent, conn->send_len - total_bytes_sent, 0);

		if (bytes > 0)
			total_bytes_sent += bytes;
		else
			break;
	} while (bytes != 0);


	return total_bytes_sent;
}


int connection_send_dynamic(struct connection *conn)
{
	/* TODO: Read data asynchronously.
	 * Returns 0 on success and -1 on error.
	 */
	connection_start_async_io(conn);

	size_t i;
	size_t nr_iters = conn->file_size / BUFSIZ;
	struct io_event event;

	if (conn->file_size % BUFSIZ)
		nr_iters++;

	for (i = 0; i < nr_iters; i++) {
		size_t bytes_to_read = 0;

		if (conn->file_size < BUFSIZ)
			bytes_to_read = conn->file_size;
		else
			bytes_to_read = BUFSIZ;


		memset(conn->send_buffer, 0, bytes_to_read);
		io_prep_pread(conn->piocb[0], conn->fd, conn->send_buffer, bytes_to_read, i * BUFSIZ);
		io_submit(conn->ctx, 1, conn->piocb);

		io_getevents(conn->ctx, 1, 1, &event, NULL);

		io_prep_pwrite(conn->piocb[0], conn->sockfd, conn->send_buffer, bytes_to_read, 0);
		io_submit(conn->ctx, 1, conn->piocb);

		io_getevents(conn->ctx, 1, 1, &event, NULL);

		conn->file_size -= bytes_to_read;
	}

	connection_complete_async_io(conn);

	return 0;
}


void handle_input(struct connection *conn)
{
	/* Handle input information: may be a new message or notification of
	 * completion of an asynchronous I/O operation.
	 */

	switch (conn->state) {
	default:
		printf("shouldn't get here %d\n", conn->state);
	}
}

void handle_output(struct connection *conn)
{
	/* Handle output information: may be a new valid requests or notification of
	 * completion of an asynchronous I/O operation or invalid requests.
	 */

	switch (conn->state) {
	default:
		ERR("Unexpected state\n");
		exit(1);
	}
}

void handle_client(uint32_t event, struct connection *conn)
{
	/* Handle new client. There can be input and output connections */
	int rc;

	if (event & EPOLLIN) {
		receive_data(conn);

		conn->request_parser.data = conn;
		parse_header(conn);

		memset(conn->send_buffer, 0, BUFSIZ);

		if (conn->fd != -1)
			connection_prepare_send_reply_header(conn);
		else
			connection_prepare_send_404(conn);

		rc = w_epoll_update_ptr_out(epollfd, conn->sockfd, conn);
		DIE(rc < 0, "w_epoll_update_ptr_inout");

	} else if (event & EPOLLOUT) {
		connection_send_data(conn);

		if (conn->state != STATE_404_SENT) {
			if (connection_get_resource_type(conn) == RESOURCE_TYPE_STATIC)
				connection_send_static(conn);
			else
				connection_send_dynamic(conn);
		}

		rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
		DIE(rc < 0, "w_epoll_remove_ptr");

		connection_remove(conn);
	}
}

int main(void)
{
	int rc;

	/* Initialize multiplexing */
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "epoll_create");

	/* Create server socket */
	listenfd = tcp_create_listener(AWS_LISTEN_PORT, DEFAULT_LISTEN_BACKLOG);
	DIE(listenfd < 0, "listen");

	/* Add server socket to epoll object*/
	rc = w_epoll_add_fd_in(epollfd, listenfd);
	DIE(rc < 0, "epoll_ctl");

	/* server main loop */
	while (1) {
		struct epoll_event rev;

		/* Wait for events */
		rc = w_epoll_wait_infinite(epollfd, &rev);
		DIE(rc < 0, "w_epoll_wait_infinite");

		if (rev.data.fd == listenfd) {
			if (rev.events & EPOLLIN)
				handle_new_connection();
		} else {
			handle_client(rev.events, (struct connection *) rev.data.ptr);
		}
	}

	return 0;
}
