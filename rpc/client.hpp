/*
retry logic - if an RPC fails, assume the peer's entry in the connection cache is stale.
Re-establish the connection to their IP address, then store that mapping in the map again, then try RPC again.

*/
#include "../node.hpp"
#include <stdexcept>
#include <string_view>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <netdb.h>

/*
When catching an exception from this function, the client fd must be closed and the fd must be recreated.
*/
void Node::client_connect(std::string_view ip_addr, std::string_view port) {
    struct addrinfo hints, *p;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET; // IPv4/v6
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_NUMERICHOST;

    if (getaddrinfo(ip_addr, port, &hints, &res_client) != 0) {
        throw std::runtime_error("Error calling getaddrinfo()");
    }

    for (p = res_client; p != NULL; p = p->ai_next) {
        if (connect(client_fd, p->ai_addr, p->ai_addrlen) == 0) {
            break; // success
        }
        throw std::runtime_error("connect() call to server failed");
    }

    if (p == NULL) {
        throw std::runtime_error("struct addrinfo* `p` is NULL");
    }

    return 0;
}

void Node::client_send(const char* msg, int len, int flags) {
    // auto bytes_sent = send(client_fd, msg, len, flags);
    // if (bytes_sent < 0) {
    //     throw std::runtime_error("Error sending on client socket");
    // }
    // int next_sent;
    // while (bytes_sent < len) {
    //     next_sent = send(client_fd, msg[bytes_sent], len - bytes_sent, flags);
    //     if (next_sent < 0) {
    //         throw std::runtime_error("Error in retry send on client socket");
    //     }
    //     bytes_sent += next_sent;
    // }
    // return bytes_sent;

    int total = 0;
    int bytes_sent = 0;
    while (total < len) {
        bytes_sent = send(client_fd, msg[bytes_sent], len - bytes_sent, flags);
        if (bytes_sent < 0) {
            throw std::runtime_error("Error sending on client socket");
        }
        total += bytes_sent;
    }
}

void Node::client_recv(int len, int flags) {
    auto bytes_recvd = recv(client_fd, (void*)sock_buf, len, flags);
    if (bytes_recvd < 0) {
        throw std::runtime_error("Error receiving from client socket");
    }
}