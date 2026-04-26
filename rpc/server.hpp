#include <string_view>
#include <sys/socket.h>
#include "../node.hpp"

void Node::server_bind(int port) { 
    struct sockaddr_in sock_addr;
    sock_addr.sin_family = AF_INET;
    sock_addr.sin_port = htons(port);
    sock_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    std::memset(&(sock_addr.sin_zero), '\0', 8); // zero the rest of the struct
    
    int ok = bind(
        sock_fd, 
        (struct sockaddr*)&sock_addr,
        sizeof(sockaddr)
    );
    if (ok != 0) {
        return -1;
    }
    return 0;
}

void Node::server_listen() {

}

void Node::server_accept() {

}

void Node::server_reply() {
    
}