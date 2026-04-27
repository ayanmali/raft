#include "../node.hpp"
#include <cerrno>
#include <netdb.h>
#include <stdexcept>
#include <unistd.h>
#include <string_view>
#include <sys/types.h>
#include <sys/socket.h>

void sigchld_handler(int s) {
    (void)s;

    // waitpid() might overwrite errno, so we save and restore it:
    int saved_errno = errno;

    while(waitpid(-1, NULL, WNOHANG) > 0);

    errno = saved_errno;
}

void Node::server_expose(std::string_view port) { 
    int new_fd; // the FD that handles the communication between this server and the peer
    struct addrinfo hints, *p;
    struct sockaddr_in dest_addr;
    struct sigaction sa;
    socklen_t sin_size;

    std::memset(&hints, '\0', sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    if (getaddrinfo(NULL, port, &hints, &res_server) != 0) {
        throw std::runtime_error("Error calling getaddrinfo() on server side");
    }

    for (p = res_server; p != NULL; p = p->ai_next) {
        if (setsockopt(int, int, int, const void *, socklen_t) != 0) {
            continue;
        }
        if (bind(server_fd, res_server->ai_addr, res_server->ai_addrlen) != 0) {
            continue;
        }
        break;
    }

    if (p == NULL) {
        throw std::runtime_error("struct addrinfo* `p` is NULL");
    }

    if (listen(server_fd, SERVER_BACKLOG) != 0) {
        throw std::runtime_error("Error listening on server socket");
    }

    sa.sa_handler = sigchld_handler; // reap all dead processes
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    if (sigaction(SIGCHLD, &sa, NULL) == -1) {
        perror("sigaction");
        exit(1);
    }

    // accept new connections and spawn new FDs
    while (true) {
        sin_size = sizeof(dest_addr);
        new_fd = accept(server_fd, (struct sockaddr*)&dest_addr, &sin_size);
        if (new_fd != 0) {
            throw std::runtime_error("Error accepting on server socket");
        }

        if (!fork()) { // this is the child process
            close(sockfd); // child doesn't need the listener
            if (send(new_fd, "Hello world!", 13, 0) == -1)
                throw std::runtime_error("Error sending on server socket");
            close(new_fd);
            exit(0);
        }
        close(new_fd);  // parent doesn't need this

    }
}

void Node::server_reply(const char* msg, int len) {

}