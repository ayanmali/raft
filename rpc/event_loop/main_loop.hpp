#pragma once
#include "./event_loop.hpp"

template <SocketType T>
inline std::optional<std::string> EventLoop<T>::Run() {
    #ifdef DEBUG
    std::cout << "starting event loop with id " << this_id << "\n";
    #endif
    epoll_event evs[EPOLL_BATCH];
    while (!stopped.load(std::memory_order_acquire)) {
        #ifdef DEBUG
        std::cout << "---\nwaiting for events...\n";
        #endif
        int n = ::epoll_wait(epoll_fd, evs, EPOLL_BATCH, -1);

        if (n < 0) {
            if (errno == EINTR) continue;
            return ("epoll_wait failed");
        }

        // loop over all ready FDs
        #ifdef DEBUG
        std::cout << "found " << n << " ready fds\n";
        #endif

        for (int i = 0; i < n; ++i) {
            const uint64_t ctx = evs[i].data.u64;
            const EpollContextKind kind = static_cast<EpollContextKind>(ctx >> 56);

            const uint32_t e = evs[i].events;
            #ifdef DEBUG
            std::cout << i << "th fd:\n";
            #endif

            switch (kind) {
                case EpollContextKind::Listen: {
                    if constexpr (T == TCP) {
                        #ifdef DEBUG
                        std::cout << "accepting new client connection\n";
                        #endif
                        std::optional<const char*> accept_err = Accept();
                        if (accept_err) {
                            return std::format(
                                "failed to accept new client connection; skipping:\n{}\n",
                                accept_err.value());
                        }
                        break;
                    }
                    if constexpr (T == UDP) {
                        if (e & EPOLLIN) {
                            std::byte bufs[UDP_RECV_BATCH][REQ_SIZE + sizeof(REQ_SIZE) + sizeof(RpcKind)];
                            struct sockaddr_in addrs[UDP_RECV_BATCH];
                            struct iovec iovecs[UDP_RECV_BATCH];
                            struct mmsghdr msgs[UDP_RECV_BATCH]{};

                            for (int k = 0; k < UDP_RECV_BATCH; ++k) {
                                iovecs[k].iov_base = bufs[k];
                                iovecs[k].iov_len  = sizeof(bufs[k]);
                                msgs[k].msg_hdr.msg_name    = &addrs[k];
                                msgs[k].msg_hdr.msg_namelen = sizeof(addrs[k]);
                                msgs[k].msg_hdr.msg_iov     = &iovecs[k];
                                msgs[k].msg_hdr.msg_iovlen  = 1;
                            }

                            for (;;) {
                                for (unsigned int k = 0; k < UDP_RECV_BATCH; ++k) {
                                    msgs[k].msg_hdr.msg_namelen = sizeof(addrs[k]);
                                }

                                int nmsgs = ::recvmmsg(listen_fd, msgs, UDP_RECV_BATCH, 0, nullptr);

                                if (nmsgs < 0) {
                                    if (errno == EINTR) continue;
                                    if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                                    return "unexpected error when trying to read client message (recvmmsg)\n";
                                }

                                for (int k = 0; k < nmsgs; ++k) {
                                    #ifdef DEBUG
                                    std::cout << "found client message (UDP)\n";
                                    #endif
                                    std::byte* buf = bufs[k];
                                    struct sockaddr_in& raw_addr = addrs[k];

                                    uint64_t key = encode(raw_addr.sin_addr.s_addr, raw_addr.sin_port);

                                    if (!client_data.client_ip_to_conn.contains(key)) {
                                        for (int attempt = 0; attempt < MAX_ATTEMPTS; ++attempt) {
                                            ClientConn<UDP>* c = client_slab.Acquire();
                                            if (!c) continue;
                                            c->client_ip_addr = key;
                                            client_data.client_ip_to_conn[key] = c;
                                            break;
                                        }
                                    }

                                    uint32_t net_len;
                                    std::memcpy(&net_len, buf, sizeof(net_len));
                                    uint32_t msg_len = ntohl(net_len);
                                    if (msg_len > sizeof(bufs[k]) - sizeof(uint32_t)) {
                                        return "failed to read client message: client sent oversized request frame\n";
                                    }

                                    auto request_raw = parse_datagram(buf, msgs[k].msg_len, key);
                                    if (std::holds_alternative<const char*>(request_raw)) {
                                        continue;
                                    }

                                    post_node_inbox(std::move(std::get<NodeMessage>(request_raw)));
                                }
                            }

                            break;
                        }

                        // ready to send reply to client
                        if (e & EPOLLOUT) {
                            bool drained = true;
                            for (auto& [client_ip_addr, c] : client_data.client_ip_to_conn) {
                                if (c->wbuf_size > 0) {
                                    auto [client_ip, client_port] = decode(c->client_ip_addr);
                                    struct sockaddr_in raw_addr;
                                    raw_addr.sin_addr.s_addr = client_ip;
                                    raw_addr.sin_port = client_port;
                                    socklen_t addrlen = sizeof(raw_addr);
                                    ssize_t sent = ::sendto(listen_fd,
                                        c->wbuf, c->wbuf_size,
                                        0,
                                        (struct sockaddr*)&raw_addr, addrlen);
                                    if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                                        drained = false; // keep the datagram queued and EPOLLOUT armed
                                        break;
                                    }
                                    c->wbuf_size = 0; // sent, or dropped on error (UDP is best-effort)
                                }
                            }
                            if (drained) {
                                // all replies flushed; disarm EPOLLOUT so the next
                                // post_reply re-arm generates a fresh edge
                                std::optional<const char*> modify_err =
                                    modify_listener_interest(listen_epoll_events & ~EPOLLOUT);
                                if (modify_err) {
                                    #ifdef DEBUG
                                    std::cout << modify_err.value() << "\n";
                                    #endif
                                }
                            }
                        }
                        break;
                    }
                }

                case EpollContextKind::Wake: {
                    #ifdef DEBUG
                    std::cout << "event fd awakened\n";
                    #endif
                    std::optional<std::string> on_event_fd_err = OnEventFd();
                    if (on_event_fd_err) {
                        return (std::format(
                            "Failed to process new event:\n{}\n",
                            on_event_fd_err.value()
                        ));
                    }
                    break;
                }

                case EpollContextKind::Peer: {
                    PeerConn& p = peer_id_to_conn[ctx & 0xFFFFFFFF];
                    if (e & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                        #ifdef DEBUG
                        std::cout << "event loop received epoll error ";
                        if (e & EPOLLERR) {
                            std::cout << "EPOLLERR";
                        }
                        else if (e & EPOLLHUP) {
                            std::cout << "EPOLLHUP";
                        }
                        else if (e & EPOLLRDHUP) {
                            std::cout << "EPOLLRDHUP";
                        }
                        std::cout << " for peer " << p.peer_id << "; disconnecting peer\n";
                        #endif
                        DropPeer(p);
                        break;
                    }
                    if (e & EPOLLIN) {
                        #ifdef DEBUG
                        std::cout << "obtained reply from peer " << p.peer_id << "\n";
                        #endif
                        std::optional<const char*> readable_err = OnPeerReadable(p);
                        if (readable_err) {
                            #ifdef DEBUG
                            std::cout << "failed to read incoming peer reply:\n" << readable_err.value() << "\n";
                            #endif
                            break;
                        }
                    }
                    if (e & EPOLLOUT) {
                        #ifdef DEBUG
                        std::cout << "ready to send RPC to peer " << p.peer_id << "\n";
                        #endif
                        std::optional<const char*> writable_err = OnPeerWritable(p);
                        if (writable_err) {
                            #ifdef DEBUG
                            std::cout << "failed to write RPC to peer socket:\n" << writable_err.value() << "\n";
                            #endif
                            break;
                        }
                    }
                    break;
                }

                case EpollContextKind::Client: {
                    if constexpr (T == TCP) {
                        const uint32_t client_fd = ctx & 0xFFFFFFFF;
                        if (!client_data.client_fd_to_ip.contains(client_fd)) break;
                        if (!client_data.client_ip_to_conn.contains(client_data.client_fd_to_ip[client_fd])) break;
                        ClientConn<TCP>* c = client_data.client_ip_to_conn[client_data.client_fd_to_ip[client_fd]];
                        if (e & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                            #ifdef DEBUG
                            std::cout << "epoll error found for client " << c->client_ip_addr << ":";
                            if (e & EPOLLERR) {
                                std::cout << "EPOLLERR\n";
                            }
                            else if (e & EPOLLHUP) {
                                std::cout << "EPOLLHUP\n";
                            }
                            else if (e & EPOLLRDHUP) {
                                std::cout << "EPOLLRDHUP\n";
                            }
                            #endif
                            CloseClient(c);
                            break;
                        }
                        if (e & EPOLLIN) {
                            #ifdef DEBUG
                            std::cout << "new client message from client with ip " << c->client_ip_addr << "\n";
                            #endif
                            std::optional<const char*> readable_err = OnClientReadable(c);
                            if (readable_err) {
                                #ifdef DEBUG
                                std::cout << "failed to read incoming client message from client with ip " << c->client_ip_addr << ":\n" << readable_err.value() << "\n";
                                #endif
                                break;
                            }
                        }
                        if (e & EPOLLOUT) {
                            #ifdef DEBUG
                            std::cout << "ready to send reply to client with ip " << c->client_ip_addr << "\n";
                            #endif
                            std::optional<const char*> writable_err = OnClientWritable(c);
                            if (writable_err) {
                                #ifdef DEBUG
                                std::cout << "failed to write to client socket:\n" << writable_err.value() << "\n";
                                #endif
                                break;
                            }
                        }
                    }
                    break;
                }

                case EpollContextKind::PeerTimer: {
                    PeerConn& p = peer_id_to_conn[ctx & 0xFFFFFFFF];
                    const TimerKind subtype = static_cast<TimerKind>((ctx >> 48) & 0xFF);

                    if (e & EPOLLIN) {
                        #ifdef DEBUG
                        const char* kind_name[] = {"heartbeat", "AE", "RV", "IS"};
                        std::cout << kind_name[static_cast<uint8_t>(kind)] << " timer fired for peer " << p.peer_id << "\n";
                        #endif
                        std::optional<const char*> timer_err;
                        switch (subtype) {
                            case TimerKind::Heartbeat: timer_err = OnPeerHeartbeatTimeout(p); break;
                            case TimerKind::AE:        timer_err = OnPeerAERPCTimeout(p);     break;
                            case TimerKind::RV:        timer_err = OnPeerRVRPCTimeout(p);     break;
                            case TimerKind::IS:        timer_err = OnPeerISRPCTimeout(p);     break;
                        }
                        if (timer_err) {
                            #ifdef DEBUG
                            std::cout << "failed to handle " << kind_name[static_cast<uint8_t>(kind)]
                                      << " timer for peer " << p.peer_id << ":\n" << timer_err.value() << "\n";
                            #endif
                            break;
                        }
                    }
                    break;
                }

                default:
                    break;
            }
        }
    }
    return {};
}
