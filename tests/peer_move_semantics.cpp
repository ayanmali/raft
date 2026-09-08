#include <cassert>
#include <iostream>
#include <vector>

void simulate_close(int fd){
    std::cout << "simulate closing file descriptor " << fd << "\n";
}

struct TimerFDs {
    int fds[3] = {-1, -1, -1};
    TimerFDs() = default;
};
struct PeerConn {
    TimerFDs timer_fds;

    PeerConn(int one, int two, int three) {
        timer_fds.fds[0] = one;
        timer_fds.fds[1] = two;
        timer_fds.fds[2] = three;
    }
    PeerConn() = default;
    PeerConn(const PeerConn&) = delete;
    PeerConn& operator=(const PeerConn&) = delete;
    PeerConn(PeerConn&& other) {
        this->timer_fds.fds[0] = other.timer_fds.fds[0];
        this->timer_fds.fds[1] = other.timer_fds.fds[1];
        this->timer_fds.fds[2] = other.timer_fds.fds[2];

        simulate_close(other.timer_fds.fds[0]);
        simulate_close(other.timer_fds.fds[1]);
        simulate_close(other.timer_fds.fds[2]);
    }

    PeerConn& operator=(PeerConn&& other) {
        this->timer_fds.fds[0] = other.timer_fds.fds[0];
        this->timer_fds.fds[1] = other.timer_fds.fds[1];
        this->timer_fds.fds[2] = other.timer_fds.fds[2];

        simulate_close(other.timer_fds.fds[0]);
        simulate_close(other.timer_fds.fds[1]);
        simulate_close(other.timer_fds.fds[2]);
        other.timer_fds.fds[0] = -1;
        other.timer_fds.fds[1] = -1;
        other.timer_fds.fds[2] = -1;

        return *this;
    }

    ~PeerConn() {
        simulate_close(timer_fds.fds[0]);
        simulate_close(timer_fds.fds[1]);
        simulate_close(timer_fds.fds[2]);
    }
};
int main() {
    std::vector<PeerConn> peer_id_to_conn{};
    peer_id_to_conn.resize(2);

    peer_id_to_conn.emplace(peer_id_to_conn.begin(), 9,6,4);
    peer_id_to_conn.emplace(peer_id_to_conn.begin() + 1, 3,7,2);
    peer_id_to_conn.resize(3);
    // triggers reallocation
    peer_id_to_conn.emplace(peer_id_to_conn.begin() + 2, 8,1,5);

    assert(peer_id_to_conn[0].timer_fds.fds[0] == 9);
    assert(peer_id_to_conn[0].timer_fds.fds[1] == 6);
    assert(peer_id_to_conn[0].timer_fds.fds[2] == 4);

    assert(peer_id_to_conn[1].timer_fds.fds[0] == 3);
    assert(peer_id_to_conn[1].timer_fds.fds[1] == 7);
    assert(peer_id_to_conn[1].timer_fds.fds[2] == 2);

    assert(peer_id_to_conn[2].timer_fds.fds[0] == 8);
    assert(peer_id_to_conn[2].timer_fds.fds[1] == 1);
    assert(peer_id_to_conn[2].timer_fds.fds[2] == 5);

    std::cout << "Test passed\n";
}
