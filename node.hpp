/*
On disk:
- currentTerm
- votedFor
- log[]

volatile (all servers):
- commitIndex
- lastApplied
volatile (leaders) (reinitialized after election):
- nextIndex[]
- matchIndex[]
*/
#include <vector>
struct Node {
    Node();
    std::vector<int> next_index; // one for each server
    std::vector<int> match_index; // one for each server
    int commit_index;
    int last_applied;
    int timeout; // randomly chosen from 150-300 ms

    void increment_current_term();
    void store_voted_for();
    void append_to_log();

    void send_heartbeats(); // send AE RPCs w/ no log entries
    void start_election(); // enter candidate mode
    void compact_log();
};

inline Node::Node() : timeout(150) {

};
inline void Node::start_election() {
    increment_current_term();
};