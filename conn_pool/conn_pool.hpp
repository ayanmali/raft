#include <array>
#include <string_view>
#include <unordered_map>
#include "../config.hpp"

/*
stores a fixed capacity of mappings between ip addresses (std::string_view) and socket FDs (int).

Connections are evicted lazily. If a message is being sent to an IP address
that doesn't exist in the map and the map is full, then the least recently/frequently
used mapping is evicted to make room for the new one.

LRU:
adding an element - when cache is full, remove the element at the tail of the list
accesses - any access moves the item to the head of the list
*/
template <typename K, typename V>
struct TemplateLRUNode {
    K key;
    V value;
    TemplateLRUNode* prev;
    TemplateLRUNode* next;
};

using LRUNode = TemplateLRUNode<std::string_view, int>;

struct ConnectionPool {
    // keys are stored in the map as well as in the node struct
    char* buffer;
    std::unordered_map<std::string_view, LRUNode*> map;
    LRUNode* head;
    int size; // total # of connections currently maintained
    int capacity; // total # of peers that can be connected to

    ConnectionPool(int cap);
    ~ConnectionPool();
    int get(std::string_view ip_addr);
    void set(std::string_view ip_addr, int sock_fd);
    void remove(std::string_view ip_addr);
};

inline ConnectionPool::ConnectionPool(int cap) : size{0}, capacity{cap} {
    map.reserve(MAX_POOL_SIZE);

    // placement new to reduce the # of syscalls
    buffer = new char[sizeof(LRUNode) * MAX_POOL_SIZE];
    head = new(buffer) LRUNode;
    head->key = nullptr;
    head->prev = nullptr;
    auto curr = head;
    for (int i = 1; i < MAX_POOL_SIZE; ++i) {
        auto prev = curr->prev;
        curr->next = new(buffer + (sizeof(LRUNode) * i)) LRUNode;
        curr->next->key = nullptr;
        auto old = curr;
        curr = curr->next;
        curr->prev = old;
        prev = old;
    }
};

inline ConnectionPool::~ConnectionPool() {
    int offset = 0;
    // deallocate each LRUNode
    auto ptr = head;
    while (ptr) {
        auto idk = ptr;
        auto next = ptr->next;
        ptr = next;
        idk->~LRUNode();
    }
    // deallocate the buffer
    delete[] buffer;
    buffer = nullptr; 
}

inline int ConnectionPool::get(std::string_view ip_addr) {
    auto it = map.find(ip_addr);
    if (it == map.end()) {
        return -1;
    }
    // move this node to the head of the list
    auto curr = it->second;
    auto prev = curr->prev;
    auto next = curr->next;
    prev->next = next;
    next->prev = prev;
    curr->prev = nullptr;
    curr->next = head;
    head->prev = curr;
    head = curr;
    return curr->value;
};

inline void ConnectionPool::set(std::string_view ip_addr, int sock_fd) {
    // if key exists already
    auto it = map.find(ip_addr);
    if (it != map.end()) {
        auto curr = it->second;
        curr->value = sock_fd;

        // move the node to the head of the list
        auto prev = curr->prev;
        auto next = curr->next;
        prev->next = next;
        next->prev = prev;
        curr->prev = nullptr;
        curr->next = head;
        head->prev = curr;
        head = curr;
        return;
    }
    // adding a new node
    
    // change the tail node's data and move it to the head
    if (size == capacity) {
        // get the tail node of the list
        auto curr = head;
        while (curr->next) {
            curr = curr->next;
        }
        // update the KV pair in the map
        map.erase(curr->key);
        map[ip_addr] = curr;

        curr->key = ip_addr;
        curr->value = sock_fd;
        
        // move it to the head of the list
        auto prev = curr->prev;
        auto next = curr->next;
        prev->next = next;
        next->prev = prev;
        curr->prev = nullptr;
        curr->next = head;
        head->prev = curr;
        head = curr;
        return;
    }

    // find the first vacant node in the buffer
    auto curr = head;
    while (curr->next) {
        if (curr->key == nullptr) break;
        curr = curr->next;
    }
    // set the nodes key, value
    curr->key = ip_addr;
    curr->value = sock_fd;

    // move this node to the head of the list
    auto prev = curr->prev;
    auto next = curr->next;
    prev->next = next;
    next->prev = prev;
    curr->prev = nullptr;
    curr->next = head;
    head->prev = curr;
    head = curr;

    // add the KV pair to `map`
    map[ip_addr] = curr;
    ++size;

};

inline void ConnectionPool::remove(std::string_view ip_addr) {
    auto it = map.find(ip_addr);
    if (it == map.end()) return;
    auto curr = it->second;
    curr->key = nullptr;
    curr->value = -1;
    map.erase(it->first);
    --size;
}
