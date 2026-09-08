#include <format>
#include <iostream>
#include "../config.hpp"
#include "../rpc/protocol/payloads.hpp"
#include "../rpc/protocol/utils.hpp"
#include "../rpc/protocol/peer.hpp"
#include "../rpc/protocol/client.hpp"

int main() {
    AppendEntriesReqPayload p(
        3, 1, 1, 0, 0, 0, 0
    );
    std::byte entries[3][CMD_SIZE] = {
        {std::byte{0x00}, std::byte{0x12}, std::byte{0x34}, std::byte{0x56}},
        {std::byte{0x01}, std::byte{0x69}, std::byte{0x67}, std::byte{0x91}},
        {std::byte{0x02}, std::byte{0x11}, std::byte{0x22}, std::byte{0x33}}
    };
    for (int i = 0; i < 3; ++i) {
        std::memcpy(p.entries[i].data_, entries[i], CMD_SIZE);
        p.entries[i].term = 1;
    }
    std::byte buf[sizeof(p)];
    BufByteWriter writer{buf};
    writer.serialize(p);

    TimerFDs tfds{};

    ByteReader reader{std::span<std::byte>(buf + sizeof(uint32_t) + sizeof(uint8_t), REQ_SIZE)};

    std::variant<NodeMessage, const char*> res = parse_ae_req(reader, 0);
    // uint32_t msg_len;
    // std::memcpy(&msg_len, buf, sizeof(msg_len));
    // msg_len = ntohl(msg_len);
    // parse_rbuf(rbuf, msg_len, tfds);

    if (!std::holds_alternative<NodeMessage>(res)) {
        std::cout << std::format("Error: {}", std::get<const char*>(res)) << "\n";
        return -1;
    }

    NodeMessage msg = std::get<NodeMessage>(res);
    assert(std::holds_alternative<AppendEntriesReqPayload>(msg));
    AppendEntriesReqPayload payload = std::get<AppendEntriesReqPayload>(msg);
    assert(payload.entries_len == 3);
    assert(payload.term == 1);
    std::cout << "Test passed\n";
    return 0;

}
