#include <iostream>
#include "../config.hpp"
#include "../rpc/protocol/payloads.hpp"
#include "../rpc/protocol/utils.hpp"
#include "../rpc/protocol/peer.hpp"

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
    std::byte buf[133];
    BufByteWriter writer{buf};
    writer.serialize(p);

    std::cout << "Test passed\n";
    return 0;

}
