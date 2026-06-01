#pragma once

#include <cstdint>
#include <cstring>

namespace packet_pipeline {

// Minimal packet representation
struct Packet {
    static constexpr size_t MAX_PAYLOAD = 256;
    
    uint8_t payload[MAX_PAYLOAD];
    uint16_t length;
    uint64_t ingress_ts_ns;  // nanosecond timestamp at ingest
    
    Packet() : length(0), ingress_ts_ns(0) {
        std::memset(payload, 0, MAX_PAYLOAD);
    }
};

}  // namespace packet_pipeline
