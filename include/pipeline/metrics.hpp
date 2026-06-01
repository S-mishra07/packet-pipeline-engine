#pragma once

#include <atomic>
#include <cstdint>

namespace packet_pipeline {

// Minimal metrics collector
struct Metrics {
    std::atomic<uint64_t> packets_ingested{0};
    std::atomic<uint64_t> packets_parsed{0};
    std::atomic<uint64_t> packets_transformed{0};
    std::atomic<uint64_t> packets_aggregated{0};
    
    std::atomic<uint64_t> total_latency_ns{0};  // sum of all latencies for averaging
    std::atomic<uint64_t> max_latency_ns{0};
    std::atomic<uint64_t> min_latency_ns{UINT64_MAX};
};

}  // namespace packet_pipeline
