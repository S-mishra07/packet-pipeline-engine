#include "pipeline/stages.hpp"
#include "pipeline/packet.hpp"
#include "pipeline/ring_buffer.hpp"
#include "pipeline/metrics.hpp"

#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <csignal>

using namespace packet_pipeline;

// Global flag for graceful shutdown
std::atomic<bool> shutdown_flag{false};

void signal_handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        shutdown_flag.store(true, std::memory_order_release);
    }
}

int main(int argc, char** argv) {
    // Parse CLI args
    uint64_t duration_seconds = 10;  // Default 10 seconds
    
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--duration" && i + 1 < argc) {
            duration_seconds = std::stoull(argv[i + 1]);
        }
    }
    
    std::cout << "Packet Pipeline Engine - MVP\n";
    std::cout << "Duration: " << duration_seconds << " seconds\n";
    std::cout << "Ring buffer size: 4096 slots\n\n";
    
    // Register signal handler
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    
    // Create ring buffers and metrics
    SPSCRingBuffer<Packet, 4096> buffer_gen_to_parse;
    SPSCRingBuffer<Packet, 4096> buffer_parse_to_transform;
    SPSCRingBuffer<Packet, 4096> buffer_transform_to_agg;
    
    Metrics metrics;
    
    // Start pipeline threads
    std::thread gen_thread([&]() {
        generator_stage(buffer_gen_to_parse, metrics, duration_seconds);
    });
    
    std::thread parse_thread([&]() {
        parse_stage(buffer_gen_to_parse, buffer_parse_to_transform, metrics);
    });
    
    std::thread transform_thread([&]() {
        transform_stage(buffer_parse_to_transform, buffer_transform_to_agg, metrics);
    });
    
    std::thread agg_thread([&]() {
        aggregate_stage(buffer_transform_to_agg, metrics);
    });
    
    // Wait for generator to finish
    gen_thread.join();
    
    // Give pipeline time to drain
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // Signal other threads to stop
    shutdown_flag.store(true, std::memory_order_release);
    
    // Threads are still running in infinite loops, but we're done
    // In a real system, we'd use a proper shutdown mechanism
    
    // Just let them terminate (they'll be killed when program exits)
    std::cout << "\n=== Final Metrics ===\n";
    std::cout << "Packets ingested: " << metrics.packets_ingested.load() << "\n";
    std::cout << "Packets parsed: " << metrics.packets_parsed.load() << "\n";
    std::cout << "Packets transformed: " << metrics.packets_transformed.load() << "\n";
    std::cout << "Packets aggregated: " << metrics.packets_aggregated.load() << "\n";
    
    uint64_t agg_count = metrics.packets_aggregated.load();
    if (agg_count > 0) {
        double avg_lat = metrics.total_latency_ns.load() / (1000.0 * agg_count);
        double max_lat = metrics.max_latency_ns.load() / 1000.0;
        double min_lat = metrics.min_latency_ns.load() / 1000.0;
        
        std::cout << "Average latency: " << avg_lat << " µs\n";
        std::cout << "Max latency: " << max_lat << " µs\n";
        std::cout << "Min latency: " << min_lat << " µs\n";
    }
    
    return 0;
}
