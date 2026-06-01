#include "pipeline/stages.hpp"
#include "pipeline/packet.hpp"
#include "pipeline/ring_buffer.hpp"
#include "pipeline/metrics.hpp"

#include <iostream>
#include <chrono>
#include <thread>
#include <cstring>

namespace packet_pipeline {

// High-resolution timer utility
static uint64_t get_timestamp_ns() {
    return std::chrono::steady_clock::now().time_since_epoch().count();
}

// Generator: produces synthetic packets
void generator_stage(SPSCRingBuffer<Packet, 4096>& output_buffer,
                     Metrics& metrics,
                     uint64_t duration_seconds) {
    uint64_t start_time = get_timestamp_ns();
    uint64_t end_time = start_time + duration_seconds * 1'000'000'000ULL;
    
    uint32_t packet_count = 0;
    const uint32_t TARGET_PPS = 100'000;  // 100k packets per second
    const uint32_t BATCH_SIZE = 1000;
    
    uint64_t next_batch_time = start_time;
    uint64_t batch_interval_ns = (BATCH_SIZE * 1'000'000'000ULL) / TARGET_PPS;
    
    while (get_timestamp_ns() < end_time) {
        uint64_t now = get_timestamp_ns();
        
        if (now >= next_batch_time) {
            for (uint32_t i = 0; i < BATCH_SIZE; ++i) {
                Packet pkt;
                
                // Synthetic packet: random length between 64 and 256 bytes
                pkt.length = 64 + (packet_count % 192);
                
                // Fill payload with pattern (for parse stage to process)
                for (uint16_t j = 0; j < pkt.length; ++j) {
                    pkt.payload[j] = (packet_count + j) & 0xFF;
                }
                
                pkt.ingress_ts_ns = now;
                
                if (!output_buffer.try_push(pkt)) {
                    // Buffer full, drop packet
                    break;
                }
                
                packet_count++;
                metrics.packets_ingested.fetch_add(1, std::memory_order_relaxed);
            }
            
            next_batch_time += batch_interval_ns;
        }
        
        // Yield to prevent busy-spin
        std::this_thread::yield();
    }
}

// Parse: extracts header info (dummy parsing)
void parse_stage(SPSCRingBuffer<Packet, 4096>& input_buffer,
                 SPSCRingBuffer<Packet, 4096>& output_buffer,
                 Metrics& metrics) {
    Packet pkt;
    
    while (true) {
        if (input_buffer.try_pop(pkt)) {
            // Dummy parse: just validate packet length
            if (pkt.length > 0 && pkt.length <= Packet::MAX_PAYLOAD) {
                metrics.packets_parsed.fetch_add(1, std::memory_order_relaxed);
                
                // Pass to next stage
                if (!output_buffer.try_push(pkt)) {
                    // Drop if output buffer full
                }
            }
        }
        
        // Small sleep to avoid busy-spin
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
}

// Transform: applies filtering (drop if length < 64)
void transform_stage(SPSCRingBuffer<Packet, 4096>& input_buffer,
                     SPSCRingBuffer<Packet, 4096>& output_buffer,
                     Metrics& metrics) {
    Packet pkt;
    
    while (true) {
        if (input_buffer.try_pop(pkt)) {
            // Filter: only pass packets >= 64 bytes
            if (pkt.length >= 64) {
                metrics.packets_transformed.fetch_add(1, std::memory_order_relaxed);
                
                if (!output_buffer.try_push(pkt)) {
                    // Drop if output buffer full
                }
            }
        }
        
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
}

// Aggregate: collects stats and outputs to stdout
void aggregate_stage(SPSCRingBuffer<Packet, 4096>& input_buffer,
                     Metrics& metrics) {
    Packet pkt;
    uint64_t last_print = get_timestamp_ns();
    
    while (true) {
        if (input_buffer.try_pop(pkt)) {
            uint64_t now = get_timestamp_ns();
            uint64_t latency = now - pkt.ingress_ts_ns;
            
            metrics.packets_aggregated.fetch_add(1, std::memory_order_relaxed);
            metrics.total_latency_ns.fetch_add(latency, std::memory_order_relaxed);
            
            if (latency > metrics.max_latency_ns.load(std::memory_order_relaxed)) {
                metrics.max_latency_ns.store(latency, std::memory_order_relaxed);
            }
            
            uint64_t min_lat = metrics.min_latency_ns.load(std::memory_order_relaxed);
            if (latency < min_lat) {
                metrics.min_latency_ns.store(latency, std::memory_order_relaxed);
            }
        }
        
        // Print metrics every second
        uint64_t now = get_timestamp_ns();
        if (now - last_print >= 1'000'000'000ULL) {
            uint64_t agg = metrics.packets_aggregated.load(std::memory_order_relaxed);
            uint64_t parsed = metrics.packets_parsed.load(std::memory_order_relaxed);
            uint64_t trans = metrics.packets_transformed.load(std::memory_order_relaxed);
            
            double avg_latency_us = 0;
            if (agg > 0) {
                avg_latency_us = metrics.total_latency_ns.load(std::memory_order_relaxed) / 
                                 (1000.0 * agg);
            }
            
            std::cout << "Ingested: " << metrics.packets_ingested.load(std::memory_order_relaxed)
                      << " | Parsed: " << parsed
                      << " | Transform: " << trans
                      << " | Agg: " << agg
                      << " | Avg Lat: " << avg_latency_us << " µs\n";
            
            last_print = now;
        }
        
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

}  // namespace packet_pipeline
