#pragma once

#include "packet.hpp"
#include "ring_buffer.hpp"
#include "metrics.hpp"

namespace packet_pipeline {

// Generator: produces synthetic packets into ring buffer
void generator_stage(SPSCRingBuffer<Packet, 4096>& output_buffer,
                     Metrics& metrics,
                     uint64_t duration_seconds);

// Parse: reads packets, extracts minimal header info
void parse_stage(SPSCRingBuffer<Packet, 4096>& input_buffer,
                 SPSCRingBuffer<Packet, 4096>& output_buffer,
                 Metrics& metrics);

// Transform: applies filtering (drop packets < 64 bytes)
void transform_stage(SPSCRingBuffer<Packet, 4096>& input_buffer,
                     SPSCRingBuffer<Packet, 4096>& output_buffer,
                     Metrics& metrics);

// Aggregate: collects stats and prints to stdout
void aggregate_stage(SPSCRingBuffer<Packet, 4096>& input_buffer,
                     Metrics& metrics);

}  // namespace packet_pipeline
