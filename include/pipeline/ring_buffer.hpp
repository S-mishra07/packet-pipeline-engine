#pragma once

#include <atomic>
#include <cstddef>
#include <array>

namespace packet_pipeline {

// Minimal SPSC ring buffer: single producer, single consumer
// No locks, no batches, just head/tail atomic indices
template <typename T, size_t Capacity>
class SPSCRingBuffer {
public:
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be power of 2");
    
    SPSCRingBuffer() : head_(0), tail_(0) {}
    
    // Producer: try to write one item
    // Returns true if written, false if buffer full
    bool try_push(const T& item) {
        size_t h = head_.load(std::memory_order_relaxed);
        size_t next_h = (h + 1) & (Capacity - 1);
        
        // Check if full: next write position equals tail
        if (next_h == tail_.load(std::memory_order_acquire)) {
            return false;  // Buffer full, drop packet
        }
        
        buffer_[h] = item;
        head_.store(next_h, std::memory_order_release);
        return true;
    }
    
    // Consumer: try to read one item
    // Returns true if read, false if buffer empty
    bool try_pop(T& item) {
        size_t t = tail_.load(std::memory_order_relaxed);
        
        if (t == head_.load(std::memory_order_acquire)) {
            return false;  // Buffer empty
        }
        
        item = buffer_[t];
        tail_.store((t + 1) & (Capacity - 1), std::memory_order_release);
        return true;
    }
    
    // Get current number of items in buffer (approximate, for monitoring)
    size_t size() const {
        size_t h = head_.load(std::memory_order_relaxed);
        size_t t = tail_.load(std::memory_order_relaxed);
        return (h - t) & (Capacity - 1);
    }
    
private:
    std::array<T, Capacity> buffer_;
    std::atomic<size_t> head_;
    std::atomic<size_t> tail_;
};

}  // namespace packet_pipeline
