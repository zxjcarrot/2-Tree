#pragma once
#include <cstdlib>
#include <cstdint>
#include <memory>
#include <thread>
#include <mutex>
#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE  64 // 64 byte cache line on x86-64
#endif

struct PadInt {
    PadInt() { data = 0; }

    union {
        uint64_t data;
        char padding[CACHE_LINE_SIZE];
    };
};


template<int buckets = 32>
class DistributedCounter {
public:
    static_assert(buckets == 0 || (buckets & (buckets - 1)) == 0, "buckets must be a multiple of 2");

    DistributedCounter(int initVal = 0) {
        for (int i = 0; i < buckets; ++i) {
            countArray[i].data = 0;
        }
        increment(initVal);
    }

    void operator+=(int by) { increment(by); }

    void operator-=(int by) { decrement(by); }

    void operator++() { *this += 1; }

    void operator++(int) { *this += 1; }

    void operator--() { *this -= 1; }

    void operator--(int) { *this -= 1; }

    int64_t load() const { return get(); }
    
    operator int64_t() {
        return get();
    }

    void store(int64_t v) {
        for (int i = 0; i < buckets; ++i) {
            countArray[i].data = 0;
        }
        countArray[0].data = v;
    }

    inline void increment(int v = 1) {
        int idx =  arrayIndex();
        assert(idx>=0);
        assert(idx < buckets);
        __atomic_add_fetch(&countArray[idx].data, v, __ATOMIC_RELAXED);
    }

    inline void decrement(int v = 1) {
        int idx =  arrayIndex();
        assert(idx>=0);
        assert(idx < buckets);
        __atomic_sub_fetch(&countArray[idx].data, v, __ATOMIC_RELAXED);
    }

    int64_t get() const {
        int64_t val = 0;
        for (int i = 0; i < buckets; ++i) {
            val += __atomic_load_n(&countArray[i].data, __ATOMIC_RELAXED);
        }
        return val;
    }

private:
    inline uint64_t getCPUId() {
        return cpuId;
    }

    inline int arrayIndex() {
        return getCPUId() & (buckets - 1);
    }

    static thread_local uint64_t cpuId;
    PadInt countArray[buckets];
};

template<int buckets>
thread_local uint64_t DistributedCounter<buckets>::cpuId = (uint64_t) std::hash<std::thread::id>{}(std::this_thread::get_id());