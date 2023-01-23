#pragma once
#include "interface/StorageInterface.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "Units.hpp"
#include "leanstore/utils/FNVHash.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{

constexpr static int kLockWords = 4093;
class OptimisticLockGuard;
enum class OptimisticLockState {
    UNINITIALIZED,
    READ_LOCKED,
    WRITE_LOCKED,
    MOVED
};

class OptimisticLockTable {
public:
    OptimisticLockTable() {
        for (size_t i = 0; i < kLockWords; ++i) {
            words[i].store(0);
        }
    }

    u64 get_key_version(u64 hash) {
        return words[hash % kLockWords];
    }
friend class OptimisticLockGuard;

private:

std::atomic<u64> words[kLockWords];

};

class OptimisticLockGuard {
public:
    OptimisticLockGuard() = delete;
    OptimisticLockGuard(OptimisticLockGuard&&) = delete;
    OptimisticLockGuard(OptimisticLockGuard&) = delete;
    // Assignment operator
    constexpr OptimisticLockGuard& operator=(OptimisticLockGuard& other) = delete;
    constexpr OptimisticLockGuard& operator=(OptimisticLockGuard && other)
    {
        state = other.state;
        lock_table = other.lock_table;
        word_pos = other.word_pos;
        version = other.version;
        other.state = OptimisticLockState::MOVED;
        other.lock_table = nullptr;
        return *this;
    }
    
    OptimisticLockGuard(OptimisticLockTable *lock_table, u64 hash): lock_table(lock_table) {
        word_pos = hash % kLockWords;
        version = lock_table->words[word_pos];
        state = OptimisticLockState::UNINITIALIZED;
    }

    OptimisticLockGuard(OptimisticLockTable *lock_table, 
                      const u8 * key, const u32 length): lock_table(lock_table) {
        word_pos = leanstore::utils::FNV::hash(key, length) % kLockWords;
        version = lock_table->words[word_pos];
        state = OptimisticLockState::UNINITIALIZED;
    }

    bool read_lock() {
        assert(state == OptimisticLockState::UNINITIALIZED);
        version = lock_table->words[word_pos];
        if (version % 2 == 0) {
            state = OptimisticLockState::READ_LOCKED;
            return true;
        }
        return false;
    }

    bool write_lock() {
        assert(state == OptimisticLockState::UNINITIALIZED);
        version = lock_table->words[word_pos];
        if (version % 2 == 1) {
            return false;
        }
        u64 v = version;
        if (lock_table->words[word_pos].compare_exchange_strong(v, v + 1)) {
            version = version + 1;
            state = OptimisticLockState::WRITE_LOCKED;
            return true;
        } else {
            return false;
        }
    }

    bool more_than_one_writer_since(u64 v) {
        assert(version > v);
        return version - v > 1;
    }

    bool validate() {
        return lock_table->words[word_pos].load() == version;
    }

    bool upgrade_to_write_lock() {
        assert(version % 2 == 0);// read mode
        u64 v = version;
        if (lock_table->words[word_pos].compare_exchange_strong(v, v + 1)) {
            version = version + 1;
            state = OptimisticLockState::WRITE_LOCKED;
            return true;
        } else {
            return false;
        }
    }

    ~OptimisticLockGuard() {
        if (state == OptimisticLockState::WRITE_LOCKED) {
            assert(lock_table->words[word_pos] == version);
            assert(lock_table->words[word_pos].load() % 2 == 1);// must be write-locked
            lock_table->words[word_pos].fetch_add(1);
            state = OptimisticLockState::MOVED;
        }
    }

    u64 get_version() {
        return version;
    }
private:
    u32 word_pos;
    u64 version;
    OptimisticLockTable* lock_table = nullptr;
    OptimisticLockState state = OptimisticLockState::UNINITIALIZED;
};



}