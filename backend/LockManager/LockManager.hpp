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
friend class PessimisticLockGuard;

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
        if (version % 2 == 1) { // write-locked
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


class PessimisticLockGuard {
public:
    u64 kReadersOffset = 56;
    u64 kReadersMask = 0xFF00000000000000ULL;
    u64 kWriteVersionOffset = 0;
    u64 kWriteVersionMask = ~kReadersMask;
    
    u16 get_readers(u64 word) {
        return (word & kReadersMask) >> kReadersOffset;
    }

    u16 get_writer_version(u64 word) {
        return (word & kWriteVersionMask) >> kWriteVersionOffset;
    }
    
    u64 make_word(u64 readers, u64 writer_version) {
        return (readers << kReadersOffset) | writer_version;
    }

    bool do_write_lock(std::atomic<u64> & word) {
        auto old_word = word.load();
        u64 readers = get_readers(old_word);
        u64 writer_version = get_writer_version(old_word);
        if (readers != 0 || writer_version % 2) { // has readers or write-locked
            return false;
        }
        auto new_word = make_word(readers, writer_version + 1);
        return word.compare_exchange_strong(old_word, new_word);
    }

    void do_write_unlock(std::atomic<u64> & word) {
        while (true) {
            auto old_word = word.load();
            u64 readers = get_readers(old_word);
            u64 writer_version = get_writer_version(old_word);
            assert((writer_version % 2) == 1); // must be write-locked
            assert(readers == 0); // must has no readers
            auto new_word = make_word(readers, writer_version + 1);
            if (word.compare_exchange_strong(old_word, new_word)) {
                return;
            }
            assert(false); // should never reach here
        }
    }

    bool do_upgrade_write_lock(std::atomic<u64> & word) {
        auto old_word = word.load();
        u64 readers = get_readers(old_word);
        u64 writer_version = get_writer_version(old_word);
        if (readers != 1 || writer_version % 2) { // has readers or write-locked
            return false;
        }
        // We are the only reader
        auto new_word = make_word(0, writer_version + 1);
        return word.compare_exchange_strong(old_word, new_word);
    }

    bool do_read_lock(std::atomic<u64> & word) {
        auto old_word = word.load();
        u64 readers = get_readers(old_word);
        u64 writer_version = get_writer_version(old_word);
        if (writer_version % 2) { // write-locked
            return false;
        }
        auto new_word = make_word(readers + 1, writer_version);
        return word.compare_exchange_strong(old_word, new_word);
    }

    void do_read_unlock(std::atomic<u64> & word) {
        while (true) {
            auto old_word = word.load();
            u64 readers = get_readers(old_word);
            u64 writer_version = get_writer_version(old_word);
            assert((writer_version % 2) == 0); // must not be write-locked
            assert(readers >= 1); // must has readers
            auto new_word = make_word(readers - 1, writer_version);
            if (word.compare_exchange_strong(old_word, new_word)) {
                return;
            }
        }
    }


    PessimisticLockGuard() = delete;
    PessimisticLockGuard(PessimisticLockGuard&&) = delete;
    PessimisticLockGuard(PessimisticLockGuard&) = delete;
    // Assignment operator
    constexpr PessimisticLockGuard& operator=(PessimisticLockGuard& other) = delete;
    constexpr PessimisticLockGuard& operator=(PessimisticLockGuard && other)
    {
        state = other.state;
        lock_table = other.lock_table;
        word_pos = other.word_pos;
        version = other.version;
        other.state = OptimisticLockState::MOVED;
        other.version = nullptr;
        other.lock_table = nullptr;
        return *this;
    }
    
    PessimisticLockGuard(OptimisticLockTable *lock_table, u64 hash): lock_table(lock_table) {
        word_pos = hash % kLockWords;
        version = &lock_table->words[word_pos];
        state = OptimisticLockState::UNINITIALIZED;
    }

    PessimisticLockGuard(OptimisticLockTable *lock_table, 
                      const u8 * key, const u32 length): lock_table(lock_table) {
        word_pos = leanstore::utils::FNV::hash(key, length) % kLockWords;
        version = &lock_table->words[word_pos];
        state = OptimisticLockState::UNINITIALIZED;
    }

    bool read_lock() {
        assert(state == OptimisticLockState::UNINITIALIZED);
        version = &lock_table->words[word_pos];
        if (do_read_lock(*version)) {
            state = OptimisticLockState::READ_LOCKED;
            return true;
        }
        return false;
    }

    bool write_lock() {
        assert(state == OptimisticLockState::UNINITIALIZED);
        version = &lock_table->words[word_pos];
        if (do_write_lock(*version)) {
            state = OptimisticLockState::WRITE_LOCKED;
            return true;
        }
        return false;
    }

    bool more_than_one_writer_since(u64 v) {
        auto writer_version_latest = get_writer_version(version->load());
        auto writer_version = get_writer_version(v);
        assert(writer_version_latest > writer_version);
        return writer_version_latest - writer_version > 1;
    }

    bool validate() {
        return true;
    }

    bool upgrade_to_write_lock() {
        assert(state == OptimisticLockState::READ_LOCKED);
        version = &lock_table->words[word_pos];
        if (do_upgrade_write_lock(*version)) {
            state = OptimisticLockState::WRITE_LOCKED;
            return true;
        }
        return false;
    }

    ~PessimisticLockGuard() {
        if (state == OptimisticLockState::WRITE_LOCKED) {
            do_write_unlock(*version);
            state = OptimisticLockState::MOVED;
        } else if (state == OptimisticLockState::READ_LOCKED) {
            do_read_unlock(*version);
            state = OptimisticLockState::MOVED;
        }
    }

    u64 get_version() {
        return version->load();
    }
private:
    u32 word_pos;
    std::atomic<u64> * version;
    OptimisticLockTable* lock_table = nullptr;
    OptimisticLockState state = OptimisticLockState::UNINITIALIZED;
};

#define LockGuardProxy OptimisticLockGuard

//#define LockGuardProxy PessimisticLockGuard
}