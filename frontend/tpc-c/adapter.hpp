#pragma once
#include "types.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/LeanStore.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <map>
#include <string>

using namespace leanstore;

template<class Record>
struct Adapter {
   typedef typename Record::Key KeyType;

   typedef std::function<bool(const KeyType &, const Record &)> ScanFunc;
   virtual void scanDesc(const KeyType & key, const ScanFunc& fn, std::function<void()> undo) = 0;
   virtual void scan(const KeyType& key, const ScanFunc& fn, std::function<void()> undo);

   virtual void insert(const KeyType& rec_key, const Record& record) = 0;

   typedef std::function<void(const Record &)> LookupFunc;
   virtual void lookup1(const KeyType & key, const LookupFunc& fn) = 0;

   typedef std::function<void(Record &)> UpdateFunc;
   virtual void update1(const KeyType& key, const UpdateFunc& fn, storage::btree::WALUpdateGenerator wal_update_generator) = 0;

   virtual bool erase(const KeyType& key) = 0;

   virtual uint64_t count() = 0;
};
template <class Record>
struct LeanStoreBTreeAdapter: public Adapter<Record> {

   using typename Adapter<Record>::KeyType;
   using typename Adapter<Record>::ScanFunc;
   using typename Adapter<Record>::LookupFunc;
   using typename Adapter<Record>::UpdateFunc;

   storage::btree::BTreeInterface* btree;
   std::map<std::string, Record> map;
   string name;
   LeanStoreBTreeAdapter()
   {
      // hack
   }
   LeanStoreBTreeAdapter(LeanStore& db, string name) : name(name)
   {
      if (FLAGS_recover) {
         btree = &db.retrieveBTreeLL(name);
      } else {
         btree = &db.registerBTreeLL(name);
      }
   }
   // -------------------------------------------------------------------------------------
   void printTreeHeight() { cout << name << " height = " << btree->getHeight() << endl; }
   // -------------------------------------------------------------------------------------
   
   void scanDesc(const KeyType& key, const ScanFunc& fn, std::function<void()> undo) override
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      btree->scanDesc(
          folded_key, folded_key_len,
          [&](const u8* key, [[maybe_unused]] u16 key_length, const u8* payload, [[maybe_unused]] u16 payload_length) {
             if (key_length != folded_key_len) {
                return false;
             }
             KeyType typed_key;
             Record::unfoldRecord(key, typed_key);
             const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
             return fn(typed_key, typed_payload);
          },
          undo);
   }
   // -------------------------------------------------------------------------------------
   void insert(const KeyType& rec_key, const Record& record) override
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, rec_key);
      const auto res = btree->insert(folded_key, folded_key_len, (u8*)(&record), sizeof(Record));
      ensure(res == btree::OP_RESULT::OK || res == btree::OP_RESULT::ABORT_TX);
      if (res == btree::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
   }

   
   void lookup1(const KeyType& key, const LookupFunc& fn) override
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      const auto res = btree->lookup(folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
         static_cast<void>(payload_length);
         const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
         assert(payload_length == sizeof(Record));
         fn(typed_payload);
      });
      ensure(res == btree::OP_RESULT::OK);
   }

   
   void update1(const KeyType& key, const UpdateFunc& fn, storage::btree::WALUpdateGenerator wal_update_generator) override
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      const auto res = btree->updateSameSize(
          folded_key, folded_key_len,
          [&](u8* payload, u16 payload_length) {
             static_cast<void>(payload_length);
             assert(payload_length == sizeof(Record));
             Record& typed_payload = *reinterpret_cast<Record*>(payload);
             fn(typed_payload);
          },
          wal_update_generator);
      ensure(res != btree::OP_RESULT::NOT_FOUND);
      if (res == btree::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
   }

   bool erase(const KeyType& key) override
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      const auto res = btree->remove(folded_key, folded_key_len);
      if (res == btree::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
      return (res == btree::OP_RESULT::OK);
   }
   // -------------------------------------------------------------------------------------
   
   void scan(const KeyType& key, const ScanFunc& fn, std::function<void()> undo) override
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      btree->scanAsc(
          folded_key, folded_key_len,
          [&](const u8* key, u16 key_length, const u8* payload, u16 payload_length) {
             if (key_length != folded_key_len) {
                return false;
             }
             static_cast<void>(payload_length);
             KeyType typed_key;
             Record::unfoldRecord(key, typed_key);
             const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
             return fn(typed_key, typed_payload);
          },
          undo);
   }
   uint64_t count() override { return btree->countEntries(); }
};
