/*
  Adaptive Radix Tree
  Viktor Leis, 2012
  leis@in.tum.de

  Modified by Huanchen Zhang, 2016
 */

#include "ART.hpp"

#pragma once
#include <unordered_map>
#include <iostream>

#include "ART.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------

template<typename Key, typename Value>
class ARTIndex {
public:
	Key reverseKey(uint8_t*key_buf) {
		if (std::is_same<Key, uint16_t>::value) {
			return Key(__builtin_bswap16(*reinterpret_cast<uint16_t*>(key_buf)));
		} else if (std::is_same<Key, uint32_t>::value) {
			return Key(__builtin_bswap32(*reinterpret_cast<uint32_t*>(key_buf)));
		} else if (std::is_same<Key, uint64_t>::value) {
			return Key(__builtin_bswap64(*reinterpret_cast<uint64_t*>(key_buf)));
		} else {
			Key return_key;
			memcpy(&return_key, key_buf, sizeof(Key));
			return return_key;
		}
	}
	static constexpr std::size_t key_length = sizeof(Key);
	ARTIndex() {
		if (std::is_same<Key, uint16_t>::value) {
			prepareKeyBytes = [this](const Key & key,uint8_t* key_buf) {
				reinterpret_cast<uint16_t*>(key_buf)[0]=__builtin_bswap16(key);
			};
			loadKey = [this](uintptr_t value,uint8_t* key_buf) {
				this->prepareKeyBytes(reinterpret_cast<Value*>(value)->key, key_buf);
			};
		} else if (std::is_same<Key, uint32_t>::value) {
			prepareKeyBytes = [this](const Key & key,uint8_t* key_buf) {
				reinterpret_cast<uint32_t*>(key_buf)[0]=__builtin_bswap32(key);
			};
			loadKey = [this](uintptr_t value,uint8_t* key_buf) {
				this->prepareKeyBytes(reinterpret_cast<Value*>(value)->key, key_buf);
			};
		} else if (std::is_same<Key, uint64_t>::value) {
			prepareKeyBytes = [this](const Key & key,uint8_t* key_buf) {
				reinterpret_cast<uint64_t*>(key_buf)[0]=__builtin_bswap64(key);
			};
			loadKey = [this](uintptr_t value,uint8_t*key_buf) {
				this->prepareKeyBytes(reinterpret_cast<Value*>(value)->key, key_buf);
			};
		} else {
			prepareKeyBytes = [this](const Key & key,uint8_t* key_buf) {
				memcpy(reinterpret_cast<void*>(key_buf), (const void*)key, this->key_length);
			};
			loadKey = [this](uintptr_t value,uint8_t* key_buf) {
				this->prepareKeyBytes(reinterpret_cast<Value*>(value)->key, key_buf);
			};
		}
		idx = new ART(sizeof(Key), loadKey);
	}

	bool find(const Key & key, Value *& v_ptr) {
		uint8_t key_bytes[sizeof(key)];
		if (std::is_same<Key, uint16_t>::value || std::is_same<Key, uint32_t>::value || std::is_same<Key, uint64_t>::value) {
			prepareKeyBytes((uintptr_t)key, key_bytes);
		} else {
			prepareKeyBytes((uintptr_t)&key, key_bytes);
		}
		auto found_value = idx->lookup(key_bytes, sizeof(Key), sizeof(Key));
		if (found_value == 0) {
			return false;
		} else {
			v_ptr = reinterpret_cast<Value*>(found_value);
			return true;
		}
	}

	bool exists(const Key & key) {
		Value * v_ptr;
		return find(key, v_ptr);
	}

	void insert(const Key & key, Value * v) {
		uint8_t key_bytes[sizeof(key)];
		if (std::is_same<Key, uint16_t>::value || std::is_same<Key, uint32_t>::value || std::is_same<Key, uint64_t>::value) {
			prepareKeyBytes((uintptr_t)key, key_bytes);
		} else {
			prepareKeyBytes((uintptr_t)&key, key_bytes);
		}
		idx->insert(key_bytes, (uintptr_t)v, sizeof(Key));
	}

	void erase(const Key & key) {
		uint8_t key_bytes[sizeof(key)];
		if (std::is_same<Key, uint16_t>::value || std::is_same<Key, uint32_t>::value || std::is_same<Key, uint64_t>::value) {
			prepareKeyBytes((uintptr_t)key, key_bytes);
		} else {
			prepareKeyBytes((uintptr_t)&key, key_bytes);
		}
		idx->erase(key_bytes, sizeof(Key), sizeof(Key));
	}

	std::size_t get_cache_size() {
		return idx->getMemory() + idx->getNumItems() * sizeof(Value);
	}

	std::size_t size() {
		return idx->getNumItems();
	}

	class Iterator {
	public:
		Iterator(): idx(nullptr) {}
		Iterator(ARTIndex<Key, Value> * idx, const ARTIter & iter): idx(idx), iter(iter) {
			reached_end = iter.value() == 0;
		}

		Value* value() {
			return reinterpret_cast<Value*>(iter.value());
		}

		Key key() {
			return idx->reverseKey(iter.key());
		}

		void operator ++(int) {
			bool has_more = iter++;
			reached_end = !has_more || iter.value() == 0;
		}

		bool end(){
			return reached_end;
		}
	private:
		ARTIndex<Key, Value> * idx;
		ARTIter iter;
		bool reached_end;
	};

	Iterator begin() {
		ARTIter iter(idx);
		iter.begin();
		iter++;
		return Iterator(this, iter);
	}

	Iterator lower_bound(const Key & key) {
		uint8_t key_bytes[sizeof(key)];
		if (std::is_same<Key, uint16_t>::value || std::is_same<Key, uint32_t>::value || std::is_same<Key, uint64_t>::value) {
			prepareKeyBytes((uintptr_t)key, key_bytes);
		} else {
			prepareKeyBytes((uintptr_t)&key, key_bytes);
		}
		ARTIter iter(idx);
		idx->lower_bound(key_bytes, sizeof(Key), sizeof(Key), &iter);
		return Iterator(this, iter);
	}
	
private:
	std::function<void(uintptr_t,uint8_t*)> loadKey;
	std::function<void(const Key &,uint8_t*)> prepareKeyBytes;
	ART * idx;
};