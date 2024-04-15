#pragma once
#include <functional>
#include <cstdint>

class DeferCode {
public:
   DeferCode() = delete;
   DeferCode(std::function<void()> f): f(f) {}
   ~DeferCode() { 
      f(); 
   }
   std::function<void()> f;
};

class DeferCodeWithContext {
public:
   DeferCodeWithContext() = delete;
   DeferCodeWithContext(std::function<void(uint64_t)> f, uint64_t ctx): f(f), ctx(ctx) {}
   ~DeferCodeWithContext() { 
      f(ctx); 
   }
   std::function<void(uint64_t)> f;
   uint64_t ctx;
};