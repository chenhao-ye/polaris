//
// Implemented by authors of SILO.
//

#ifndef _AMD64_H_
#define _AMD64_H_

#include <stdint.h>

#define ALWAYS_INLINE __attribute__((always_inline))

inline ALWAYS_INLINE void
nop_pause()
{
  __asm volatile("pause" : :);
}

inline void
memory_barrier() {
  asm volatile("mfence" : : : "memory");
}

inline ALWAYS_INLINE uint64_t
rdtsc(void)
{
  uint32_t hi, lo;
  __asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
  return ((uint64_t)lo)|(((uint64_t)hi)<<32);
}

#endif /* _AMD64_H_ */

