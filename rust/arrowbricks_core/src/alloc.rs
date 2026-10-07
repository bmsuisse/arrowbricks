//! Returns large buffers to the OS as soon as they are freed.
//!
//! glibc raises its mmap threshold (up to 32 MiB) whenever a large mapped
//! block is freed, so later multi-MiB buffers come from the fragmented heap
//! and are never handed back: repeating one query in a single process grew
//! idle RSS from 200 MB to 1.4 GB. Blocks of `LARGE` bytes or more bypass
//! malloc here (`mmap`/`munmap`, `mremap` to grow without copying); smaller
//! ones use the system allocator. Only Rust allocations in this extension are
//! affected, not the host process's malloc. Linux with glibc only (the one
//! configuration this was measured on); see `lib.rs` for the gate.
//!
//! Failure mode: `mmap`/`mremap` return null when the address-space limit is
//! hit (`RLIMIT_AS`, strict overcommit, `vm.max_map_count`), which Rust turns
//! into an abort like any other failed allocation.

#![warn(clippy::undocumented_unsafe_blocks)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::ptr;

const LARGE: usize = 1 << 20;
const PAGE: usize = 4096;

pub struct LargeBlockAlloc;

/// Must stay a pure function of the `Layout`: `alloc`, `realloc` and `dealloc`
/// each call it on the same block and must get the same answer, otherwise a
/// block is freed by the wrong allocator.
fn is_large(layout: Layout) -> bool {
    layout.size() >= LARGE && layout.align() <= PAGE
}

/// The length the kernel maps for `size` bytes. Cannot overflow: a `Layout`
/// size is at most `isize::MAX`.
fn mapped_len(size: usize) -> usize {
    size.next_multiple_of(PAGE)
}

// SAFETY: every method routes a block to `mmap` or to `System` based only on
// `is_large(layout)`, and the caller must pass the block's original layout to
// `dealloc`/`realloc` (the `GlobalAlloc` contract), so a block is always
// released by the allocator that produced it. Mappings are page-aligned, which
// satisfies every `align <= PAGE`; larger alignments go to `System`.
unsafe impl GlobalAlloc for LargeBlockAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if !is_large(layout) {
            // SAFETY: `layout` is forwarded unchanged; the caller upholds `alloc`'s contract.
            return unsafe { System.alloc(layout) };
        }
        // SAFETY: an anonymous private mapping with no address hint; the length is
        // non-zero because `layout.size() >= LARGE`.
        let p = unsafe {
            libc::mmap(
                ptr::null_mut(),
                mapped_len(layout.size()),
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if p == libc::MAP_FAILED {
            ptr::null_mut()
        } else {
            p.cast()
        }
    }

    // Fresh anonymous mappings are already zeroed.
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if is_large(layout) {
            // SAFETY: same contract as `alloc`, which returns zeroed pages for large layouts.
            unsafe { self.alloc(layout) }
        } else {
            // SAFETY: `layout` is forwarded unchanged to the system allocator.
            unsafe { System.alloc_zeroed(layout) }
        }
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        if is_large(layout) {
            // SAFETY: `p` and `layout` came from this allocator, so a large layout means `p`
            // is the start of a mapping of exactly `mapped_len(layout.size())` bytes.
            let rc = unsafe { libc::munmap(p.cast(), mapped_len(layout.size())) };
            debug_assert_eq!(rc, 0, "munmap failed: the layout does not match the block");
        } else {
            // SAFETY: a small layout means `p` came from `System` with this same layout.
            unsafe { System.dealloc(p, layout) };
        }
    }

    unsafe fn realloc(&self, p: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller guarantees `new_size`, rounded up to `layout.align()`, does not
        // overflow `isize::MAX`, and `layout.align()` is already a valid alignment.
        let new_layout = unsafe { Layout::from_size_align_unchecked(new_size, layout.align()) };
        match (is_large(layout), is_large(new_layout)) {
            // SAFETY: both layouts are small, so `p` is a `System` block of `layout`.
            (false, false) => unsafe { System.realloc(p, layout, new_size) },
            (true, true) => {
                let (old_len, new_len) = (mapped_len(layout.size()), mapped_len(new_size));
                if old_len == new_len {
                    return p;
                }
                // SAFETY: `p` is the start of a mapping of `old_len` bytes; on failure
                // `mremap` leaves that mapping intact and the null result tells the caller.
                let q = unsafe { libc::mremap(p.cast(), old_len, new_len, libc::MREMAP_MAYMOVE) };
                if q == libc::MAP_FAILED {
                    ptr::null_mut()
                } else {
                    q.cast()
                }
            }
            _ => {
                // Crossing the threshold: new block from the matching allocator, copy, free old.
                // SAFETY: `new_layout` is valid (see above) and non-zero sized.
                let q = unsafe { self.alloc(new_layout) };
                if !q.is_null() {
                    // SAFETY: `p` is valid for `layout.size()` bytes and `q` for `new_size`, so
                    // both are valid for the smaller of the two; they are distinct blocks.
                    unsafe {
                        ptr::copy_nonoverlapping(p, q, layout.size().min(new_size));
                        self.dealloc(p, layout);
                    }
                }
                q
            }
        }
    }
}

#[cfg(test)]
// The tests drive the allocator's raw interface directly, so each block would repeat the same note.
#[allow(clippy::undocumented_unsafe_blocks)]
mod tests {
    use super::*;

    fn fill(p: *mut u8, n: usize, seed: u8) {
        for i in 0..n {
            unsafe { *p.add(i) = seed.wrapping_add(i as u8) };
        }
    }

    fn check(p: *const u8, n: usize, seed: u8) {
        for i in 0..n {
            assert_eq!(unsafe { *p.add(i) }, seed.wrapping_add(i as u8), "byte {i}");
        }
    }

    /// Fails if the `#[global_allocator]` static in `lib.rs` is removed or mis-gated: only
    /// this allocator hands out page-aligned multi-MiB blocks (glibc's mmap path returns
    /// a 16-byte header offset).
    #[test]
    fn global_allocator_serves_large_vecs_from_page_aligned_mappings() {
        let blocks: Vec<Vec<u8>> = (0..8).map(|_| Vec::with_capacity(2 * LARGE)).collect();
        for b in &blocks {
            assert_eq!(
                b.as_ptr() as usize % PAGE,
                0,
                "LargeBlockAlloc is not the global allocator"
            );
        }
    }

    #[test]
    fn realloc_keeps_contents_across_the_large_threshold() {
        let a = LargeBlockAlloc;
        let sizes = [
            100,
            4096,
            LARGE - 1,
            LARGE,
            LARGE + 1,
            3 * LARGE + 17,
            40 * LARGE,
            LARGE + 5,
            64,
            2 * LARGE,
        ];
        unsafe {
            let mut layout = Layout::from_size_align(sizes[0], 8).unwrap();
            let mut p = a.alloc(layout);
            assert!(!p.is_null());
            fill(p, layout.size(), 7);
            for &next in &sizes[1..] {
                let keep = layout.size().min(next);
                let q = a.realloc(p, layout, next);
                assert!(!q.is_null());
                check(q, keep, 7);
                layout = Layout::from_size_align(next, 8).unwrap();
                fill(q, next, 7);
                p = q;
            }
            a.dealloc(p, layout);
        }
    }

    #[test]
    fn large_blocks_are_zeroed_and_writable_to_the_last_byte() {
        let a = LargeBlockAlloc;
        let layout = Layout::from_size_align(5 * LARGE + 123, 64).unwrap();
        unsafe {
            let p = a.alloc_zeroed(layout);
            assert!(!p.is_null());
            assert_eq!(p as usize % 64, 0);
            assert!((0..layout.size()).step_by(4093).all(|i| *p.add(i) == 0));
            *p.add(layout.size() - 1) = 9;
            a.dealloc(p, layout);
        }
    }

    #[test]
    fn over_aligned_large_requests_fall_back_to_the_system_allocator() {
        let a = LargeBlockAlloc;
        let layout = Layout::from_size_align(2 * LARGE, 1 << 16).unwrap();
        unsafe {
            let p = a.alloc(layout);
            assert!(!p.is_null());
            assert_eq!(p as usize % (1 << 16), 0);
            a.dealloc(p, layout);
        }
    }

    #[test]
    fn concurrent_random_alloc_realloc_dealloc_keeps_every_block_intact() {
        let handles: Vec<_> = (0..8u64)
            .map(|t| {
                std::thread::spawn(move || {
                    let a = LargeBlockAlloc;
                    let mut state = 0x9E37_79B9_7F4A_7C15u64 ^ t;
                    let mut next = move || {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        state
                    };
                    let mut live: Vec<(*mut u8, Layout, u8)> = Vec::new();
                    for _ in 0..1500 {
                        let r = next();
                        let size = match r % 4 {
                            0 => (r >> 8) as usize % 5000 + 1,
                            1 => LARGE - 3 + (r >> 8) as usize % 7,
                            2 => LARGE + (r >> 8) as usize % (3 * LARGE),
                            _ => (r >> 8) as usize % (300 * 1024) + 1,
                        };
                        let seed = (r >> 32) as u8;
                        if live.len() < 24 && r % 3 != 0 {
                            let layout = Layout::from_size_align(size, 8).unwrap();
                            let p = unsafe { a.alloc(layout) };
                            assert!(!p.is_null());
                            fill(p, size.min(8192), seed);
                            live.push((p, layout, seed));
                        } else if !live.is_empty() {
                            let i = (r >> 16) as usize % live.len();
                            let (p, layout, seed) = live.swap_remove(i);
                            check(p, layout.size().min(8192), seed);
                            if r % 2 == 0 {
                                unsafe { a.dealloc(p, layout) };
                            } else {
                                let q = unsafe { a.realloc(p, layout, size) };
                                assert!(!q.is_null());
                                check(q, layout.size().min(size).min(8192), seed);
                                fill(q, size.min(8192), seed);
                                live.push((q, Layout::from_size_align(size, 8).unwrap(), seed));
                            }
                        }
                    }
                    for (p, layout, seed) in live {
                        check(p, layout.size().min(8192), seed);
                        unsafe { a.dealloc(p, layout) };
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    }
}
