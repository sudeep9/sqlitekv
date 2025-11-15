package main

import (
	"sync"
	"time"
)

type HLC struct {
	mu       sync.Mutex
	physical int64  // last physical time seen (seconds)
	logical  uint16 // logical counter
}

// New creates a new Hybrid Logical Clock.
func NewHLC() *HLC {
	return &HLC{}
}

// Now returns the current HLC timestamp (64-bit integer).
func (h *HLC) Now() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now().Unix() // epoch seconds

	if now > h.physical {
		// Real time moved forward — reset logical counter
		h.physical = now
		h.logical = 0
	} else {
		// Same or past physical time — increment logical counter
		if h.logical < 0xFFFF {
			h.logical++
		} else {
			// Logical overflow — block until next second
			for now <= h.physical {
				time.Sleep(time.Millisecond)
				now = time.Now().Unix()
			}
			h.physical = now
			h.logical = 0
		}
	}

	return pack(h.physical, h.logical)
}

// Update merges a remote HLC timestamp into the local clock.
/*
func (h *HLC) Update(remote uint64) uint64 {
	rPhys, rLog := unpack(remote)

	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now().Unix()

	localPhys := h.physical
	localLog := h.logical

	// new physical = max(now, localPhys, rPhys)
	physical := max64(now, localPhys, rPhys)

	var logical uint16

	if physical == localPhys && physical == rPhys {
		logical = max16(localLog, rLog) + 1
	} else if physical == localPhys {
		logical = localLog + 1
	} else if physical == rPhys {
		logical = rLog + 1
	} else {
		logical = 0
	}

	h.physical = physical
	h.logical = logical

	return pack(physical, logical)
}
*/

// pack builds a 64-bit HLC: [48 bits physical | 16 bits logical]
func pack(physical int64, logical uint16) int64 {
	return (int64(physical) << 16) | int64(logical)
}

// unpack extracts physical and logical components
func unpack(ts uint64) (physical int64, logical uint16) {
	physical = int64(ts >> 16)
	logical = uint16(ts & 0xFFFF)
	return
}

func max64(a, b, c int64) int64 {
	if a >= b && a >= c {
		return a
	}
	if b >= a && b >= c {
		return b
	}
	return c
}

func max16(a, b uint16) uint16 {
	if a > b {
		return a
	}
	return b
}
