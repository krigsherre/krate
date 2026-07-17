package sketch

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sync/atomic"
)

const (
	magicBytes     = "CM"
	versionByte    = byte(1)
	headerSize     = 19
	goldenRatioMix = 0x9e3779b97f4a7c15
)

type CountMinSketch struct {
	width    uint32
	depth    uint32
	table    [][]uint64
	seeds    []uint64
	baseSeed uint64
}

func NewCMS(width, depth uint32, seed uint64) *CountMinSketch {
	cms := &CountMinSketch{
		width:    width,
		depth:    depth,
		table:    make([][]uint64, depth),
		seeds:    make([]uint64, depth),
		baseSeed: seed,
	}
	for i := uint32(0); i < depth; i++ {
		cms.table[i] = make([]uint64, width)
		cms.seeds[i] = seed + uint64(i)*goldenRatioMix
	}
	return cms
}

func (c *CountMinSketch) hash(key string, seed uint64) uint32 {
	h := fnv.New64a()
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, seed)
	h.Write(b)
	h.Write([]byte(key))
	return uint32(h.Sum64() % uint64(c.width))
}

func (c *CountMinSketch) Add(key string, count uint64) {
	for i := uint32(0); i < c.depth; i++ {
		idx := c.hash(key, c.seeds[i])
		atomic.AddUint64(&c.table[i][idx], count)
	}
}

func (c *CountMinSketch) Query(key string) uint64 {
	min := ^uint64(0)
	for i := uint32(0); i < c.depth; i++ {
		idx := c.hash(key, c.seeds[i])
		v := atomic.LoadUint64(&c.table[i][idx])
		if v < min {
			min = v
		}
	}
	return min
}

func (c *CountMinSketch) Reset() {
	for i := uint32(0); i < c.depth; i++ {
		for j := uint32(0); j < c.width; j++ {
			c.table[i][j] = 0
		}
	}
}

func (c *CountMinSketch) Clone() *CountMinSketch {
	clone := &CountMinSketch{
		width:    c.width,
		depth:    c.depth,
		table:    make([][]uint64, c.depth),
		seeds:    make([]uint64, c.depth),
		baseSeed: c.baseSeed,
	}
	for i := uint32(0); i < c.depth; i++ {
		clone.table[i] = make([]uint64, c.width)
		copy(clone.table[i], c.table[i])
	}
	copy(clone.seeds, c.seeds)
	return clone
}

func (c *CountMinSketch) Merge(other *CountMinSketch, strategy MergeStrategy) {
	mergeTables(c, other, strategy)
}

func (c *CountMinSketch) Marshal() []byte {
	size := int(headerSize) + int(c.width)*int(c.depth)*8
	buf := make([]byte, size)

	copy(buf[0:2], magicBytes)
	buf[2] = versionByte
	binary.BigEndian.PutUint32(buf[3:7], c.width)
	binary.BigEndian.PutUint32(buf[7:11], c.depth)
	binary.BigEndian.PutUint64(buf[11:19], c.baseSeed)

	off := int(headerSize)
	for i := uint32(0); i < c.depth; i++ {
		for j := uint32(0); j < c.width; j++ {
			binary.BigEndian.PutUint64(buf[off:off+8], c.table[i][j])
			off += 8
		}
	}
	return buf
}

func Unmarshal(data []byte) (*CountMinSketch, error) {
	if len(data) < int(headerSize) {
		return nil, fmt.Errorf("sketch: data too short for header (%d bytes)", len(data))
	}
	if string(data[0:2]) != magicBytes {
		return nil, fmt.Errorf("sketch: invalid magic %q", string(data[0:2]))
	}
	if data[2] != versionByte {
		return nil, fmt.Errorf("sketch: unsupported version %d", data[2])
	}

	width := binary.BigEndian.Uint32(data[3:7])
	depth := binary.BigEndian.Uint32(data[7:11])
	seed := binary.BigEndian.Uint64(data[11:19])

	expected := int(headerSize) + int(width)*int(depth)*8
	if len(data) < expected {
		return nil, fmt.Errorf("sketch: data too short for table (have %d, need %d)", len(data), expected)
	}

	cms := NewCMS(width, depth, seed)
	off := int(headerSize)
	for i := uint32(0); i < depth; i++ {
		for j := uint32(0); j < width; j++ {
			cms.table[i][j] = binary.BigEndian.Uint64(data[off : off+8])
			off += 8
		}
	}
	return cms, nil
}
