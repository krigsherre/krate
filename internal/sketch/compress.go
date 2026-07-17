package sketch

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func Compress(data []byte) []byte {
	if len(data) < int(headerSize) {
		return data
	}

	width := binary.BigEndian.Uint32(data[3:7])
	depth := binary.BigEndian.Uint32(data[7:11])

	var buf bytes.Buffer
	buf.Grow(headerSize + int(width)*int(depth))
	buf.Write(data[:headerSize])

	varintBuf := make([]byte, binary.MaxVarintLen64)
	off := int(headerSize)

	for i := uint32(0); i < depth; i++ {
		var prev uint64
		for j := uint32(0); j < width; j++ {
			cell := binary.BigEndian.Uint64(data[off : off+8])
			off += 8

			var delta int64
			if j == 0 {
				delta = int64(cell)
			} else {
				delta = int64(cell) - int64(prev)
			}
			prev = cell

			zz := zigzagEncode(delta)
			n := binary.PutUvarint(varintBuf, zz)
			buf.Write(varintBuf[:n])
		}
	}
	return buf.Bytes()
}

func Decompress(data []byte) ([]byte, error) {
	if len(data) < int(headerSize) {
		return nil, fmt.Errorf("sketch: compressed data too short (%d bytes)", len(data))
	}
	if string(data[0:2]) != magicBytes {
		return nil, fmt.Errorf("sketch: invalid magic %q in compressed data", string(data[0:2]))
	}
	if data[2] != versionByte {
		return nil, fmt.Errorf("sketch: unsupported version %d in compressed data", data[2])
	}

	width := binary.BigEndian.Uint32(data[3:7])
	depth := binary.BigEndian.Uint32(data[7:11])

	outSize := int(headerSize) + int(width)*int(depth)*8
	out := make([]byte, outSize)
	copy(out[:headerSize], data[:headerSize])

	r := bytes.NewReader(data[headerSize:])
	outOff := int(headerSize)

	for i := uint32(0); i < depth; i++ {
		var prev int64
		for j := uint32(0); j < width; j++ {
			zz, err := binary.ReadUvarint(r)
			if err != nil {
				return nil, fmt.Errorf("sketch: varint read error at row %d col %d: %w", i, j, err)
			}

			delta := zigzagDecode(zz)

			var cell int64
			if j == 0 {
				prev = delta
				cell = delta
			} else {
				prev = prev + delta
				cell = prev
			}

			binary.BigEndian.PutUint64(out[outOff:outOff+8], uint64(cell))
			outOff += 8
		}
	}

	if r.Len() > 0 {
		return nil, fmt.Errorf("sketch: %d trailing bytes in compressed data", r.Len())
	}
	return out, nil
}

func zigzagEncode(n int64) uint64 {
	return uint64((n >> 63) ^ (n << 1))
}

func zigzagDecode(u uint64) int64 {
	return int64((u >> 1) ^ -(u & 1))
}
