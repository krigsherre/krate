package sketch

import "fmt"

type MergeStrategy int

const (
	MergeMax MergeStrategy = iota
	MergeMin
	MergeSum
	MergeReplace
)

func mergeTables(dst, src *CountMinSketch, strategy MergeStrategy) {
	if dst.width != src.width || dst.depth != src.depth {
		panic(fmt.Sprintf("sketch: dimension mismatch: dst=%dx%d, src=%dx%d",
			dst.width, dst.depth, src.width, src.depth))
	}

	for i := uint32(0); i < dst.depth; i++ {
		for j := uint32(0); j < dst.width; j++ {
			switch strategy {
			case MergeMax:
				if src.table[i][j] > dst.table[i][j] {
					dst.table[i][j] = src.table[i][j]
				}
			case MergeMin:
				if src.table[i][j] < dst.table[i][j] {
					dst.table[i][j] = src.table[i][j]
				}
			case MergeSum:
				dst.table[i][j] += src.table[i][j]
			case MergeReplace:
				dst.table[i][j] = src.table[i][j]
			}
		}
	}
}
