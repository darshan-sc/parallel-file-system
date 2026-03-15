package client

// BlockIndex returns which logical block number contains the given byte offset.
func BlockIndex(offset, blockSize int64) int64 {
	return offset / blockSize
}

// ServerIndex returns which server (0-based) owns the given logical block.
func ServerIndex(blockIndex int64, stripeWidth int) int {
	return int(blockIndex) % stripeWidth
}

// OffsetOnServer returns the byte offset within a single server's storage
// for the start of the given logical block.
func OffsetOnServer(blockIndex int64, stripeWidth int, blockSize int64) int64 {
	return (blockIndex/int64(stripeWidth))*blockSize
}

// BlockRange describes a contiguous region within a single block on one server.
type BlockRange struct {
	BlockIndex   int64
	ServerIdx    int
	ServerOffset int64 // byte offset on server where this block starts
	IntraOffset  int64 // byte offset within the block (first byte of data)
	Length       int64 // number of bytes from this block
}

// ComputeRanges breaks [offset, offset+length) into BlockRange slices,
// one entry per block touched.
func ComputeRanges(offset, length, blockSize int64, stripeWidth int) []BlockRange {
	if length <= 0 {
		return nil
	}
	var ranges []BlockRange
	remaining := length
	pos := offset

	for remaining > 0 {
		bi := BlockIndex(pos, blockSize)
		intraOffset := pos % blockSize
		canRead := blockSize - intraOffset
		if canRead > remaining {
			canRead = remaining
		}
		ranges = append(ranges, BlockRange{
			BlockIndex:   bi,
			ServerIdx:    ServerIndex(bi, stripeWidth),
			ServerOffset: OffsetOnServer(bi, stripeWidth, blockSize),
			IntraOffset:  intraOffset,
			Length:       canRead,
		})
		pos += canRead
		remaining -= canRead
	}
	return ranges
}
