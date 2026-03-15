package tests

import (
	"testing"

	"pfs/pkg/client"
)

func TestBlockIndex(t *testing.T) {
	cases := []struct {
		offset, blockSize, want int64
	}{
		{0, 512, 0},
		{511, 512, 0},
		{512, 512, 1},
		{1023, 512, 1},
		{1024, 512, 2},
		{0, 1, 0},
		{100, 1, 100},
	}
	for _, tc := range cases {
		got := client.BlockIndex(tc.offset, tc.blockSize)
		if got != tc.want {
			t.Errorf("BlockIndex(%d, %d) = %d, want %d", tc.offset, tc.blockSize, got, tc.want)
		}
	}
}

func TestServerIndex(t *testing.T) {
	cases := []struct {
		blockIndex int64
		width      int
		want       int
	}{
		{0, 3, 0},
		{1, 3, 1},
		{2, 3, 2},
		{3, 3, 0},
		{4, 3, 1},
		{5, 3, 2},
		{0, 1, 0},
		{7, 1, 0},
	}
	for _, tc := range cases {
		got := client.ServerIndex(tc.blockIndex, tc.width)
		if got != tc.want {
			t.Errorf("ServerIndex(%d, %d) = %d, want %d", tc.blockIndex, tc.width, got, tc.want)
		}
	}
}

func TestOffsetOnServer(t *testing.T) {
	bs := int64(512)
	cases := []struct {
		blockIndex int64
		width      int
		want       int64
	}{
		{0, 3, 0},    // block 0 → server 0, local block 0 → offset 0
		{1, 3, 0},    // block 1 → server 1, local block 0 → offset 0
		{2, 3, 0},    // block 2 → server 2, local block 0 → offset 0
		{3, 3, 512},  // block 3 → server 0, local block 1 → offset 512
		{4, 3, 512},  // block 4 → server 1, local block 1 → offset 512
		{5, 3, 512},  // block 5 → server 2, local block 1 → offset 512
		{6, 3, 1024}, // block 6 → server 0, local block 2 → offset 1024
	}
	for _, tc := range cases {
		got := client.OffsetOnServer(tc.blockIndex, tc.width, bs)
		if got != tc.want {
			t.Errorf("OffsetOnServer(%d, %d, %d) = %d, want %d",
				tc.blockIndex, tc.width, bs, got, tc.want)
		}
	}
}

func TestComputeRanges(t *testing.T) {
	bs := int64(512)
	sw := 3

	t.Run("single block aligned", func(t *testing.T) {
		r := client.ComputeRanges(0, 512, bs, sw)
		if len(r) != 1 {
			t.Fatalf("want 1 range, got %d", len(r))
		}
		if r[0].BlockIndex != 0 || r[0].IntraOffset != 0 || r[0].Length != 512 {
			t.Errorf("unexpected range: %+v", r[0])
		}
	})

	t.Run("partial first block", func(t *testing.T) {
		r := client.ComputeRanges(256, 256, bs, sw)
		if len(r) != 1 {
			t.Fatalf("want 1 range, got %d", len(r))
		}
		if r[0].BlockIndex != 0 || r[0].IntraOffset != 256 || r[0].Length != 256 {
			t.Errorf("unexpected range: %+v", r[0])
		}
	})

	t.Run("spans two blocks", func(t *testing.T) {
		r := client.ComputeRanges(256, 512, bs, sw)
		if len(r) != 2 {
			t.Fatalf("want 2 ranges, got %d", len(r))
		}
		if r[0].BlockIndex != 0 || r[0].IntraOffset != 256 || r[0].Length != 256 {
			t.Errorf("range[0]: %+v", r[0])
		}
		if r[1].BlockIndex != 1 || r[1].IntraOffset != 0 || r[1].Length != 256 {
			t.Errorf("range[1]: %+v", r[1])
		}
	})

	t.Run("spans three blocks across servers", func(t *testing.T) {
		// offset=0, length=3*512 → blocks 0,1,2 on servers 0,1,2
		r := client.ComputeRanges(0, 3*512, bs, sw)
		if len(r) != 3 {
			t.Fatalf("want 3 ranges, got %d", len(r))
		}
		for i, rng := range r {
			if rng.BlockIndex != int64(i) {
				t.Errorf("range[%d].BlockIndex = %d, want %d", i, rng.BlockIndex, i)
			}
			if rng.ServerIdx != i {
				t.Errorf("range[%d].ServerIdx = %d, want %d", i, rng.ServerIdx, i)
			}
		}
	})

	t.Run("zero length", func(t *testing.T) {
		r := client.ComputeRanges(0, 0, bs, sw)
		if r != nil {
			t.Errorf("want nil, got %v", r)
		}
	})
}
