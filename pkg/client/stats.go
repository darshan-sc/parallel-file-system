package client

// PfsExecstat tracks client-side I/O statistics across all operations.
type PfsExecstat struct {
	NumReadHits        int
	NumWriteHits       int
	NumEvictions       int
	NumWritebacks      int
	NumInvalidations   int
	NumCloseWritebacks int
	NumCloseEvictions  int
}

// PfsMetadata is the client-facing view of file metadata.
type PfsMetadata struct {
	Filename    string
	Size        int64
	Ctime       int64
	Mtime       int64
	BlockSize   int64
	StripeWidth int32
	ServerAddrs []string
}
