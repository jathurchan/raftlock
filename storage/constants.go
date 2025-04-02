package storage

const (
	// OwnRWOthR represents file permission 0644 (owner read/write, others read)
	OwnRWOthR = 0644

	// OwnRWXOthRX represents directory permission 0755 (owner read/write/execute, others read/execute)
	OwnRWXOthRX = 0755

	// DefaultLockTimeoutSeconds is the default timeout for lock acquisition
	DefaultLockTimeoutSeconds = 5

	// DefaultChunkSizeBytes is the default chunk size for large I/O operations (1MB)
	DefaultChunkSizeBytes = 1024 * 1024

	// DefaultRetainedLogSize is the default minimum number of log entries to keep after truncation [NEW]
	DefaultRetainedLogSize = 100

	// DefaultIndexMapInitialCapacity is the default initial capacity for the index-to-offset map [NEW]
	DefaultIndexMapInitialCapacity = 1024

	// MaxEntrySize is the maximum allowed size for a log entry (64MB)
	MaxEntrySizeBytes = 64 * 1024 * 1024

	// LengthPrefixSize is the size of length prefixes in bytes
	LengthPrefixSize = 4

	// IndexSize is the size of index field in binary format
	IndexSize = 8

	// TermSize is the size of term field in binary format
	TermSize = 8

	// DataLengthSize is the size of data length field in binary format
	DataLengthSize = 8

	// HeaderSize is the total size of binary entry header (index + term + data length)
	HeaderSize = IndexSize + TermSize + DataLengthSize
)
