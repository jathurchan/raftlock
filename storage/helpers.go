package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/jathurchan/raftlock/types"
)

// fileExists checks if a file exists
func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err // Some other error occurred
}

// atomicWriteFile writes data to a temporary file and then renames it.
func atomicWriteFile(targetPath string, data []byte, perm os.FileMode) error {
	// Create parent directories if needed
	dir := filepath.Dir(targetPath)
	if err := os.MkdirAll(dir, OwnRWXOthRX); err != nil {
		return fmt.Errorf("%w: failed to create directory %q: %v", ErrStorageIO, dir, err)
	}

	// Write to temporary file
	tmpPath := targetPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, perm); err != nil {
		return handleErrorWithCleanup(
			fmt.Errorf("%w: failed to write temporary file %q: %v", ErrStorageIO, tmpPath, err),
			tmpPath,
		)
	}

	// Rename temp file to target path (atomic)
	if err := os.Rename(tmpPath, targetPath); err != nil {
		return handleErrorWithCleanup(
			fmt.Errorf("%w: failed to rename temporary file %q to %q: %v", ErrStorageIO, tmpPath, targetPath, err),
			tmpPath,
		)
	}

	return nil
}

// handleErrorWithCleanup attempts to remove the temporary file and combines any cleanup
// error with the primary error.
func handleErrorWithCleanup(primaryErr error, tmpPath string) error {
	if rmErr := os.Remove(tmpPath); rmErr != nil {
		return fmt.Errorf("%w; additionally failed to clean up temp file: %v", primaryErr, rmErr)
	}
	return primaryErr
}

// serializeLogEntry encodes a LogEntry into a binary format with a trailing checksum.
//
// The serialized layout is:
// [Index (8 bytes)][Term (8 bytes)][Command Length (8 bytes)][Command Data][Checksum (4 bytes)]
//
// The checksum is calculated over all preceding bytes (i.e., from Index through Command Data).
// This helps detect data corruption during storage or transmission.
//
// Returns the encoded byte slice representing the log entry.
func serializeLogEntry(entry types.LogEntry) []byte {
	cmdLen := len(entry.Command)
	checksumOffset := HeaderSize + cmdLen
	totalSize := checksumOffset + ChecksumSize
	buf := make([]byte, totalSize)

	writeHeader(buf, entry.Index, entry.Term, cmdLen)
	copy(buf[HeaderSize:], entry.Command)

	checksum := computeChecksum(buf[:checksumOffset])
	binary.BigEndian.PutUint32(buf[checksumOffset:], checksum)

	return buf
}

// writeHeader writes the log entry header into the provided buffer.
//
// Header format:
// [Index (8 bytes)][Term (8 bytes)][Command Length (8 bytes)]
func writeHeader(buf []byte, index types.Index, term types.Term, commandLen int) {
	binary.BigEndian.PutUint64(buf[0:IndexSize], uint64(index))
	binary.BigEndian.PutUint64(buf[IndexSize:IndexSize+TermSize], uint64(term))
	binary.BigEndian.PutUint64(buf[IndexSize+TermSize:HeaderSize], uint64(commandLen))
}

// deserializeLogEntry decodes a binary blob back into a LogEntry,
// verifying its structure and checksum for integrity.
//
// Expects the format:
// [Index (8 bytes)][Term (8 bytes)][Command Length (8 bytes)][Command Data][Checksum (4 bytes)]
//
// Returns an error if:
// - the data is too short
// - the declared command length doesn't match actual data length
// - the checksum does not match the expected value
func deserializeLogEntry(data []byte) (types.LogEntry, error) {
	index, term, cmdLen, err := parseHeader(data)
	if err != nil {
		return types.LogEntry{}, err
	}

	checksumOffset := HeaderSize + cmdLen
	expectedLen := checksumOffset + ChecksumSize
	if len(data) != expectedLen {
		return types.LogEntry{}, fmt.Errorf("%w: incorrect total length", ErrCorruptedLog)
	}

	checksum := binary.BigEndian.Uint32(data[checksumOffset:])
	if err := verifyChecksum(data[:checksumOffset], checksum); err != nil {
		return types.LogEntry{}, err
	}

	command := make([]byte, cmdLen)
	copy(command, data[HeaderSize:checksumOffset])

	return types.LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}, nil
}

// parseHeader extracts the index, term, and command length fields from the binary log entry header.
//
// Returns an error if the data is too short or the declared command length would exceed available data.
func parseHeader(data []byte) (index types.Index, term types.Term, commandLen int, err error) {
	if len(data) < HeaderSize {
		return 0, 0, 0, fmt.Errorf("%w: header too short", ErrCorruptedLog)
	}

	index = types.Index(binary.BigEndian.Uint64(data[0:IndexSize]))
	term = types.Term(binary.BigEndian.Uint64(data[IndexSize : IndexSize+TermSize]))
	cmdLen := binary.BigEndian.Uint64(data[IndexSize+TermSize : HeaderSize])

	if cmdLen > uint64(len(data)-HeaderSize-ChecksumSize) {
		return 0, 0, 0, fmt.Errorf("%w: declared command length too large", ErrCorruptedLog)
	}

	return index, term, int(cmdLen), nil
}

// verifyChecksum compares the computed checksum against the expected value.
//
// Returns an error if they do not match.
func verifyChecksum(data []byte, expected uint32) error {
	actual := computeChecksum(data)
	if actual != expected {
		return fmt.Errorf("%w: checksum mismatch", ErrCorruptedLog)
	}
	return nil
}

// computeChecksum calculates a CRC32 checksum over the provided data using IEEE polynomial.
func computeChecksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
