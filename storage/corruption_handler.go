package storage

import (
	"fmt"

	"github.com/jathurchan/raftlock/logger"
)

// LogCorruptionHandler provides methods for handling log corruption
type LogCorruptionHandler interface {
	HandleCorruption(path string, offset int64, reason string, err error) error
}

// DefaultLogCorruptionHandler implements LogCorruptionHandler
type DefaultLogCorruptionHandler struct {
	fs     FileSystem
	logger logger.Logger
}

// NewDefaultLogCorruptionHandler creates a new DefaultLogCorruptionHandler
func NewDefaultLogCorruptionHandler(fs FileSystem, logger logger.Logger) *DefaultLogCorruptionHandler {
	return &DefaultLogCorruptionHandler{
		fs:     fs,
		logger: logger,
	}
}

// HandleCorruption logs the corruption, truncates the log file,
// and returns any error encountered during truncation.
func (h *DefaultLogCorruptionHandler) HandleCorruption(path string, offset int64, reason string, err error) error {
	h.logger.Warnw("Log corruption detected; truncating log file",
		"reason", reason,
		"corruptionOffset", offset,
		"logPath", path,
		"error", err,
	)

	if err := h.truncateLogAt(path, offset); err != nil {
		h.logger.Errorw("Failed to truncate corrupted log file", "path", path, "offset", offset, "error", err)
		return err
	}

	h.logger.Infow("Successfully truncated corrupted log file", "path", path, "offset", offset)
	return nil
}

// truncateLogAt truncates the log file at the specified offset.
func (h *DefaultLogCorruptionHandler) truncateLogAt(path string, offset int64) error {
	if offset < 0 {
		return fmt.Errorf("invalid negative offset (%d) for truncation", offset)
	}
	return h.fs.Truncate(path, offset)
}
