package storage

import (
	"fmt"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// LogConsistencyVerifier verifies log consistency
type LogConsistencyVerifier struct {
	logger logger.Logger
}

// NewLogConsistencyVerifier creates a new LogConsistencyVerifier
func NewLogConsistencyVerifier(logger logger.Logger) *LogConsistencyVerifier {
	return &LogConsistencyVerifier{
		logger: logger,
	}
}

// VerifyLogContinuity ensures that the indexMap forms a contiguous sequence of log entries.
func (v *LogConsistencyVerifier) VerifyLogContinuity(indexMap []types.IndexOffsetPair) error {
	mapLen := len(indexMap)

	if mapLen == 0 {
		v.logger.Debugw("Log continuity check skipped: empty index map")
		return nil
	}

	for i := 1; i < mapLen; i++ {
		expected := indexMap[i-1].Index + 1
		actual := indexMap[i].Index

		if actual != expected {
			v.logger.Errorw("Discontinuity detected in log index map",
				"previousIndex", indexMap[i-1].Index,
				"expectedNext", expected,
				"actualNext", actual)
			return fmt.Errorf("%w: log entries not contiguous at %d -> %d",
				ErrCorruptedLog, indexMap[i-1].Index, actual)
		}
	}

	v.logger.Debugw("Log continuity verified: all entries are contiguous")
	return nil
}
