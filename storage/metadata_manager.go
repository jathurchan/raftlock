package storage

import (
	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// MetadataManager manages the log metadata
type MetadataManager struct {
	logger logger.Logger
}

// NewMetadataManager creates a new MetadataManager
func NewMetadataManager(logger logger.Logger) *MetadataManager {
	return &MetadataManager{
		logger: logger,
	}
}

// SyncMetadataFromIndex updates firstLogIndex and lastLogIndex based on the indexMap
func (m *MetadataManager) GetIndicesFromMap(
	indexMap []types.IndexOffsetPair,
	currentFirst, currentLast types.Index,
	operationContext string) (types.Index, types.Index, bool, error) {

	if len(indexMap) > 0 {
		newFirstIndex := indexMap[0].Index
		newLastIndex := indexMap[len(indexMap)-1].Index

		if currentFirst == newFirstIndex && currentLast == newLastIndex {
			m.logger.Debugw("Metadata already up to date, skipping update",
				"context", operationContext,
				"firstIndex", newFirstIndex,
				"lastIndex", newLastIndex)
			return currentFirst, currentLast, false, nil
		}

		m.logger.Infow("Updated metadata from index map",
			"context", operationContext,
			"firstIndex", newFirstIndex,
			"lastIndex", newLastIndex)

		return newFirstIndex, newLastIndex, true, nil
	}

	// No entries in the map: reset state only if it's not already zeroed
	if currentFirst == 0 && currentLast == 0 {
		m.logger.Debugw("Index map empty and metadata already zeroed",
			"context", operationContext)
		return 0, 0, false, nil
	}

	m.logger.Warnw("Index map empty, reset metadata to empty state",
		"context", operationContext)

	return 0, 0, true, nil
}
