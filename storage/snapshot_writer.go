package storage

import (
	"context"
	"fmt"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// snapshotWriteHooks provides lifecycle callbacks during snapshot writing.
type snapshotWriteHooks struct {
	OnTempFilesWritten  func()
	OnMetadataCommitted func()
}

// snapshotWriter writes Raft snapshots to persistent storage.
type snapshotWriter interface {
	Write(ctx context.Context, metadata types.SnapshotMetadata, data []byte) error
}

type defaultSnapshotWriter struct {
	fs              fileSystem
	serializer      serializer
	logger          logger.Logger
	dir             string
	hooks           *snapshotWriteHooks
	enableChunkedIO bool
	chunkSize       int
}

func newSnapshotWriter(
	fs fileSystem,
	serializer serializer,
	logger logger.Logger,
	dir string,
	hooks *snapshotWriteHooks,
	enableChunkedIO bool,
	chunkSize int,
) snapshotWriter {
	return &defaultSnapshotWriter{
		fs:              fs,
		serializer:      serializer,
		logger:          logger.WithComponent("snapshotwriter"),
		dir:             dir,
		hooks:           hooks,
		enableChunkedIO: enableChunkedIO,
		chunkSize:       chunkSize,
	}
}

func (sw *defaultSnapshotWriter) Write(ctx context.Context, metadata types.SnapshotMetadata, data []byte) error {
	metaPath := sw.fs.Path(sw.dir, snapshotMetaFilename)
	dataPath := sw.fs.Path(sw.dir, snapshotDataFilename)
	tmpMetaPath := sw.fs.TempPath(metaPath)
	tmpDataPath := sw.fs.TempPath(dataPath)

	var err error

	defer func() {
		if err != nil {
			sw.cleanupOnError(tmpMetaPath, tmpDataPath)
		}
	}()

	if err = sw.prepareSnapshotFiles(ctx, metadata, data, tmpMetaPath, tmpDataPath); err != nil {
		return err
	}

	if err = sw.commitSnapshotFiles(tmpMetaPath, tmpDataPath, metaPath, dataPath); err != nil {
		return err
	}

	sw.logger.Infow("Snapshot written successfully",
		"lastIndex", metadata.LastIncludedIndex,
		"lastTerm", metadata.LastIncludedTerm,
		"size", len(data))

	return nil
}

func (w *defaultSnapshotWriter) prepareSnapshotFiles(
	ctx context.Context,
	metadata types.SnapshotMetadata,
	data []byte,
	tmpMetaPath string,
	tmpDataPath string,
) error {
	metaBytes, err := w.serializer.MarshalSnapshotMetadata(metadata)
	if err != nil {
		return fmt.Errorf("%w: failed to marshal snapshot metadata: %v", ErrStorageIO, err)
	}

	if err := w.fs.WriteFile(tmpMetaPath, metaBytes, ownRWOthR); err != nil {
		return fmt.Errorf("%w: failed to write temp snapshot metadata: %v", ErrStorageIO, err)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := w.writeSnapshotData(ctx, tmpDataPath, data); err != nil {
		return err
	}

	if w.hooks != nil && w.hooks.OnTempFilesWritten != nil {
		w.hooks.OnTempFilesWritten()
	}

	return nil
}

func (w *defaultSnapshotWriter) writeSnapshotData(ctx context.Context, path string, data []byte) error {
	file, err := w.fs.Open(path)
	if err != nil {
		return fmt.Errorf("%w: could not open snapshot file: %v", ErrStorageIO, err)
	}
	defer file.Close()

	if w.enableChunkedIO && len(data) > w.chunkSize {
		return writeChunks(ctx, file, data, w.chunkSize)
	}

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("%w: failed to write snapshot data: %v", ErrStorageIO, err)
	}
	return file.Sync()
}

func (w *defaultSnapshotWriter) commitSnapshotFiles(
	tmpMetaPath, tmpDataPath, finalMetaPath, finalDataPath string,
) error {
	if err := atomicRenameOrCleanup(w.fs, tmpMetaPath, finalMetaPath); err != nil {
		return fmt.Errorf("%w: failed to commit snapshot metadata: %v", ErrStorageIO, err)
	}

	if w.hooks != nil && w.hooks.OnMetadataCommitted != nil {
		w.hooks.OnMetadataCommitted()
	}

	if err := atomicRenameOrCleanup(w.fs, tmpDataPath, finalDataPath); err != nil {
		_ = w.fs.Remove(finalMetaPath) // rollback
		return fmt.Errorf("%w: failed to commit snapshot data: %v", ErrStorageIO, err)
	}

	return nil
}

func (w *defaultSnapshotWriter) cleanupOnError(tmpMeta, tmpData string) {
	_ = w.fs.Remove(tmpMeta)
	_ = w.fs.Remove(tmpData)
}
