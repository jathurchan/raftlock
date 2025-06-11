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

// snapshotReader reads metadata and snapshot data from storage.
type snapshotReader interface {
	Read(ctx context.Context) (types.SnapshotMetadata, []byte, error)
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

type defaultSnapshotReader struct {
	fs              fileSystem
	serializer      serializer
	logger          logger.Logger
	dir             string
	enableChunkedIO bool
	chunkSize       int
}

func (sw *defaultSnapshotWriter) Write(
	ctx context.Context,
	metadata types.SnapshotMetadata,
	data []byte,
) error {
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
		return fmt.Errorf("%w: failed to marshal snapshot metadata: %w", ErrStorageIO, err)
	}

	if err := w.fs.WriteFile(tmpMetaPath, metaBytes, ownRWOthR); err != nil {
		return fmt.Errorf("%w: failed to write temp snapshot metadata: %w", ErrStorageIO, err)
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

func (w *defaultSnapshotWriter) writeSnapshotData(
	ctx context.Context,
	path string,
	data []byte,
) error {
	file, err := w.fs.Open(path)
	if err != nil {
		return fmt.Errorf("%w: could not open snapshot file: %w", ErrStorageIO, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			w.logger.Errorw("error closing file: %v", err)
		}
	}()

	if w.enableChunkedIO && len(data) > w.chunkSize {
		return writeChunks(ctx, file, data, w.chunkSize)
	}

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("%w: failed to write snapshot data: %w", ErrStorageIO, err)
	}
	return file.Sync()
}

func (w *defaultSnapshotWriter) commitSnapshotFiles(
	tmpMetaPath, tmpDataPath, finalMetaPath, finalDataPath string,
) error {
	if err := atomicRenameOrCleanup(w.fs, tmpMetaPath, finalMetaPath); err != nil {
		return fmt.Errorf("%w: failed to commit snapshot metadata: %w", ErrStorageIO, err)
	}

	if w.hooks != nil && w.hooks.OnMetadataCommitted != nil {
		w.hooks.OnMetadataCommitted()
	}

	if err := atomicRenameOrCleanup(w.fs, tmpDataPath, finalDataPath); err != nil {
		_ = w.fs.Remove(finalMetaPath) // rollback
		return fmt.Errorf("%w: failed to commit snapshot data: %w", ErrStorageIO, err)
	}

	return nil
}

func (w *defaultSnapshotWriter) cleanupOnError(tmpMeta, tmpData string) {
	_ = w.fs.Remove(tmpMeta)
	_ = w.fs.Remove(tmpData)
}

func newSnapshotReader(
	fs fileSystem,
	serializer serializer,
	logger logger.Logger,
	dir string,
) snapshotReader {
	return &defaultSnapshotReader{
		fs:         fs,
		serializer: serializer,
		logger:     logger.WithComponent("snapshotreader"),
		dir:        dir,
	}
}

func (sr *defaultSnapshotReader) Read(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
	if err := ctx.Err(); err != nil {
		return types.SnapshotMetadata{}, nil, err
	}

	metaPath, err := sr.readAndValidateMetadata()
	if err != nil {
		return types.SnapshotMetadata{}, nil, err
	}

	snapshotPath := sr.fs.Path(sr.dir, snapshotDataFilename)

	data, err := sr.readSnapshotData(ctx, snapshotPath)
	if err != nil {
		return types.SnapshotMetadata{}, nil, err
	}

	sr.logger.Infow("Snapshot loaded",
		"lastIndex", metaPath.LastIncludedIndex,
		"lastTerm", metaPath.LastIncludedTerm,
		"dataSize", len(data),
	)

	return metaPath, data, nil
}

func (r *defaultSnapshotReader) readAndValidateMetadata() (types.SnapshotMetadata, error) {
	metaPath := r.fs.Path(r.dir, snapshotMetaFilename)

	metaBytes, err := r.fs.ReadFile(metaPath)
	if err != nil {
		if r.fs.IsNotExist(err) {
			return types.SnapshotMetadata{}, ErrNoSnapshot
		}
		return types.SnapshotMetadata{}, fmt.Errorf(
			"%w: could not read snapshot metadata: %w",
			ErrStorageIO,
			err,
		)
	}

	meta, err := r.serializer.UnmarshalSnapshotMetadata(metaBytes)
	if err != nil {
		return types.SnapshotMetadata{}, fmt.Errorf(
			"%w: invalid snapshot metadata: %w",
			ErrCorruptedSnapshot,
			err,
		)
	}

	if meta.LastIncludedIndex == 0 {
		return types.SnapshotMetadata{}, fmt.Errorf(
			"%w: LastIncludedIndex cannot be zero",
			ErrCorruptedSnapshot,
		)
	}

	return meta, nil
}

func (r *defaultSnapshotReader) readSnapshotData(ctx context.Context, path string) ([]byte, error) {
	info, err := r.fs.Stat(path)
	if err != nil {
		if r.fs.IsNotExist(err) {
			return nil, fmt.Errorf("%w: snapshot data file does not exist", ErrCorruptedSnapshot)
		}
		return nil, fmt.Errorf("%w: unable to stat snapshot file: %w", ErrStorageIO, err)
	}

	size := info.Size()
	useChunkedIO := r.enableChunkedIO && r.chunkSize > 0 && size > int64(r.chunkSize)

	if useChunkedIO {
		file, err := r.fs.Open(path)
		if err != nil {
			return nil, fmt.Errorf("%w: unable to open snapshot file: %w", ErrStorageIO, err)
		}
		defer func() {
			if err := file.Close(); err != nil {
				r.logger.Errorw("error closing file: %v", err)
			}
		}()

		data, err := readChunks(ctx, file, size, r.chunkSize)
		if err != nil {
			return nil, fmt.Errorf("%w: chunked read failed: %w", ErrStorageIO, err)
		}
		return data, nil
	}

	data, err := r.fs.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read snapshot file: %w", ErrStorageIO, err)
	}
	return data, nil
}
