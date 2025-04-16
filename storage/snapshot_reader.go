package storage

import (
	"context"
	"fmt"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// snapshotReader reads metadata and snapshot data from storage.
type snapshotReader interface {
	Read(ctx context.Context) (types.SnapshotMetadata, []byte, error)
}

type defaultSnapshotReader struct {
	fs              fileSystem
	serializer      serializer
	logger          logger.Logger
	dir             string
	enableChunkedIO bool
	chunkSize       int
}

func newSnapshotReader(fs fileSystem, serializer serializer, logger logger.Logger, dir string) snapshotReader {
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

	if err := ctx.Err(); err != nil {
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
		return types.SnapshotMetadata{}, fmt.Errorf("%w: could not read snapshot metadata: %v", ErrStorageIO, err)
	}

	meta, err := r.serializer.UnmarshalSnapshotMetadata(metaBytes)
	if err != nil {
		return types.SnapshotMetadata{}, fmt.Errorf("%w: invalid snapshot metadata: %v", ErrCorruptedSnapshot, err)
	}

	if meta.LastIncludedIndex == 0 {
		return types.SnapshotMetadata{}, fmt.Errorf("%w: LastIncludedIndex cannot be zero", ErrCorruptedSnapshot)
	}

	return meta, nil
}

func (r *defaultSnapshotReader) readSnapshotData(ctx context.Context, path string) ([]byte, error) {
	info, err := r.fs.Stat(path)
	if err != nil {
		if r.fs.IsNotExist(err) {
			return nil, fmt.Errorf("%w: snapshot data file does not exist", ErrCorruptedSnapshot)
		}
		return nil, fmt.Errorf("%w: unable to stat snapshot file: %v", ErrStorageIO, err)
	}

	size := info.Size()
	useChunkedIO := r.enableChunkedIO && r.chunkSize > 0 && size > int64(r.chunkSize)

	if useChunkedIO {
		file, err := r.fs.Open(path)
		if err != nil {
			return nil, fmt.Errorf("%w: unable to open snapshot file: %v", ErrStorageIO, err)
		}
		defer file.Close()

		data, err := readChunks(ctx, file, size, r.chunkSize)
		if err != nil {
			return nil, fmt.Errorf("%w: chunked read failed: %v", ErrStorageIO, err)
		}
		return data, nil
	}

	data, err := r.fs.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read snapshot file: %v", ErrStorageIO, err)
	}
	return data, nil
}
