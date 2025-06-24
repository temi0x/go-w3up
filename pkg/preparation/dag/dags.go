package dags

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/dag/file"
	"github.com/storacha/guppy/pkg/preparation/dag/model"
	"github.com/storacha/guppy/pkg/preparation/dag/visitor"
	"github.com/storacha/guppy/pkg/preparation/types"
)

type UnixFSParams interface {
	ChunkSize() uint64
	LinksPerNode() uint64
	HAMTDirectoryMinimumSize() uint64
}

type DAGAPI struct {
	Repo         Repo
	FileAccessor FileAccessorFn
	UnixFSParams UnixFSParamsFn
}

type FileAccessorFn func(ctx context.Context, fsEntryID types.FSEntryID) (fs.File, types.SourceID, string, error)
type UnixFSParamsFn func(ctx context.Context, uploadID types.UploadID) UnixFSParams

// ExecuteDAGScan executes a dag scan on the given fs entry, creating a unix fs dag for the given file or directory.
func (d DAGAPI) ExecuteDAGScan(ctx context.Context, dagScan model.DAGScan, nodeCB func(node model.Node, data []byte) error) error {
	err := dagScan.Start()
	if err != nil {
		return fmt.Errorf("starting scan: %w", err)
	}
	if err := d.Repo.UpdateDAGScan(ctx, dagScan); err != nil {
		return fmt.Errorf("updating scan: %w", err)
	}
	cid, err := d.executeDAGScan(ctx, dagScan, nodeCB)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			if err := dagScan.Cancel(); err != nil {
				return fmt.Errorf("cancelling scan: %w", err)
			}
		} else {
			if err := dagScan.Fail(err.Error()); err != nil {
				return fmt.Errorf("failing scan: %w", err)
			}
		}
	} else {
		if err := dagScan.Complete(cid); err != nil {
			return fmt.Errorf("completing scan: %w", err)
		}
	}
	if err := d.Repo.UpdateDAGScan(ctx, dagScan); err != nil {
		return fmt.Errorf("updating scan after fail: %w", err)
	}
	return nil
}

func (d DAGAPI) executeDAGScan(ctx context.Context, dagScan model.DAGScan, nodeCB func(node model.Node, data []byte) error) (cid.Cid, error) {
	switch ds := dagScan.(type) {
	case *model.FileDAGScan:
		return d.executeFileDAGScan(ctx, ds, nodeCB)
	case *model.DirectoryDAGScan:
		return d.executeDirectoryDAGScan(ctx, ds, nodeCB)
	default:
		return cid.Undef, fmt.Errorf("unrecognized DAG scan type: %T", dagScan)
	}
}

func (d DAGAPI) executeFileDAGScan(ctx context.Context, dagScan *model.FileDAGScan, nodeCB func(node model.Node, data []byte) error) (cid.Cid, error) {
	f, sourceID, path, err := d.FileAccessor(ctx, dagScan.FsEntryID())
	if err != nil {
		return cid.Undef, fmt.Errorf("accessing file for DAG scan: %w", err)
	}
	defer f.Close()
	unixFSParams := d.UnixFSParams(ctx, dagScan.UploadID())
	visitor := visitor.NewUnixFSVisitor(ctx, d.Repo, sourceID, path, nodeCB)
	return file.BuildUnixFSFile(f, unixFSParams.ChunkSize(), unixFSParams.LinksPerNode(), visitor)
}

func (d DAGAPI) executeDirectoryDAGScan(ctx context.Context, dagScan *model.DirectoryDAGScan, nodeCB func(node model.Node, data []byte) error) (cid.Cid, error) {
	return cid.Undef, errors.New("not implemented: executeDirectoryDAGScan")
}
