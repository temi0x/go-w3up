package dags

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/dag/model"
	"github.com/storacha/guppy/pkg/preparation/types"
)

type Repo interface {
	UpdateDAGScan(ctx context.Context, dagScan model.DAGScan) error
	FileOrCreateRawNode(ctx context.Context, cid cid.Cid, size uint64, path string, sourceID types.SourceID, offset uint64) (*model.RawNode, bool, error)
	FindOrCreateUnixFSNode(ctx context.Context, cid cid.Cid, size uint64, ufsdata []byte) (*model.UnixFSNode, bool, error)
	CreateLinks(ctx context.Context, parent cid.Cid, links []model.LinkParams) error
}
