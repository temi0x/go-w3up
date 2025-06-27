package unixfs

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/multiformats/go-multihash"
)

var EmptyFileCID cid.Cid

func init() {
	hasher := sha256.New()
	hash := hasher.Sum(nil)
	mh, _ := multihash.Encode(hash, multihash.SHA2_256)
	EmptyFileCID = cid.NewCidV1(cid.Raw, mh)
}

type Chunker interface {
	NextBytes() (cid.Cid, []byte, error)
	Offset() uint64
}

func NewChunker(r io.Reader, chunkSize uint64) Chunker {
	if chunkSize == 0 {
		chunkSize = 1024 * 1024 // Default to 1MB chunks
	}
	return &chunker{
		currentOffset: 0,
		chunkSize:     chunkSize,
		r:             r,
	}
}

type chunker struct {
	currentOffset uint64
	chunkSize     uint64
	r             io.Reader
}

func (c *chunker) NextBytes() (cid.Cid, []byte, error) {
	buf := make([]byte, c.chunkSize)
	hasher := sha256.New()
	io.TeeReader(c.r, hasher) // Discard the read bytes to avoid memory leaks
	n, err := c.r.Read(buf)
	if err != nil {
		return cid.Undef, nil, err
	}
	c.currentOffset += uint64(n)
	hash := hasher.Sum(nil)
	mh, err := multihash.Encode(hash, multihash.SHA2_256)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("failed to encode multihash: %w", err)
	}
	return cid.NewCidV1(cid.Raw, mh), buf[:n], nil
}

func (c *chunker) Offset() uint64 {
	return c.currentOffset
}

func BuildNode(ufsData []byte, pbLinks []dagpb.PBLink) (datamodel.Node, error) {
	dpbb := dagpb.Type.PBNode.NewBuilder()
	pbm, err := dpbb.BeginMap(2)
	if err != nil {
		return nil, err
	}
	pblb, err := pbm.AssembleEntry("Links")
	if err != nil {
		return nil, err
	}
	pbl, err := pblb.BeginList(int64(len(pbLinks)))
	if err != nil {
		return nil, err
	}
	for _, pbln := range pbLinks {
		if err = pbl.AssembleValue().AssignNode(pbln); err != nil {
			return nil, err
		}
	}
	if err = pbl.Finish(); err != nil {
		return nil, err
	}
	if err = pbm.AssembleKey().AssignString("Data"); err != nil {
		return nil, err
	}
	if err = pbm.AssembleValue().AssignBytes(ufsData); err != nil {
		return nil, err
	}
	if err = pbm.Finish(); err != nil {
		return nil, err
	}
	return dpbb.Build(), nil
}

func WritePBNode(nd datamodel.Node) (cid.Cid, []byte, error) {
	var buf bytes.Buffer
	hasher := sha256.New()
	w := io.MultiWriter(&buf, hasher)
	if err := dagpb.Encode(nd, w); err != nil {
		return cid.Undef, nil, fmt.Errorf("failed to encode PBNode: %w", err)
	}
	data := buf.Bytes()
	hash := hasher.Sum(nil)
	mh, err := multihash.Encode(hash, multihash.SHA2_256)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("failed to encode multihash: %w", err)
	}
	cid := cid.NewCidV1(cid.DagProtobuf, mh)
	return cid, data, nil
}
