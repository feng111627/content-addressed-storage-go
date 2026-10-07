// Package store 在多个存储节点之上实现多副本写入、取回、完整性校验与修复。
package store

import (
	"context"
	"errors"

	"github.com/feng111627/content-addressed-storage-go/pkg/contentid"
	"github.com/feng111627/content-addressed-storage-go/pkg/node"
)

// 可区分的失败原因。
var (
	ErrReplicationUnmet = errors.New("store: replication factor cannot be satisfied")
	ErrNoQuorum         = errors.New("store: not enough healthy replicas to reconstruct")
	ErrCorruptChunk     = errors.New("store: chunk failed integrity verification")
	ErrContentMismatch  = errors.New("store: reassembled content does not match content id")
	ErrManifestMissing  = errors.New("store: manifest not found on any node")
)

// Options 是存储集群的配置。
type Options struct {
	Replication int // 期望副本数
	ChunkSize   int // 分片大小
}

// Store 在一组节点上协调内容的写入与取回。
type Store struct {
	nodes []node.Node
	opts  Options
}

// ReplicaInfo 描述一个分片实际落在哪些节点上。
type ReplicaInfo struct {
	ChunkID contentid.ID
	Nodes   []string
}

// PutResult 是一次写入的结果。
type PutResult struct {
	ContentID  contentid.ID
	ManifestID contentid.ID
	Replicas   []ReplicaInfo
}

// FailureReason 在副本要求无法满足时携带可区分原因（骨架中暂不使用）。
type FailureReason struct {
	ContentID contentid.ID
	Wanted    int
	Got       int
	Failed    map[string]error
}

func New(nodes []node.Node, opts Options) *Store {
	return &Store{nodes: nodes, opts: opts}
}

func (s *Store) Put(ctx context.Context, data []byte) (*PutResult, error) {
	return nil, nil
}

func (s *Store) Get(ctx context.Context, contentID contentid.ID) ([]byte, error) {
	return nil, nil
}

func (s *Store) Replicas(ctx context.Context, contentID contentid.ID) ([]ReplicaInfo, error) {
	return nil, nil
}

