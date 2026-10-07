// Package node 用本地磁盘目录与内存模拟去中心化存储中的单个存储节点。
//
// 每个节点：
//   - 以分片内容标识为键（文件名即十六进制 ID），天然去重：
//     同一内容的并发写入只会落同一份数据，不会产生重复分片；
//   - dir 非空时数据持久化到 dir 目录（tmp 文件 + rename 原子落盘），
//     节点重建时自动加载；dir 为空时仅使用内存；
//   - 通过 SetAvailable 模拟节点宕机（读写一律返回 ErrNodeDown，
//     绝不能被上层当作成功）；
//   - 通过 Corrupt 模拟磁盘静默损坏：保留文件但翻转一个字节，
//     供上层校验时识别坏副本。
package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/feng111627/content-addressed-storage-go/pkg/contentid"
)

// ErrNotFound 表示节点上不存在该分片。
var ErrNotFound = errors.New("chunk not found on node")

// ErrNodeDown 表示节点当前不可用（宕机/被隔离）。
var ErrNodeDown = errors.New("node is unavailable")

// Node 是一个存储节点的抽象。
type Node interface {
	Name() string
	Put(ctx context.Context, id contentid.ID, data []byte) error
	Get(ctx context.Context, id contentid.ID) ([]byte, error)
	Has(ctx context.Context, id contentid.ID) (bool, error)
	// Repair 用经校验的正确数据覆盖该节点上同 ID 的坏副本（自愈）。
	Repair(ctx context.Context, id contentid.ID, good []byte) error
	Close() error
}

// LocalNode 是基于本地磁盘（必要时纯内存）的存储节点实现，支持故障注入。
type LocalNode struct {
	name string
	dir  string // 为空表示纯内存

	mu        sync.Mutex
	available bool
	blobs     map[contentid.ID][]byte
}

// NewLocalNode 创建一个以 dir 为持久化目录的节点；dir 为空时使用纯内存存储。
// 节点初始为可用状态；若 dir 中已有历史数据会被加载到内存。
func NewLocalNode(name, dir string) (*LocalNode, error) {
	n := &LocalNode{
		name:      name,
		dir:       dir,
		available: true,
		blobs:     make(map[contentid.ID][]byte),
	}
	if dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("node %s: mkdir: %w", name, err)
		}
		if err := n.load(); err != nil {
			return nil, err
		}
	}
	return n, nil
}

func (n *LocalNode) path(id contentid.ID) string {
	return filepath.Join(n.dir, string(id))
}

// load 把磁盘上历史分片载入内存。
func (n *LocalNode) load() error {
	entries, err := os.ReadDir(n.dir)
	if err != nil {
		return fmt.Errorf("node %s: read dir: %w", n.name, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		id := contentid.ID(e.Name())
		if !id.Valid() {
			continue // 忽略 tmp 等无关文件
		}
		raw, err := os.ReadFile(n.path(id))
		if err != nil {
			return fmt.Errorf("node %s: load %s: %w", n.name, id, err)
		}
		n.blobs[id] = raw
	}
	return nil
}

func (n *LocalNode) Name() string { return n.name }

// Put 幂等写入：相同 id 的数据完全一致时直接视为成功。
func (n *LocalNode) Put(_ context.Context, id contentid.ID, data []byte) error {
	if !id.Valid() {
		return fmt.Errorf("node %s: invalid chunk id", n.name)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.available {
		return fmt.Errorf("node %s: %w", n.name, ErrNodeDown)
	}
	// 内容寻址去重：已存在且字节一致，幂等成功；同 id 不同内容属于严重冲突，报错。
	if existing, ok := n.blobs[id]; ok {
		if bytes.Equal(existing, data) {
			return nil
		}
		return fmt.Errorf("node %s: chunk id %s already exists with different data", n.name, id)
	}
	cp := append([]byte(nil), data...)
	if n.dir != "" {
		if err := n.writeAtomic(id, cp); err != nil {
			return err
		}
	}
	n.blobs[id] = cp
	return nil
}

// writeAtomic 先写临时文件再 rename，避免并发/崩溃产生半截文件。
func (n *LocalNode) writeAtomic(id contentid.ID, data []byte) (err error) {
	final := n.path(id)
	tmp := final + ".tmp"
	if err = os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("node %s: write tmp: %w", n.name, err)
	}
	if err = os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("node %s: rename: %w", n.name, err)
	}
	return nil
}

func (n *LocalNode) Get(_ context.Context, id contentid.ID) ([]byte, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.available {
		return nil, fmt.Errorf("node %s: %w", n.name, ErrNodeDown)
	}
	data, ok := n.blobs[id]
	if !ok {
		return nil, fmt.Errorf("node %s, chunk %s: %w", n.name, id, ErrNotFound)
	}
	return append([]byte(nil), data...), nil // 返回副本，调用方修改不会污染节点
}

func (n *LocalNode) Has(_ context.Context, id contentid.ID) (bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.available {
		return false, fmt.Errorf("node %s: %w", n.name, ErrNodeDown)
	}
	_, ok := n.blobs[id]
	return ok, nil
}

// Repair 用正确数据覆盖坏副本。仅在调用方已通过哈希校验时使用。
func (n *LocalNode) Repair(_ context.Context, id contentid.ID, good []byte) error {
	if !id.Valid() {
		return fmt.Errorf("node %s: invalid chunk id", n.name)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.available {
		return fmt.Errorf("node %s: %w", n.name, ErrNodeDown)
	}
	cp := append([]byte(nil), good...)
	if n.dir != "" {
		if err := n.writeAtomic(id, cp); err != nil {
			return err
		}
	}
	n.blobs[id] = cp
	return nil
}

func (n *LocalNode) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.available = false
	return nil
}

// SetAvailable 模拟节点上下线。
func (n *LocalNode) SetAvailable(up bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.available = up
}

// Available 查询节点当前状态。
func (n *LocalNode) Available() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.available
}

// Corrupt 模拟节点上某分片被篡改/损坏：翻转第一个字节（空分片无法翻转）。
// 文件仍然存在，Get 仍能读出字节，但字节与其内容标识不再一致。
func (n *LocalNode) Corrupt(id contentid.ID) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	data, ok := n.blobs[id]
	if !ok {
		return fmt.Errorf("node %s, chunk %s: %w", n.name, id, ErrNotFound)
	}
	if len(data) == 0 {
		return fmt.Errorf("node %s: cannot corrupt empty chunk %s", n.name, id)
	}
	bad := append([]byte(nil), data...)
	bad[0] ^= 0xFF
	if n.dir != "" {
		if err := os.WriteFile(n.path(id), bad, 0o644); err != nil {
			return fmt.Errorf("node %s: corrupt on disk: %w", n.name, err)
		}
	}
	n.blobs[id] = bad
	return nil
}
