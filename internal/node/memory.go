package node

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// MemoryNode 是存储节点实现：内存为工作副本，若指定目录则以磁盘为准持久化。
type MemoryNode struct {
	mu      sync.RWMutex
	id      string
	dir     string // 为空表示纯内存节点
	healthy bool
	data    map[string][]byte
}

// NewMemoryNode 创建一个纯内存节点。
func NewMemoryNode(id string) *MemoryNode {
	return &MemoryNode{id: id, healthy: true, data: make(map[string][]byte)}
}

// NewDiskBackedNode 创建以 dir 为持久化目录的节点：
// 启动时从目录加载既有对象，之后每次写入都落盘（崩溃后可恢复）。
func NewDiskBackedNode(id, dir string) (*MemoryNode, error) {
	if strings.TrimSpace(id) == "" {
		return nil, errors.New("node: empty node id")
	}
	n := &MemoryNode{id: id, dir: dir, healthy: true, data: make(map[string][]byte)}
	if dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("node: mkdir %s: %w", dir, err)
		}
		if err := n.load(); err != nil {
			return nil, err
		}
	}
	return n, nil
}

func (n *MemoryNode) ID() string { return n.id }

func (n *MemoryNode) Put(key string, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.healthy {
		return fmt.Errorf("node: node %s is unavailable", n.id)
	}
	buf := make([]byte, len(data))
	copy(buf, data)
	if n.dir != "" {
		if err := n.writeFile(key, buf); err != nil {
			return err
		}
	}
	n.data[key] = buf
	return nil
}

func (n *MemoryNode) Get(key string) ([]byte, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.healthy {
		return nil, fmt.Errorf("node: node %s is unavailable", n.id)
	}
	b, ok := n.data[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s on node %s", ErrNotFound, key, n.id)
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

func (n *MemoryNode) Has(key string) (bool, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.healthy {
		return false, fmt.Errorf("node: node %s is unavailable", n.id)
	}
	_, ok := n.data[key]
	return ok, nil
}

func (n *MemoryNode) Delete(key string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.healthy {
		return fmt.Errorf("node: node %s is unavailable", n.id)
	}
	delete(n.data, key)
	if n.dir != "" {
		if err := os.Remove(n.path(key)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("node: delete %s: %w", key, err)
		}
	}
	return nil
}

func (n *MemoryNode) Healthy() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.healthy
}

func (n *MemoryNode) SetHealthy(ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.healthy = ok
}

// Corrupt 用于故障注入：直接改写对象内容模拟静默损坏（位腐烂/篡改）。
// 若对象只在内存，改内存；磁盘文件也同步改写成坏数据，保持“磁盘为准”。
func (n *MemoryNode) Corrupt(key string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	b, ok := n.data[key]
	if !ok {
		return fmt.Errorf("%w: %s on node %s", ErrNotFound, key, n.id)
	}
	bad := make([]byte, len(b))
	copy(bad, b)
	// 翻转首字节；空对象则写入一个字节，确保内容标识必然改变。
	if len(bad) == 0 {
		bad = []byte{0x01}
	} else {
		bad[0] ^= 0xFF
	}
	if n.dir != "" {
		if err := os.WriteFile(n.path(key), bad, 0o644); err != nil {
			return fmt.Errorf("node: corrupt %s: %w", key, err)
		}
	}
	n.data[key] = bad
	return nil
}

// Keys 返回当前持有对象的键（诊断/测试用）。
func (n *MemoryNode) Keys() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	keys := make([]string, 0, len(n.data))
	for k := range n.data {
		keys = append(keys, k)
	}
	return keys
}

func (n *MemoryNode) path(key string) string {
	// 文件名用十六进制编码，保证任意键（含 "/" 等）可逆且无路径穿越风险。
	return filepath.Join(n.dir, hex.EncodeToString([]byte(key))+".obj")
}

// writeFile 原子写：先写临时文件再 rename，避免崩溃留下半截对象。
func (n *MemoryNode) writeFile(key string, data []byte) error {
	final := n.path(key)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("node: write %s: %w", key, err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("node: rename %s: %w", key, err)
	}
	return nil
}

func (n *MemoryNode) load() error {
	entries, err := os.ReadDir(n.dir)
	if err != nil {
		return fmt.Errorf("node: load dir %s: %w", n.dir, err)
	}
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".obj") {
			continue
		}
		name := strings.TrimSuffix(ent.Name(), ".obj")
		raw, err := hex.DecodeString(name)
		if err != nil {
			return fmt.Errorf("node: decode object file %s: %w", ent.Name(), err)
		}
		b, err := os.ReadFile(filepath.Join(n.dir, ent.Name()))
		if err != nil {
			return fmt.Errorf("node: load %s: %w", ent.Name(), err)
		}
		n.data[string(raw)] = b
	}
	return nil
}
