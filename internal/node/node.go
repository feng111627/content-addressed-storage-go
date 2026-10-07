// Package node 用本地磁盘与内存模拟去中心化存储中的单个存储节点。
package node

import "errors"

// ErrNotFound 表示节点上不存在指定键的对象。
var ErrNotFound = errors.New("node: object not found")

// Node 是一个存储节点的最小抽象。
type Node interface {
	// ID 返回节点唯一标识。
	ID() string
	// Put 写入（覆盖）一个对象；节点不可用或写失败时返回非 nil 错误。
	Put(key string, data []byte) error
	// Get 读取一个对象；不存在返回 ErrNotFound。
	Get(key string) ([]byte, error)
	// Has 判断对象是否存在。
	Has(key string) (bool, error)
	// Delete 删除一个对象（不存在不视为错误）。
	Delete(key string) error
	// Healthy 返回节点当前是否可服务。
	Healthy() bool
	// SetHealthy 用于故障注入：模拟节点下线/恢复。
	SetHealthy(ok bool)
}
