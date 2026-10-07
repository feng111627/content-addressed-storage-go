// Package cas 实现内容寻址存储的核心原语：分片、内容标识与分片清单。
package cas

import (
	"errors"
	"strings"
)

// 可区分的错误原因：调用方可用 errors.Is 精确判断失败类别。
var (
	// ErrInvalidSize 表示分片大小等参数非法。
	ErrInvalidSize = errors.New("cas: invalid chunk size")
	// ErrChunkMismatch 表示分片数据与其内容标识不一致（损坏或被篡改）。
	ErrChunkMismatch = errors.New("cas: chunk checksum mismatch")
	// ErrManifestMismatch 表示分片清单自身的校验失败。
	ErrManifestMismatch = errors.New("cas: manifest checksum mismatch")
	// ErrContentMismatch 表示重组后的整体内容与清单记录的内容标识不一致。
	ErrContentMismatch = errors.New("cas: reassembled content checksum mismatch")
	// ErrMissingChunk 表示在所有可用节点上都找不到某个分片的完好副本。
	ErrMissingChunk = errors.New("cas: no intact chunk replica found on any node")
	// ErrInsufficientNodes 表示健康节点数无法满足副本数要求。
	ErrInsufficientNodes = errors.New("cas: insufficient healthy nodes for replication")
	// ErrManifestNotFound 表示指定的清单标识不存在。
	ErrManifestNotFound = errors.New("cas: manifest not found")
	// ErrContentNotFound 表示指定的内容标识在本地索引中不存在。
	ErrContentNotFound = errors.New("cas: content not found")
)

// CorruptionError 描述一个在具体节点上发现的损坏分片，用于诊断与日志。
type CorruptionError struct {
	NodeID  string
	ChunkID string
	Err     error
}

func (e *CorruptionError) Error() string {
	return "cas: corrupted chunk " + e.ChunkID + " on node " + e.NodeID + ": " + e.Err.Error()
}

func (e *CorruptionError) Unwrap() error { return e.Err }

// ReplicationError 描述无法满足副本要求时的详细原因。
type ReplicationError struct {
	ChunkID  string
	Required int
	Achieved int
	Failed   []NodeFailure
}

// NodeFailure 记录一次针对节点的写/读失败。
type NodeFailure struct {
	NodeID string
	Op     string
	Err    error
}

func (e *ReplicationError) Error() string {
	var sb strings.Builder
	sb.WriteString("cas: replication factor not met for chunk ")
	sb.WriteString(e.ChunkID)
	sb.WriteString(": required=")
	sb.WriteString(itoa(e.Required))
	sb.WriteString(", achieved=")
	sb.WriteString(itoa(e.Achieved))
	for _, f := range e.Failed {
		sb.WriteString("; ")
		sb.WriteString(f.Op)
		sb.WriteString(" on node ")
		sb.WriteString(f.NodeID)
		sb.WriteString(": ")
		sb.WriteString(f.Err.Error())
	}
	return sb.String()
}

// Unwrap 使 errors.Is(err, ErrInsufficientNodes) 成立，
// 同时保留结构化细节（需要的副本数、已达成数、各节点失败原因）。
func (e *ReplicationError) Unwrap() error { return ErrInsufficientNodes }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
