// Package chunk 负责把内容切分为固定大小的分片，并维护可寻址的分片清单。
//
// 切分语义：
//   - 分片大小由调用方给定，必须 > 0；
//   - 除最后一片外，每个分片恰好 chunkSize 字节；最后一片 1..chunkSize 字节；
//   - 空内容（len(data)==0，含 nil）确定地产生 0 个分片，
//     重组 0 个分片即得到空字节，与内容标识 SHA-256("") 对应；
//   - 分片按数据切片在内容中的偏移从 0 开始编号；
//   - 每个分片的标识 = SHA-256(分片字节)；整体内容标识 = SHA-256(全部字节)；
//   - 清单(Manifest)以确定性 JSON 序列化，其标识 = SHA-256(清单字节)，
//     因此清单本身也是内容寻址、可持久化、可校验的。
package chunk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/feng111627/content-addressed-storage-go/pkg/contentid"
)

// DefaultChunkSize 是默认的固定分片大小（4 KiB）。
const DefaultChunkSize = 4 * 1024

// ErrBadChunkSize 表示分片大小参数非法。
var ErrBadChunkSize = errors.New("chunk: chunk size must be positive")

// Chunk 描述一个分片：在内容中的序号、字节数据、由数据计算出的内容标识。
type Chunk struct {
	Index int          `json:"index"`
	ID    contentid.ID `json:"id"`
	Size  int          `json:"size"`
	Data  []byte       `json:"-"`
}

// ChunkRef 是清单中的分片引用（不含数据本体）。
type ChunkRef struct {
	Index int          `json:"index"`
	ID    contentid.ID `json:"id"`
	Size  int          `json:"size"`
}

// Manifest 是分片清单，清单本身也可寻址并被持久化。
type Manifest struct {
	// Version 用于将来格式演进；当前固定为 1。
	Version    int          `json:"version"`
	HashAlgo   string       `json:"hash_algo"`
	ContentID  contentid.ID `json:"content_id"`
	ChunkSize  int          `json:"chunk_size"`
	TotalSize  int64        `json:"total_size"`
	ChunkCount int          `json:"chunk_count"`
	Chunks     []ChunkRef   `json:"chunks"`
}

// manifestVersion 是当前清单格式版本。
const manifestVersion = 1

// Split 把内容按 chunkSize 切分为固定大小的分片，并返回整体内容标识。
func Split(data []byte, chunkSize int) ([]Chunk, contentid.ID, error) {
	if chunkSize <= 0 {
		return nil, "", ErrBadChunkSize
	}
	contentID := contentid.Compute(data)
	if len(data) == 0 {
		// 空内容：确定地产生 0 个分片。
		return []Chunk{}, contentID, nil
	}
	count := (len(data) + chunkSize - 1) / chunkSize
	chunks := make([]Chunk, 0, count)
	for off, idx := 0, 0; off < len(data); off, idx = off+chunkSize, idx+1 {
		end := off + chunkSize
		if end > len(data) {
			end = len(data)
		}
		part := data[off:end]
		chunks = append(chunks, Chunk{
			Index: idx,
			ID:    contentid.Compute(part),
			Size:  len(part),
			Data:  append([]byte(nil), part...), // 复制，避免与输入共享底层数组
		})
	}
	return chunks, contentID, nil
}

// BuildManifest 由分片结果构建清单，并计算清单自身的标识。
// 调用方通常传入 Split 得到的 contentID 与 chunks。
func BuildManifest(contentID contentid.ID, chunkSize int, totalSize int64, chunks []Chunk) (*Manifest, contentid.ID, error) {
	if chunkSize <= 0 {
		return nil, "", ErrBadChunkSize
	}
	m := &Manifest{
		Version:    manifestVersion,
		HashAlgo:   contentid.Algorithm,
		ContentID:  contentID,
		ChunkSize:  chunkSize,
		TotalSize:  totalSize,
		ChunkCount: len(chunks),
		Chunks:     make([]ChunkRef, len(chunks)),
	}
	for i, c := range chunks {
		if c.Index != i {
			return nil, "", fmt.Errorf("chunk: chunk index out of order at position %d (got %d)", i, c.Index)
		}
		if c.Size != len(c.Data) {
			return nil, "", fmt.Errorf("chunk: declared size %d != actual %d at index %d", c.Size, len(c.Data), i)
		}
		if got := contentid.Compute(c.Data); got != c.ID {
			return nil, "", fmt.Errorf("chunk: declared id %s != computed %s at index %d", c.ID, got, i)
		}
		m.Chunks[i] = ChunkRef{Index: c.Index, ID: c.ID, Size: c.Size}
	}
	raw, err := json.Marshal(m) // 结构体字段顺序固定 → 序列化结果确定
	if err != nil {
		return nil, "", fmt.Errorf("chunk: marshal manifest: %w", err)
	}
	return m, contentid.Compute(raw), nil
}

// Marshal 把清单序列化为确定的 JSON 字节，用于持久化。
func (m *Manifest) Marshal() ([]byte, error) {
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("chunk: marshal manifest: %w", err)
	}
	return raw, nil
}

// UnmarshalManifest 从持久化字节恢复清单。
func UnmarshalManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("chunk: unmarshal manifest: %w", err)
	}
	if m.Version != manifestVersion {
		return nil, fmt.Errorf("chunk: unsupported manifest version %d", m.Version)
	}
	if m.HashAlgo != contentid.Algorithm {
		return nil, fmt.Errorf("chunk: unsupported hash algorithm %q", m.HashAlgo)
	}
	if len(m.Chunks) != m.ChunkCount {
		return nil, fmt.Errorf("chunk: chunk_count %d != refs %d", m.ChunkCount, len(m.Chunks))
	}
	for i, ref := range m.Chunks {
		if ref.Index != i {
			return nil, fmt.Errorf("chunk: chunk ref index out of order at position %d (got %d)", i, ref.Index)
		}
		if !ref.ID.Valid() {
			return nil, fmt.Errorf("chunk: invalid chunk id at index %d", i)
		}
		if ref.Size < 0 {
			return nil, fmt.Errorf("chunk: negative chunk size at index %d", i)
		}
	}
	return &m, nil
}

// IDOfManifest 计算清单字节的内容标识。
func IDOfManifest(raw []byte) contentid.ID {
	return contentid.Compute(raw)
}

// VerifyChunk 校验单个分片：长度声明一致、字节与其内容标识一致。
func VerifyChunk(c Chunk) error {
	if c.Size != len(c.Data) {
		return fmt.Errorf("%w: index %d declared size %d != actual %d",
			ErrCorruptChunk, c.Index, c.Size, len(c.Data))
	}
	if got := contentid.Compute(c.Data); got != c.ID {
		return fmt.Errorf("%w: index %d declared id %s != computed %s",
			ErrCorruptChunk, c.Index, c.ID, got)
	}
	return nil
}

// ErrCorruptChunk 表示分片字节与其标识不一致（损坏或篡改）。
var ErrCorruptChunk = errors.New("chunk: corrupt chunk")

// VerifyData 校验重组后的整体数据是否与内容标识一致。
func VerifyData(_ context.Context, contentID contentid.ID, data []byte) error {
	if got := contentid.Compute(data); got != contentID {
		return fmt.Errorf("chunk: %w content id mismatch: declared %s, computed %s",
			ErrContentMismatch, contentID, got)
	}
	return nil
}

// ErrContentMismatch 表示重组内容与整体内容标识不一致。
var ErrContentMismatch = errors.New("content verification failed")

// Join 按清单顺序把分片数据拼接重组为完整内容。
// 调用方应保证传入 chunks 已按 Index 排序且通过 VerifyChunk。
func Join(m *Manifest, chunks []Chunk) []byte {
	total := m.TotalSize
	out := make([]byte, 0, total)
	for i := range chunks {
		out = append(out, chunks[i].Data...)
	}
	return out
}
