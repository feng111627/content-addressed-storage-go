package cas

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
)

// IDPrefix 是所有内容标识的统一前缀，格式为 sha256-<64个十六进制字符>。
const IDPrefix = "sha256-"

// IDLen 是完整内容标识的长度（前缀 7 + 十六进制摘要 64）。
const IDLen = 7 + 64

// Chunk 表示一个定长分片及其稳定的内容标识。
type Chunk struct {
	ID   string // 分片内容标识：sha256-<hex>
	Seq  int    // 分片在原始内容中的序号（从 0 开始）
	Data []byte // 分片数据
}

// ContentID 计算任意字节内容的稳定标识：sha256-<hex 小写>。
// 相同内容必然得到相同标识；任意一位变化都会以压倒性概率得到不同标识。
// 空内容的标识是对空字节序列做 SHA-256 的确定结果（见 EmptyID）。
func ContentID(data []byte) string {
	sum := sha256.Sum256(data)
	return IDPrefix + hex.EncodeToString(sum[:])
}

// EmptyID 是空内容的确定标识：sha256(e3b0c442...)。
const EmptyID = IDPrefix + "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// ChunkSize 默认分片大小（16 KiB）。
const ChunkSize = 16 * 1024

// MaxChunkSize 限制可配置分片大小的上限，防止误用导致内存爆炸。
const MaxChunkSize = 64 * 1024 * 1024

// Split 将内容按固定大小切分为带序号与标识的分片。
// 空内容返回空切片（长度 0）与 nil 错误，具备确定语义。
func Split(data []byte, size int) ([]Chunk, error) {
	if err := checkSize(size); err != nil {
		return nil, err
	}
	if len(data) == 0 {
		// 确定语义：空内容没有任何分片；其标识由 ContentID(nil) 单独表达。
		return []Chunk{}, nil
	}
	n := (len(data) + size - 1) / size
	chunks := make([]Chunk, 0, n)
	for seq, off := 0, 0; off < len(data); seq, off = seq+1, off+size {
		end := off + size
		if end > len(data) {
			end = len(data)
		}
		buf := make([]byte, end-off)
		copy(buf, data[off:end])
		chunks = append(chunks, Chunk{
			ID:   ContentID(buf),
			Seq:  seq,
			Data: buf,
		})
	}
	return chunks, nil
}

// SplitReader 以流式方式切分，避免超大内容全部驻留内存。
// 注意：当前返回值仍会物化全部分片以完成内容定址；调用方应按序消费，
// 超大内容可通过 PutContent 的流式路径处理，语义与 Split 完全一致。
func SplitReader(r io.Reader, size int) ([]Chunk, error) {
	if err := checkSize(size); err != nil {
		return nil, err
	}
	var chunks []Chunk
	buf := make([]byte, size)
	seq := 0
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			part := make([]byte, n)
			copy(part, buf[:n])
			chunks = append(chunks, Chunk{ID: ContentID(part), Seq: seq, Data: part})
			seq++
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("cas: read content: %w", err)
		}
	}
	if chunks == nil {
		chunks = []Chunk{}
	}
	return chunks, nil
}

// VerifyChunk 校验分片数据与其标识是否一致。
func VerifyChunk(c Chunk) error {
	if got := ContentID(c.Data); got != c.ID {
		return fmt.Errorf("%w: chunk seq=%d expected %s got %s",
			ErrChunkMismatch, c.Seq, c.ID, got)
	}
	return nil
}

// Join 按序号重组分片为完整内容。重组前会：
//  1. 检查序号连续无缺失/重复；
//  2. 逐分片校验内容标识，损坏分片绝不参与拼接。
func Join(chunks []Chunk) ([]byte, error) {
	sorted := make([]Chunk, len(chunks))
	copy(sorted, chunks)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Seq < sorted[j].Seq })

	var total int
	for i, c := range sorted {
		if c.Seq != i {
			return nil, fmt.Errorf("cas: chunk sequence gap: expected seq=%d got seq=%d", i, c.Seq)
		}
		if err := VerifyChunk(c); err != nil {
			return nil, err
		}
		total += len(c.Data)
	}
	out := make([]byte, 0, total)
	for _, c := range sorted {
		out = append(out, c.Data...)
	}
	return out, nil
}

// VerifyBytes 是便捷函数：校验数据是否等于给定内容标识。
func VerifyBytes(data []byte, wantID string) error {
	if got := ContentID(data); got != wantID {
		return fmt.Errorf("%w: expected %s got %s", ErrContentMismatch, wantID, got)
	}
	return nil
}

// EqualChunks 判断两组分片的数据内容是否完全一致（忽略顺序）。
func EqualChunks(a, b []Chunk) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string][]byte, len(a))
	for _, c := range a {
		seen[c.ID] = c.Data
	}
	for _, c := range b {
		d, ok := seen[c.ID]
		if !ok || !bytes.Equal(d, c.Data) {
			return false
		}
	}
	return true
}

func checkSize(size int) error {
	if size <= 0 || size > MaxChunkSize {
		return fmt.Errorf("%w: size=%d (allowed 1..%d)", ErrInvalidSize, size, MaxChunkSize)
	}
	return nil
}
