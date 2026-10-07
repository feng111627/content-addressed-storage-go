package cas

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Manifest 是某个内容的分片清单，本身也可寻址。
type Manifest struct {
	ContentID string     `json:"contentID"` // 完整内容的标识
	ChunkSize int        `json:"chunkSize"` // 切分所用的固定分片大小
	Length    int64      `json:"length"`    // 原始内容字节数
	Chunks    []ChunkRef `json:"chunks"`    // 按序号排列的分片引用
}

// ChunkRef 引用一个分片（清单中不内联数据，仅存标识、长度与序号）。
type ChunkRef struct {
	ID     string `json:"id"`
	Length int    `json:"length"`
	Seq    int    `json:"seq"`
}

// NewManifest 依据分片列表构建清单，并计算整体内容标识。
// chunks 必须按序号排列；函数内部会复制，不持有调用方切片。
func NewManifest(chunks []Chunk, chunkSize int) (*Manifest, error) {
	if chunkSize <= 0 {
		return nil, fmt.Errorf("%w: size=%d", ErrInvalidSize, chunkSize)
	}
	refs := make([]ChunkRef, len(chunks))
	var length int64
	for i, c := range chunks {
		if c.Seq != i {
			return nil, fmt.Errorf("cas: manifest chunk not ordered: index=%d seq=%d", i, c.Seq)
		}
		if err := VerifyChunk(c); err != nil {
			return nil, err
		}
		refs[i] = ChunkRef{ID: c.ID, Length: len(c.Data), Seq: c.Seq}
		length += int64(len(c.Data))
	}

	joined, err := Join(chunks)
	if err != nil {
		return nil, err
	}
	m := &Manifest{
		ContentID: ContentID(joined),
		ChunkSize: chunkSize,
		Length:    length,
		Chunks:    refs,
	}
	return m, nil
}

// MarshalManifest 将清单序列化为可持久化的确定字节表示。
// 使用固定字段顺序的紧凑 JSON（无多余空白、无 map），保证跨进程一致。
func MarshalManifest(m *Manifest) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(m); err != nil {
		return nil, fmt.Errorf("cas: encode manifest: %w", err)
	}
	// json.Encoder.Encode 会追加一个换行，统一裁掉以保持字节确定。
	out := buf.Bytes()
	return bytes.TrimRight(out, "\n"), nil
}

// UnmarshalManifest 从字节表示恢复清单。
func UnmarshalManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrManifestMismatch, err)
	}
	return &m, nil
}

// ManifestID 计算清单自身的稳定标识：对规范化字节做内容定址。
func ManifestID(m *Manifest) (string, error) {
	b, err := MarshalManifest(m)
	if err != nil {
		return "", err
	}
	return ContentID(b), nil
}

// VerifyManifest 校验清单内部一致性：
//  1. 分片序号连续、长度为正（除整体为空时无分片）；
//  2. 除最后一个分片外，其余分片长度必须等于 ChunkSize；
//  3. 分片标识格式合法；
//  4. 长度字段与各分片长度之和一致；
//  5. 内容标识与按引用重算结果一致（需要外部提供数据时在取回阶段做整体校验）。
func VerifyManifest(m *Manifest) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrManifestMismatch)
	}
	if len(m.ContentID) != IDLen || m.ContentID[:len(IDPrefix)] != IDPrefix {
		return fmt.Errorf("%w: bad content id %q", ErrManifestMismatch, m.ContentID)
	}
	if err := checkSize(m.ChunkSize); err != nil {
		return fmt.Errorf("%w: %v", ErrManifestMismatch, err)
	}
	var sum int64
	for i, ref := range m.Chunks {
		if ref.Seq != i {
			return fmt.Errorf("%w: chunk seq gap at %d (seq=%d)", ErrManifestMismatch, i, ref.Seq)
		}
		if len(ref.ID) != IDLen || ref.ID[:len(IDPrefix)] != IDPrefix {
			return fmt.Errorf("%w: bad chunk id %q", ErrManifestMismatch, ref.ID)
		}
		if ref.Length <= 0 {
			return fmt.Errorf("%w: non-positive chunk length at seq=%d", ErrManifestMismatch, i)
		}
		// 非最后分片必须恰好为 ChunkSize；最后一片不得大于 ChunkSize。
		if i < len(m.Chunks)-1 {
			if ref.Length != m.ChunkSize {
				return fmt.Errorf("%w: chunk seq=%d length=%d want %d",
					ErrManifestMismatch, i, ref.Length, m.ChunkSize)
			}
		} else if ref.Length > m.ChunkSize {
			return fmt.Errorf("%w: last chunk oversize: %d > %d",
				ErrManifestMismatch, ref.Length, m.ChunkSize)
		}
		sum += int64(ref.Length)
	}
	if sum != m.Length {
		return fmt.Errorf("%w: declared length=%d but chunks sum=%d",
			ErrManifestMismatch, m.Length, sum)
	}
	return nil
}

// ChunkIDs 返回清单中的全部分片标识（按序号）。
func (m *Manifest) ChunkIDs() []string {
	ids := make([]string, len(m.Chunks))
	for i, ref := range m.Chunks {
		ids[i] = ref.ID
	}
	return ids
}

// FindRef 在清单中查找指定分片标识，返回引用与是否找到。
func (m *Manifest) FindRef(chunkID string) (ChunkRef, bool) {
	for _, ref := range m.Chunks {
		if ref.ID == chunkID {
			return ref, true
		}
	}
	return ChunkRef{}, false
}
