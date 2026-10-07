// Package store 在多个本地存储节点之上实现内容寻址、多副本写入与校验取回。
package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/feng111627/content-addressed-storage-go/internal/cas"
	"github.com/feng111627/content-addressed-storage-go/internal/node"
)

// 对象键的统一命名空间。
const (
	keyChunkPrefix    = "cas/chunk/"
	keyManifestPrefix = "cas/manifest/"
	keyIndexPrefix    = "cas/index/"
)

// Config 控制写入时的副本与重试策略。
type Config struct {
	Replication int // 每个分片要求的成功副本数（最小为 1）
	ChunkSize   int // 分片大小（字节）
	MaxRetries  int // 首选节点写失败后，最多再尝试多少个“其他节点”
}

// DefaultConfig 返回默认配置：3 副本、16KiB 分片、2 个额外节点重试。
func DefaultConfig() Config {
	return Config{Replication: 3, ChunkSize: cas.ChunkSize, MaxRetries: 2}
}

// Store 协调多个节点，对外提供内容的存与取。
type Store struct {
	nodes []node.Node
	cfg   Config

	mu     sync.Mutex
	flight map[string]*writeFlight // contentID -> 进行中的写入（并发去重）
	logf   func(format string, args ...any)
}

// New 创建一个 Store；nodes 为候选节点集合。
func New(nodes []node.Node, cfg Config) (*Store, error) {
	if len(nodes) == 0 {
		return nil, errors.New("store: no nodes provided")
	}
	if cfg.Replication < 1 {
		return nil, fmt.Errorf("%w: replication=%d", cas.ErrInvalidSize, cfg.Replication)
	}
	if cfg.ChunkSize <= 0 {
		return nil, fmt.Errorf("%w: chunk size=%d", cas.ErrInvalidSize, cfg.ChunkSize)
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	cp := make([]node.Node, len(nodes))
	copy(cp, nodes)
	return &Store{
		nodes:  cp,
		cfg:    cfg,
		flight: make(map[string]*writeFlight),
		logf:   log.Printf,
	}, nil
}

// SetLogger 替换内部日志输出（测试可注入静默或带前缀的 logger）。
func (s *Store) SetLogger(f func(format string, args ...any)) {
	if f != nil {
		s.logf = f
	}
}

// PutResult 是一次成功写入的结果。
type PutResult struct {
	ContentID  string
	ManifestID string
	Chunks     []cas.Chunk
	Replicas   map[string][]string // chunkID -> 实际持有副本的节点 ID
}

// PutContent 将内容切分、按副本写入，并持久化分片清单。
// 并发写入同一内容会被去重：同一时刻只有一个实际写入流程，其余调用等待并
// 复用其结果，因此不会产生重复分片或损坏的副本记录。
func (s *Store) PutContent(ctx context.Context, data []byte) (*PutResult, error) {
	// 先做切分与清单计算（纯内存、确定性），以内容标识作为去重键。
	chunks, err := cas.Split(data, s.cfg.ChunkSize)
	if err != nil {
		return nil, err
	}
	manifest, err := cas.NewManifest(chunks, s.cfg.ChunkSize)
	if err != nil {
		return nil, err
	}
	contentID := manifest.ContentID

	flight, leader := s.joinFlight(contentID)
	if !leader {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-flight.done:
			if flight.err != nil {
				return nil, flight.err
			}
			res := *flight.res
			return &res, nil
		}
	}

	res, perr := s.putUnderFlight(ctx, chunks, manifest)
	s.finishFlight(contentID, flight, res, perr)
	if perr != nil {
		return nil, perr
	}
	out := *res
	return &out, nil
}

func (s *Store) putUnderFlight(ctx context.Context, chunks []cas.Chunk, manifest *cas.Manifest) (*PutResult, error) {
	contentID := manifest.ContentID

	manifestBytes, err := cas.MarshalManifest(manifest)
	if err != nil {
		return nil, err
	}
	manifestID, err := cas.ManifestID(manifest)
	if err != nil {
		return nil, err
	}

	// 快速路径：若已有足够副本的同名清单存在，内容寻址保证数据一致，直接复用。
	if s.manifestIntact(manifestID, manifestBytes) {
		s.logf("[cas] content %s 已存在（清单 %s），写入幂等跳过", contentID, manifestID)
		replicas := s.replicaMap(chunks)
		return &PutResult{
			ContentID: contentID, ManifestID: manifestID,
			Chunks: chunks, Replicas: replicas,
		}, nil
	}

	// 1) 逐分片写入，满足副本数才算成功。
	replicas := make(map[string][]string, len(chunks))
	for _, c := range chunks {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		holders, err := s.writeChunk(c)
		if err != nil {
			return nil, err
		}
		replicas[c.ID] = holders
	}

	// 2) 持久化分片清单本身（按副本数）。
	if _, err := s.writeObject(
		keyManifestPrefix+manifestID, manifestBytes, s.cfg.Replication, "manifest"); err != nil {
		return nil, err
	}

	// 3) 持久化 contentID -> manifestID 索引：尽量扩散到所有健康节点，
	//    保证只要还有任意一个节点存活，就能定位到内容。
	idxValue := []byte(manifestID)
	healthy := s.healthyNodes()
	h, f := s.writeObjectTo(keyIndexPrefix+contentID, idxValue, healthy, len(healthy), "index")
	if len(h) == 0 {
		return nil, &cas.ReplicationError{
			ChunkID: keyIndexPrefix + contentID, Required: 1, Achieved: 0, Failed: f,
		}
	}

	s.logf("[cas] 写入完成 content=%s chunks=%d replicas=%d manifest=%s",
		contentID, len(chunks), s.cfg.Replication, manifestID)
	return &PutResult{
		ContentID:  contentID,
		ManifestID: manifestID,
		Chunks:     chunks,
		Replicas:   replicas,
	}, nil
}

// writeChunk 按内容寻址把单个分片放到 r 个不同节点，并做读回校验。
func (s *Store) writeChunk(c cas.Chunk) ([]string, error) {
	ordered := s.orderNodesForKey(c.ID)
	holders, failures := s.writeObjectTo(keyChunkPrefix+c.ID, c.Data, ordered, s.cfg.Replication, "chunk")
	if len(holders) < s.cfg.Replication {
		return nil, &cas.ReplicationError{
			ChunkID:  c.ID,
			Required: s.cfg.Replication,
			Achieved: len(holders),
			Failed:   failures,
		}
	}
	return holders, nil
}

// writeObject 在前 n 个节点（按确定性顺序）上写同一对象。
func (s *Store) writeObject(key string, data []byte, n int, op string) ([]string, error) {
	// 清单/索引同样按其键稳定选节点，保证不同对象分散到不同节点。
	ordered := s.orderNodesForKey(key)
	holders, failures := s.writeObjectTo(key, data, ordered, n, op)
	if len(holders) < n {
		id := key
		return nil, &cas.ReplicationError{
			ChunkID: id, Required: n, Achieved: len(holders), Failed: failures,
		}
	}
	return holders, nil
}

// writeObjectTo 在给定候选节点上写入，直到凑够 want 个成功副本或耗尽候选。
// 不健康节点直接跳过（不消耗重试预算）；对健康节点的写失败，在已尝试的
// “首选 want 个可写位置”之外，最多再容忍 MaxRetries 个失败。
// 写后立即读回并校验，任何失败/不一致都不计入成功。
func (s *Store) writeObjectTo(key string, data []byte, candidates []node.Node, want int, op string) ([]string, []cas.NodeFailure) {
	var holders []string
	var failures []cas.NodeFailure
	attempted := 0 // 已尝试的可写位置数（健康节点，无论成败）
	for _, nd := range candidates {
		if len(holders) >= want {
			break
		}
		if !nd.Healthy() {
			failures = append(failures, cas.NodeFailure{NodeID: nd.ID(), Op: op,
				Err: errors.New("node unavailable")})
			continue
		}
		attempted++
		err := nd.Put(key, data)
		if err == nil {
			// 读回校验：防止写入半截或写入路径上的静默损坏被当作成功。
			got, gerr := nd.Get(key)
			if gerr == nil && bytes.Equal(got, data) {
				holders = append(holders, nd.ID())
				continue
			}
			if gerr != nil {
				err = fmt.Errorf("read-back failed: %w", gerr)
			} else {
				err = errors.New("read-back data mismatch")
			}
		}
		failures = append(failures, cas.NodeFailure{NodeID: nd.ID(), Op: op, Err: err})
		// 首选 want 个位置之外的健康节点失败，消耗重试预算。
		if attempted > want && attempted-want > s.cfg.MaxRetries {
			break
		}
	}
	return holders, failures
}

// GetContent 依据内容标识取回并重组内容，逐分片与整体校验。
// 会自动识别并跳过坏副本；无法修复时返回可区分原因。
func (s *Store) GetContent(ctx context.Context, contentID string) ([]byte, error) {
	manifestID, err := s.lookupManifestID(contentID)
	if err != nil {
		return nil, err
	}
	return s.getByManifest(ctx, manifestID, contentID)
}

// GetContentByManifest 依据清单标识取回内容。
func (s *Store) GetContentByManifest(ctx context.Context, manifestID string) ([]byte, error) {
	return s.getByManifest(ctx, manifestID, "")
}

func (s *Store) getByManifest(ctx context.Context, manifestID, wantContentID string) ([]byte, error) {
	manifest, err := s.fetchManifest(manifestID)
	if err != nil {
		return nil, err
	}
	if wantContentID != "" && manifest.ContentID != wantContentID {
		return nil, fmt.Errorf("%w: manifest points to %s but index says %s",
			cas.ErrManifestMismatch, manifest.ContentID, wantContentID)
	}

	recovered := make([]cas.Chunk, 0, len(manifest.Chunks))
	for _, ref := range manifest.Chunks {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		c, corruptions, err := s.fetchIntactChunk(ref)
		if err != nil {
			return nil, err
		}
		if len(corruptions) > 0 {
			for _, ce := range corruptions {
				s.logf("[cas] 发现坏副本已跳过: chunk=%s node=%s (%v)",
					ce.ChunkID, ce.NodeID, ce.Err)
			}
		}
		s.logf("[cas] 取回分片 seq=%d id=%s 来自节点=%s（探测坏副本 %d 个）",
			ref.Seq, c.ID[:19], c.nodeID(), len(corruptions))
		recovered = append(recovered, c.Chunk)
	}

	joined, err := cas.Join(recovered)
	if err != nil {
		// Join 内部再次校验每个分片，任何损坏都不会进入结果。
		return nil, err
	}
	if int64(len(joined)) != manifest.Length {
		return nil, fmt.Errorf("%w: length=%d manifest=%d",
			cas.ErrContentMismatch, len(joined), manifest.Length)
	}
	if err := cas.VerifyBytes(joined, manifest.ContentID); err != nil {
		return nil, err
	}
	s.logf("[cas] 内容校验通过 content=%s bytes=%d", manifest.ContentID, len(joined))
	return joined, nil
}

// fetchedChunk 包装取回结果与来源节点（便于日志）。
type fetchedChunk struct {
	cas.Chunk
	from string
}

func (f fetchedChunk) nodeID() string { return f.from }

func (s *Store) fetchIntactChunk(ref cas.ChunkRef) (fetchedChunk, []cas.CorruptionError, error) {
	var corruptions []cas.CorruptionError
	var notFound int
	for _, nd := range s.orderNodesForKey(ref.ID) {
		if !nd.Healthy() {
			continue
		}
		data, err := nd.Get(keyChunkPrefix + ref.ID)
		if err != nil {
			if errors.Is(err, node.ErrNotFound) {
				notFound++
			}
			continue
		}
		if len(data) != ref.Length {
			corruptions = append(corruptions, cas.CorruptionError{
				NodeID: nd.ID(), ChunkID: ref.ID,
				Err: fmt.Errorf("%w: length=%d want=%d", cas.ErrChunkMismatch, len(data), ref.Length),
			})
			continue
		}
		c := cas.Chunk{ID: ref.ID, Seq: ref.Seq, Data: data}
		if err := cas.VerifyChunk(c); err != nil {
			corruptions = append(corruptions, cas.CorruptionError{
				NodeID: nd.ID(), ChunkID: ref.ID, Err: err,
			})
			continue
		}
		return fetchedChunk{Chunk: c, from: nd.ID()}, corruptions, nil
	}
	if len(corruptions) > 0 || notFound > 0 {
		return fetchedChunk{}, corruptions,
			fmt.Errorf("%w: seq=%d id=%s (坏副本=%d, 缺失节点=%d)",
				cas.ErrMissingChunk, ref.Seq, ref.ID, len(corruptions), notFound)
	}
	return fetchedChunk{}, nil,
		fmt.Errorf("%w: seq=%d id=%s (无健康节点持有该分片)",
			cas.ErrMissingChunk, ref.Seq, ref.ID)
}

func (s *Store) fetchManifest(manifestID string) (*cas.Manifest, error) {
	key := keyManifestPrefix + manifestID
	var lastErr error
	for _, nd := range s.orderNodesForKey(manifestID) {
		if !nd.Healthy() {
			continue
		}
		b, err := nd.Get(key)
		if err != nil {
			lastErr = err
			continue
		}
		// 清单自检：重算清单标识必须与寻址键一致，且内部结构合法。
		if got := cas.ContentID(b); got != manifestID {
			lastErr = fmt.Errorf("%w: on node %s: id %s != %s",
				cas.ErrManifestMismatch, nd.ID(), got, manifestID)
			s.logf("[cas] 清单副本损坏已跳过: node=%s", nd.ID())
			continue
		}
		m, err := cas.UnmarshalManifest(b)
		if err != nil {
			lastErr = err
			continue
		}
		if err := cas.VerifyManifest(m); err != nil {
			lastErr = err
			continue
		}
		return m, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no healthy node holds the manifest")
	}
	return nil, fmt.Errorf("%w: %s: %v", cas.ErrManifestNotFound, manifestID, lastErr)
}

func (s *Store) lookupManifestID(contentID string) (string, error) {
	key := keyIndexPrefix + contentID
	var lastErr error
	for _, nd := range s.nodes {
		if !nd.Healthy() {
			continue
		}
		v, err := nd.Get(key)
		if err == nil && len(v) > 0 {
			return string(v), nil
		}
		if err != nil && !errors.Is(err, node.ErrNotFound) {
			lastErr = err
		}
	}
	if lastErr != nil {
		return "", fmt.Errorf("%w: %s: %v", cas.ErrContentNotFound, contentID, lastErr)
	}
	return "", fmt.Errorf("%w: %s", cas.ErrContentNotFound, contentID)
}

// Stat 返回某内容当前各分片的“完好”副本所在节点（诊断用，逐副本校验）。
func (s *Store) Stat(contentID string) (map[string][]string, error) {
	manifestID, err := s.lookupManifestID(contentID)
	if err != nil {
		return nil, err
	}
	manifest, err := s.fetchManifest(manifestID)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]string, len(manifest.Chunks))
	for _, ref := range manifest.Chunks {
		var good []string
		for _, nd := range s.nodes {
			if !nd.Healthy() {
				continue
			}
			data, err := nd.Get(keyChunkPrefix + ref.ID)
			if err != nil {
				continue
			}
			if len(data) != ref.Length {
				continue
			}
			if cas.VerifyChunk(cas.Chunk{ID: ref.ID, Seq: ref.Seq, Data: data}) != nil {
				continue
			}
			good = append(good, nd.ID())
		}
		out[ref.ID] = good
	}
	return out, nil
}

// manifestIntact 检查清单是否已有至少一份完好副本（幂等快速路径）。
func (s *Store) manifestIntact(manifestID string, want []byte) bool {
	for _, nd := range s.nodes {
		if !nd.Healthy() {
			continue
		}
		b, err := nd.Get(keyManifestPrefix + manifestID)
		if err == nil && bytes.Equal(b, want) {
			return true
		}
	}
	return false
}

func (s *Store) replicaMap(chunks []cas.Chunk) map[string][]string {
	out := make(map[string][]string, len(chunks))
	for _, c := range chunks {
		var holders []string
		for _, nd := range s.nodes {
			if !nd.Healthy() {
				continue
			}
			if b, err := nd.Get(keyChunkPrefix + c.ID); err == nil && bytes.Equal(b, c.Data) {
				holders = append(holders, nd.ID())
			}
		}
		out[c.ID] = holders
	}
	return out
}

func (s *Store) healthyNodes() []node.Node {
	var out []node.Node
	for _, nd := range s.nodes {
		if nd.Healthy() {
			out = append(out, nd)
		}
	}
	return out
}

// orderNodesForKey 返回按“键+节点ID”哈希排序的节点序列，
// 使不同分片的副本稳定地分散到不同节点，同时同一次重试顺序确定。
func (s *Store) orderNodesForKey(key string) []node.Node {
	type scored struct {
		n  node.Node
		ks [32]byte
	}
	all := make([]scored, len(s.nodes))
	for i, nd := range s.nodes {
		h := sha256.New()
		h.Write([]byte(key))
		h.Write([]byte{0})
		h.Write([]byte(nd.ID()))
		all[i] = scored{n: nd, ks: [32]byte(h.Sum(nil))}
	}
	sort.SliceStable(all, func(i, j int) bool {
		return bytes.Compare(all[i].ks[:], all[j].ks[:]) < 0
	})
	out := make([]node.Node, len(all))
	for i, sc := range all {
		out[i] = sc.n
	}
	return out
}

// ---- 并发写入去重（单飞） ----

type writeFlight struct {
	done chan struct{}
	res  *PutResult
	err  error
}

func (s *Store) joinFlight(contentID string) (*writeFlight, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f, ok := s.flight[contentID]; ok {
		return f, false
	}
	f := &writeFlight{done: make(chan struct{})}
	s.flight[contentID] = f
	return f, true
}

func (s *Store) finishFlight(contentID string, f *writeFlight, res *PutResult, err error) {
	s.mu.Lock()
	f.res, f.err = res, err
	close(f.done)
	delete(s.flight, contentID)
	s.mu.Unlock()
}
