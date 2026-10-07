package store_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/feng111627/content-addressed-storage-go/internal/cas"
	"github.com/feng111627/content-addressed-storage-go/internal/node"
	"github.com/feng111627/content-addressed-storage-go/internal/store"
)

func logCase(t *testing.T, business, caseName string) {
	t.Helper()
	t.Logf("【业务】%-20s 【用例】%s ... ✅ 通过", business, caseName)
}

func newNodes(n int) []node.Node {
	ns := make([]node.Node, n)
	for i := 0; i < n; i++ {
		ns[i] = node.NewMemoryNode(fmt.Sprintf("node-%d", i))
	}
	return ns
}

func newDiskNodes(t *testing.T, n int) ([]node.Node, string) {
	t.Helper()
	dir := t.TempDir()
	return openDiskNodes(t, dir, n), dir
}

// openDiskNodes 用既有目录重新构造节点，模拟进程重启后从磁盘恢复。
func openDiskNodes(t *testing.T, dir string, n int) []node.Node {
	t.Helper()
	ns := make([]node.Node, n)
	for i := 0; i < n; i++ {
		mn, err := node.NewDiskBackedNode(fmt.Sprintf("node-%d", i), dir+fmt.Sprintf("/n%d", i))
		if err != nil {
			t.Fatal(err)
		}
		ns[i] = mn
	}
	return ns
}

// findChunkKey 返回节点上某个分片 ID 对应的完整存储键。
func findChunkKey(t *testing.T, n node.Node, chunkID string) string {
	t.Helper()
	suffix := "chunk/" + chunkID
	for _, k := range n.(*node.MemoryNode).Keys() {
		if len(k) >= len(suffix) && k[len(k)-len(suffix):] == suffix {
			return k
		}
	}
	t.Fatalf("节点 %s 上找不到分片 %s", n.ID(), chunkID)
	return ""
}

func quietStore(t *testing.T, ns []node.Node, cfg store.Config) *store.Store {
	t.Helper()
	st, err := store.New(ns, cfg)
	if err != nil {
		t.Fatal(err)
	}
	st.SetLogger(func(string, ...any) {}) // 默认静默，保持测试输出聚焦业务日志
	return st
}

func TestPutAndReplicas(t *testing.T) {
	biz := "多副本写入"
	ctx := context.Background()

	t.Run("每个分片恰好落在配置的副本数个不同节点", func(t *testing.T) {
		ns := newNodes(5)
		st := quietStore(t, ns, store.Config{Replication: 3, ChunkSize: 64, MaxRetries: 2})
		// 使用伪随机数据，确保每个分片内容（因而标识）互不相同。
		res, err := st.PutContent(ctx, pseudoRandom(700))
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Replicas) != len(res.Chunks) {
			t.Fatal("副本映射缺少分片")
		}
		for cid, holders := range res.Replicas {
			if len(holders) != 3 {
				t.Fatalf("分片 %s 副本数=%d want=3", cid, len(holders))
			}
			seen := map[string]bool{}
			for _, h := range holders {
				if seen[h] {
					t.Fatalf("分片 %s 在节点 %s 上重复计数", cid, h)
				}
				seen[h] = true
			}
		}
		logCase(t, biz, "5节点3副本：每片恰好3个不同节点持有")
	})

	t.Run("取回内容与整体标识校验通过", func(t *testing.T) {
		ns := newNodes(4)
		st := quietStore(t, ns, store.DefaultConfig())
		data := []byte("end to end content addressed retrieval")
		res, err := st.PutContent(ctx, data)
		if err != nil {
			t.Fatal(err)
		}
		got, err := st.GetContent(ctx, res.ContentID)
		if err != nil {
			t.Fatalf("取回失败: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Fatal("取回内容与原文不符")
		}
		// 用不存在的标识取回必须报 ErrContentNotFound。
		if _, err := st.GetContent(ctx, cas.ContentID([]byte("other"))); !errors.Is(err, cas.ErrContentNotFound) {
			t.Fatalf("期望 ErrContentNotFound，得到 %v", err)
		}
		logCase(t, biz, "正常路径取回一致；未知标识返回 ErrContentNotFound")
	})

	t.Run("按清单标识也可取回", func(t *testing.T) {
		ns := newNodes(3)
		st := quietStore(t, ns, store.Config{Replication: 2, ChunkSize: 16})
		data := []byte("manifest addressed fetch")
		res, _ := st.PutContent(ctx, data)
		got, err := st.GetContentByManifest(ctx, res.ManifestID)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("按清单标识取回失败: %v", err)
		}
		logCase(t, biz, "GetContentByManifest 依据清单标识取回成功")
	})

	t.Run("重复写入幂等且不产生重复分片", func(t *testing.T) {
		ns := newNodes(3)
		st := quietStore(t, ns, store.Config{Replication: 2, ChunkSize: 32})
		data := []byte("idempotent same content put")
		r1, err := st.PutContent(ctx, data)
		if err != nil {
			t.Fatal(err)
		}
		r2, err := st.PutContent(ctx, append([]byte(nil), data...))
		if err != nil {
			t.Fatal(err)
		}
		if r1.ContentID != r2.ContentID || r1.ManifestID != r2.ManifestID {
			t.Fatal("同一内容两次写入的标识不一致")
		}
		// 再写一次：对象键全部按内容寻址，节点上的对象数量不应增长。
		before := countKeys(ns)
		if _, err := st.PutContent(ctx, append([]byte(nil), data...)); err != nil {
			t.Fatal(err)
		}
		if after := countKeys(ns); after != before {
			t.Fatalf("幂等写入改变了节点上的对象数量: before=%d after=%d", before, after)
		}
		logCase(t, biz, "同一内容重复写入结果一致且对象数不增长")
	})
}

func countKeys(ns []node.Node) int {
	c := 0
	for _, n := range ns {
		c += len(n.(*node.MemoryNode).Keys())
	}
	return c
}

// pseudoRandom 用 xorshift 生成确定性的伪随机字节，保证各分片内容互不相同。
func pseudoRandom(size int) []byte {
	data := make([]byte, size)
	var x uint32 = 2463534242
	for i := range data {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		data[i] = byte(x >> 24)
	}
	return data
}

func TestInsufficientReplication(t *testing.T) {
	biz := "故障导致副本不足"
	ctx := context.Background()

	t.Run("健康节点数少于副本数时写入失败且原因可区分", func(t *testing.T) {
		ns := newNodes(5)
		// 先让 3 个节点下线，只剩 2 个健康节点，无法满足 3 副本。
		for i := 0; i < 3; i++ {
			ns[i].SetHealthy(false)
		}
		st := quietStore(t, ns, store.Config{Replication: 3, ChunkSize: 64, MaxRetries: 2})
		_, err := st.PutContent(ctx, []byte("need three replicas"))
		if !errors.Is(err, cas.ErrInsufficientNodes) {
			t.Fatalf("期望 ErrInsufficientNodes，得到 %v", err)
		}
		var re *cas.ReplicationError
		if !errors.As(err, &re) {
			t.Fatalf("错误应可解析为 *ReplicationError，实际 %T", err)
		}
		if re.Required != 3 || re.Achieved != 2 {
			t.Fatalf("副本统计错误: required=%d achieved=%d", re.Required, re.Achieved)
		}
		if len(re.Failed) == 0 {
			t.Fatal("错误中应当包含具体节点失败原因")
		}
		logCase(t, biz, "5节点仅2健康→3副本写入失败，报 ErrInsufficientNodes 及 achieved=2")
	})

	t.Run("写入中途某节点失败会重试其他节点", func(t *testing.T) {
		ns := newNodes(4)
		st := quietStore(t, ns, store.Config{Replication: 3, ChunkSize: 64, MaxRetries: 2})
		// 先下线一个节点，仍有3个健康节点，应通过取舍重试成功。
		ns[0].SetHealthy(false)
		data := bytes.Repeat([]byte("retry-"), 50)
		res, err := st.PutContent(ctx, data)
		if err != nil {
			t.Fatalf("3个健康节点应能满足3副本: %v", err)
		}
		got, err := st.GetContent(ctx, res.ContentID)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("重试写入后取回失败: %v", err)
		}
		logCase(t, biz, "首选节点不可用时自动选用其他健康节点凑齐3副本")
	})

	t.Run("副本不足的失败不得留下半成品内容可被取回", func(t *testing.T) {
		ns := newNodes(3)
		st := quietStore(t, ns, store.Config{Replication: 3, ChunkSize: 64})
		// 下线2个：第一个分片都写不齐3副本，整体必须失败。
		ns[1].SetHealthy(false)
		ns[2].SetHealthy(false)
		_, err := st.PutContent(ctx, bytes.Repeat([]byte("partial"), 100))
		if !errors.Is(err, cas.ErrInsufficientNodes) {
			t.Fatalf("期望副本不足错误，得到 %v", err)
		}
		// 恢复节点后，按内容标识仍查不到“成功内容”（没有索引/清单）。
		ns[1].SetHealthy(true)
		ns[2].SetHealthy(true)
		cid := cas.ContentID(bytes.Repeat([]byte("partial"), 100))
		if _, err := st.GetContent(ctx, cid); !errors.Is(err, cas.ErrContentNotFound) {
			t.Fatalf("失败写入不应产生可取回内容，得到 %v", err)
		}
		logCase(t, biz, "副本不足时整体写入判失败，且不产生可取回的半成品内容")
	})
}

func TestCorruptionDetectionAndRecovery(t *testing.T) {
	biz := "坏分片检测与恢复"
	ctx := context.Background()

	t.Run("单个副本损坏被识别跳过并从好副本恢复", func(t *testing.T) {
		ns := newNodes(4)
		st := quietStore(t, ns, store.Config{Replication: 3, ChunkSize: 64, MaxRetries: 2})
		data := bytes.Repeat([]byte("corrupt-recover-"), 100)
		res, err := st.PutContent(ctx, data)
		if err != nil {
			t.Fatal(err)
		}
		// 破坏第一个分片的其中一个副本（保留另外2份完好）。
		cid := res.Chunks[0].ID
		holders := res.Replicas[cid]
		victim := nodeByID(ns, holders[0])
		if err := victim.Corrupt(findChunkKey(t, victim, cid)); err != nil {
			t.Fatal(err)
		}
		got, err := st.GetContent(ctx, res.ContentID)
		if err != nil {
			t.Fatalf("存在2份好副本时应能恢复: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Fatal("恢复内容与原文不一致")
		}
		logCase(t, biz, "3副本坏1份：跳过坏副本，从其余好副本恢复并整体校验通过")
	})

	t.Run("取回排序中第一个节点为坏副本时仍恢复", func(t *testing.T) {
		ns := newNodes(3)
		st := quietStore(t, ns, store.Config{Replication: 3, ChunkSize: 64})
		data := bytes.Repeat([]byte("bad-first-"), 80)
		res, _ := st.PutContent(ctx, data)
		cid := res.Chunks[0].ID
		// 把分片全部3份依次破坏2份：仍有1份好副本可恢复。
		holders := res.Replicas[cid]
		for i := 0; i < 2; i++ {
			vt := nodeByID(ns, holders[i])
			if err := vt.Corrupt(findChunkKey(t, vt, cid)); err != nil {
				t.Fatal(err)
			}
		}
		got, err := st.GetContent(ctx, res.ContentID)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("坏2好1时恢复失败: %v", err)
		}
		logCase(t, biz, "3副本坏2份：探测多个坏副本后仍找到唯一好副本恢复")
	})

	t.Run("所有副本都损坏时报 ErrMissingChunk 绝不返回坏内容", func(t *testing.T) {
		ns := newNodes(4)
		st := quietStore(t, ns, store.Config{Replication: 3, ChunkSize: 64})
		data := bytes.Repeat([]byte("all-bad-"), 80)
		res, _ := st.PutContent(ctx, data)
		cid := res.Chunks[0].ID
		for _, h := range res.Replicas[cid] {
			vt := nodeByID(ns, h)
			if err := vt.Corrupt(findChunkKey(t, vt, cid)); err != nil {
				t.Fatal(err)
			}
		}
		got, err := st.GetContent(ctx, res.ContentID)
		if !errors.Is(err, cas.ErrMissingChunk) {
			t.Fatalf("期望 ErrMissingChunk，得到 %v", err)
		}
		if got != nil {
			t.Fatal("无法修复时不得返回任何内容")
		}
		logCase(t, biz, "某分片所有副本损坏→ErrMissingChunk，且不返回错误内容")
	})

	t.Run("长度异常的副本被判定损坏", func(t *testing.T) {
		ns := newNodes(3)
		st := quietStore(t, ns, store.Config{Replication: 3, ChunkSize: 64})
		data := bytes.Repeat([]byte("len-"), 100)
		res, _ := st.PutContent(ctx, data)
		cid := res.Chunks[0].ID
		vt := nodeByID(ns, res.Replicas[cid][0])
		// 直接写入更长的伪造数据，且不更新标识。
		if err := vt.Put(findChunkKey(t, vt, cid), bytes.Repeat([]byte{9}, 64)); err != nil {
			t.Fatal(err)
		}
		got, err := st.GetContent(ctx, res.ContentID)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("长度异常副本应被跳过并恢复: %v", err)
		}
		logCase(t, biz, "长度/摘要不符的副本被判损坏并跳过")
	})
}

func TestNodeFailureDuringRetrieval(t *testing.T) {
	biz := "取回时节点故障"
	ctx := context.Background()

	t.Run("持有副本的部分节点下线仍可取回", func(t *testing.T) {
		ns := newNodes(5)
		st := quietStore(t, ns, store.Config{Replication: 3, ChunkSize: 64})
		data := bytes.Repeat([]byte("fault-tolerance-"), 100)
		res, _ := st.PutContent(ctx, data)
		// 让每个分片“可能”被下线2个持有者：直接整体下线2个节点。
		ns[0].SetHealthy(false)
		ns[1].SetHealthy(false)
		got, err := st.GetContent(ctx, res.ContentID)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("2节点故障下取回失败: %v", err)
		}
		logCase(t, biz, "3副本下2节点同时下线，仍从存活节点完整取回")
	})

	t.Run("仅存节点也下线则报缺失", func(t *testing.T) {
		ns := newNodes(3)
		st := quietStore(t, ns, store.Config{Replication: 3, ChunkSize: 64})
		data := bytes.Repeat([]byte("down-"), 50)
		res, _ := st.PutContent(ctx, data)
		for _, n := range ns {
			n.SetHealthy(false)
		}
		_, err := st.GetContent(ctx, res.ContentID)
		if !errors.Is(err, cas.ErrContentNotFound) && !errors.Is(err, cas.ErrManifestNotFound) {
			t.Fatalf("全节点下线应得到可区分的缺失错误: %v", err)
		}
		logCase(t, biz, "全部节点下线时返回可区分的 NotFound 错误")
	})
}

func TestConcurrentPut(t *testing.T) {
	biz := "并发写入去重"
	ctx := context.Background()

	t.Run("并发写同一内容只产生一套分片与副本记录", func(t *testing.T) {
		ns := newNodes(4)
		st := quietStore(t, ns, store.Config{Replication: 3, ChunkSize: 64})
		data := bytes.Repeat([]byte("concurrent-"), 200)

		const goroutines = 16
		var wg sync.WaitGroup
		errs := make(chan error, goroutines)
		results := make(chan string, goroutines)
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				r, err := st.PutContent(ctx, append([]byte(nil), data...))
				if err != nil {
					errs <- err
					return
				}
				results <- r.ContentID
			}()
		}
		wg.Wait()
		close(errs)
		close(results)
		for err := range errs {
			t.Fatalf("并发写入出现错误: %v", err)
		}
		idSet := map[string]int{}
		for id := range results {
			idSet[id]++
		}
		if len(idSet) != 1 || idSet[cas.ContentID(data)] != goroutines {
			t.Fatalf("并发写入结果不一致: %v", idSet)
		}
		// 检查节点上每个分片键只存在于恰好3个节点，无重复/损坏记录。
		st2 := st
		stat, err := st2.Stat(cas.ContentID(data))
		if err != nil {
			t.Fatal(err)
		}
		for cid, good := range stat {
			if len(good) != 3 {
				t.Fatalf("并发后分片 %s 完好副本=%d want=3（出现重复或损坏记录）", cid, len(good))
			}
		}
		logCase(t, biz, "16协程并发写同一内容：全部得到同一结果，每分片仍恰好3份完好副本")
	})

	t.Run("并发写不同内容互不干扰", func(t *testing.T) {
		ns := newNodes(5)
		st := quietStore(t, ns, store.Config{Replication: 2, ChunkSize: 64})
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				d := []byte(fmt.Sprintf("distinct-content-%d-%s", i, bytes.Repeat([]byte{'A' + byte(i%26)}, 50)))
				r, err := st.PutContent(ctx, d)
				if err != nil {
					t.Errorf("写入失败: %v", err)
					return
				}
				got, err := st.GetContent(ctx, r.ContentID)
				if err != nil || !bytes.Equal(got, d) {
					t.Errorf("并发写不同内容后取回不一致: %v", err)
				}
			}(i)
		}
		wg.Wait()
		logCase(t, biz, "8个不同内容并发写入后各自取回一致、互不干扰")
	})
}

func TestEmptyAndLargeContent(t *testing.T) {
	biz := "空内容与超大内容语义"
	ctx := context.Background()

	t.Run("空内容有确定一致的处理语义", func(t *testing.T) {
		ns := newNodes(3)
		st := quietStore(t, ns, store.Config{Replication: 2, ChunkSize: 64})
		r1, err := st.PutContent(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		r2, err := st.PutContent(ctx, []byte{})
		if err != nil {
			t.Fatal(err)
		}
		if r1.ContentID != cas.EmptyID || r1.ContentID != r2.ContentID {
			t.Fatalf("空内容标识不确定: %s vs %s", r1.ContentID, r2.ContentID)
		}
		if r1.ManifestID != r2.ManifestID || len(r1.Chunks) != 0 {
			t.Fatal("空内容清单应稳定且无分片")
		}
		got, err := st.GetContent(ctx, cas.EmptyID)
		if err != nil {
			t.Fatalf("空内容取回失败: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("空内容取回应为0字节，得到 %d", len(got))
		}
		logCase(t, biz, "nil/空切片: 同一标识、0分片、清单持久化、取回为0字节")
	})

	t.Run("超大内容切分取回一致", func(t *testing.T) {
		ns := newNodes(5)
		st := quietStore(t, ns, store.Config{Replication: 3, ChunkSize: 4096})
		// 约 2.5 MiB 伪随机内容，确保多分片且跨多节点。
		size := 256 * 1024
		data := make([]byte, size)
		var x uint32 = 2463534242
		for i := range data {
			x ^= x << 13
			x ^= x >> 17
			x ^= x << 5
			data[i] = byte(x >> 24)
		}
		res, err := st.PutContent(ctx, data)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Chunks) != size/4096 {
			t.Fatalf("分片数=%d want=%d", len(res.Chunks), size/4096)
		}
		got, err := st.GetContent(ctx, res.ContentID)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("大内容取回不一致: %v len=%d", err, len(got))
		}
		logCase(t, biz, "256KiB/4KiB 共64分片：写入、取回、整体哈希校验一致")
	})
}

func TestDiskPersistenceAcrossRestart(t *testing.T) {
	biz := "磁盘持久化与重启"
	ctx := context.Background()

	t.Run("进程重启后新Store从磁盘恢复全部内容", func(t *testing.T) {
		ns, dir := newDiskNodes(t, 4)
		cfg := store.Config{Replication: 3, ChunkSize: 64, MaxRetries: 2}
		st := quietStore(t, ns, cfg)
		data := bytes.Repeat([]byte("persist-across-restart-"), 60)
		res, err := st.PutContent(ctx, data)
		if err != nil {
			t.Fatal(err)
		}
		// 模拟全部进程重启：用同批目录重新构造节点与 Store。
		ns2 := openDiskNodes(t, dir, 4)
		st2 := quietStore(t, ns2, cfg)
		got, err := st2.GetContent(ctx, res.ContentID)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("重启后取回失败: %v", err)
		}
		logCase(t, biz, "分片、清单、索引全部落盘，重建Store后可完整取回")
	})
}

func TestContentTamperingEndToEnd(t *testing.T) {
	biz := "整体内容校验"
	ctx := context.Background()

	t.Run("篡改最后一个分片副本不会污染取回结果", func(t *testing.T) {
		ns := newNodes(4)
		st := quietStore(t, ns, store.Config{Replication: 3, ChunkSize: 64})
		data := bytes.Repeat([]byte("tail-tamper-"), 100)
		res, _ := st.PutContent(ctx, data)
		last := res.Chunks[len(res.Chunks)-1]
		for _, h := range res.Replicas[last.ID][:2] {
			vt := nodeByID(ns, h)
			if err := vt.Corrupt(findChunkKey(t, vt, last.ID)); err != nil {
				t.Fatal(err)
			}
		}
		got, err := st.GetContent(ctx, res.ContentID)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("尾分片损坏应能恢复: %v", err)
		}
		logCase(t, biz, "尾分片2份损坏：靠好副本恢复并通过整体内容哈希校验")
	})
}

func nodeByID(ns []node.Node, id string) *node.MemoryNode {
	for _, n := range ns {
		if n.ID() == id {
			return n.(*node.MemoryNode)
		}
	}
	return nil
}
