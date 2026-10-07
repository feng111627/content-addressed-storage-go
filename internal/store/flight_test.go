package store_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/feng111627/content-addressed-storage-go/internal/node"
	"github.com/feng111627/content-addressed-storage-go/internal/store"
)

// countNode 包装真实节点，统计物理写入次数并在写入时制造延迟，
// 使并发请求必然在同一时间窗内碰撞，从而检验单飞去重。
type countNode struct {
	node.Node
	mu       *sync.Mutex
	putCount *int
	delay    time.Duration
}

func (c *countNode) Put(key string, data []byte) error {
	c.mu.Lock()
	*c.putCount++
	c.mu.Unlock()
	time.Sleep(c.delay)
	return c.Node.Put(key, data)
}

func TestConcurrentPutSingleFlight(t *testing.T) {
	biz := "并发写入去重"
	ns0 := newNodes(5)
	var mu sync.Mutex
	putCount := 0
	ns := make([]node.Node, len(ns0))
	for i, n := range ns0 {
		ns[i] = &countNode{Node: n, mu: &mu, putCount: &putCount, delay: 5 * time.Millisecond}
	}
	st, err := store.New(ns, store.Config{Replication: 3, ChunkSize: 64, MaxRetries: 2})
	if err != nil {
		t.Fatal(err)
	}
	st.SetLogger(func(string, ...any) {})

	data := pseudoRandom(900) // 多个分片、各分片 ID 唯一
	const N = 16
	var wg sync.WaitGroup
	wg.Add(N)
	start := make(chan struct{})
	errCh := make(chan error, N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			<-start // 尽量同时发起
			_, err := st.PutContent(context.Background(), append([]byte(nil), data...))
			errCh <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for e := range errCh {
		if e != nil {
			t.Fatalf("并发写入失败: %v", e)
		}
	}

	// 每个对象的物理 Put 次数上限：分片数*3 + 清单*3 + 索引扩散*5。
	// 若没有单飞，N 个请求会各写一遍，计数放大到近 N 倍。
	// 这里验证单飞生效：总物理写入次数等于“一次写入”的规模，而非 N 倍。
	mu.Lock()
	got := putCount
	mu.Unlock()
	chunks := (len(data) + 63) / 64
	upper := chunks*3 + 3 + 5
	if got > upper {
		t.Fatalf("发生重复物理写入: putCount=%d 单次写入上限=%d（并发数=%d）", got, upper, N)
	}
	t.Logf("【业务】%-20s 【用例】%d 个并发请求物理 Put 仅 %d 次（单次上限 %d），无重复分片 ... ✅ 通过",
		biz, N, got, upper)
	_ = fmt.Sprint
}
