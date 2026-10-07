// Command demo 在本地磁盘与内存节点上演示去中心化内容存储与取回全流程：
//
//  1. 内容切分 + 内容标识 + 多副本写入
//  2. 部分节点故障下的多副本取回
//  3. 静默损坏被识别并跳过，从好副本恢复
//  4. 副本全部损坏/不足时给出可区分的错误原因
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/feng111627/content-addressed-storage-go/internal/cas"
	"github.com/feng111627/content-addressed-storage-go/internal/node"
	"github.com/feng111627/content-addressed-storage-go/internal/store"
)

func main() {
	ctx := context.Background()
	root, err := os.MkdirTemp("", "cas-demo-")
	must(err)
	defer os.RemoveAll(root)

	const nNodes = 4
	var nodes []node.Node
	for i := 0; i < nNodes; i++ {
		dir := filepath.Join(root, fmt.Sprintf("node%d", i))
		n, err := node.NewDiskBackedNode(fmt.Sprintf("node-%d", i), dir)
		must(err)
		nodes = append(nodes, n)
	}

	st, err := store.New(nodes, store.Config{Replication: 3, ChunkSize: 128, MaxRetries: 2})
	must(err)
	st.SetLogger(func(format string, args ...any) {
		fmt.Printf("    "+format+"\n", args...)
	})

	// 构造超过一个分片的内容（含重复模式，检验去重不影响正确性）。
	content := make([]byte, 900)
	for i := range content {
		content[i] = byte((i*7 + 3) % 251)
	}

	fmt.Println("== 场景1：切分、内容标识、3 副本写入 ==")
	res, err := st.PutContent(ctx, content)
	must(err)
	fmt.Printf("内容标识: %s\n分片数量: %d\n清单标识: %s\n",
		res.ContentID, len(res.Chunks), res.ManifestID)
	for cid, holders := range res.Replicas {
		fmt.Printf("  分片 %.19s... 副本节点=%v\n", cid, holders)
	}

	fmt.Println("\n== 场景2：一个节点故障，仍能取回并校验通过 ==")
	nodes[0].SetHealthy(false)
	got, err := st.GetContent(ctx, res.ContentID)
	must(err)
	checkEqual(content, got)
	fmt.Println("取回内容与原文一致 ✓（已跳过故障节点）")
	nodes[0].SetHealthy(true)

	fmt.Println("\n== 场景3：在一个节点上篡改分片，自动识别坏副本并从好副本恢复 ==")
	// 找到第一个分片落在哪些节点，挑一个非唯一副本做破坏。
	firstID := res.Chunks[0].ID
	target := pickCorruptTarget(res.Replicas[firstID])
	corruptNode(nodes, target, "cas/chunk/"+firstID)
	got, err = st.GetContent(ctx, res.ContentID)
	must(err)
	checkEqual(content, got)
	fmt.Printf("节点 %s 上的分片已损坏，取回内容仍与原文一致 ✓\n", target)

	fmt.Println("\n== 场景4：破坏全部副本（或令节点不足），必须报可区分错误 ==")
	// 场景3 已在 target 上翻转过一次字节，先再翻转一次将其还原，
	// 随后对 3 个副本节点各破坏一次，确保没有任何完好副本残留。
	corruptNode(nodes, target, "cas/chunk/"+firstID)
	for _, nid := range res.Replicas[firstID] {
		corruptNode(nodes, nid, "cas/chunk/"+firstID)
	}
	_, err = st.GetContent(ctx, res.ContentID)
	switch {
	case errors.Is(err, cas.ErrMissingChunk):
		fmt.Printf("收到预期的可区分错误 ErrMissingChunk:\n  %v\n", err)
	case errors.Is(err, cas.ErrChunkMismatch):
		fmt.Printf("收到预期的可区分错误 ErrChunkMismatch:\n  %v\n", err)
	default:
		fail("预期取回失败，但得到: %v", err)
	}

	fmt.Println("\n== 场景5：副本数无法满足时写入失败并给出原因 ==")
	for _, n := range nodes {
		n.SetHealthy(false)
	}
	_, err = st.PutContent(ctx, []byte("需要3副本但没有足够节点"))
	if errors.Is(err, cas.ErrInsufficientNodes) {
		var re *cas.ReplicationError
		if errors.As(err, &re) {
			fmt.Printf("收到 ReplicationError: required=%d achieved=%d 失败节点数=%d\n",
				re.Required, re.Achieved, len(re.Failed))
		}
	} else {
		fail("预期 ErrInsufficientNodes，得到: %v", err)
	}

	fmt.Println("\n全部演示场景结束 ✓")
}

func pickCorruptTarget(holders []string) string {
	if len(holders) < 2 {
		fail("该分片副本数不足 2，无法在保留好副本的前提下做损坏实验")
	}
	return holders[0]
}

func corruptNode(nodes []node.Node, id, key string) {
	for _, n := range nodes {
		if n.ID() == id {
			mn, ok := n.(*node.MemoryNode)
			if !ok {
				fail("节点 %s 不是 *MemoryNode，无法注入损坏", id)
			}
			if err := mn.Corrupt(key); err != nil {
				fail("损坏注入失败 node=%s key=%s: %v", id, key, err)
			}
			return
		}
	}
	fail("找不到节点 %s", id)
}

func checkEqual(want, got []byte) {
	if len(want) != len(got) {
		fail("长度不一致 want=%d got=%d", len(want), len(got))
	}
	for i := range want {
		if want[i] != got[i] {
			fail("第 %d 字节不一致", i)
		}
	}
}

func must(err error) {
	if err != nil {
		fail("%v", err)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "demo 失败: "+format+"\n", args...)
	os.Exit(1)
}
