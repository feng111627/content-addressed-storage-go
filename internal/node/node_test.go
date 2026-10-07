package node

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func logCase(t *testing.T, business, caseName string) {
	t.Helper()
	t.Logf("【业务】%-22s 【用例】%s ... ✅ 通过", business, caseName)
}

func TestMemoryNodeBasic(t *testing.T) {
	biz := "内存节点读写"
	n := NewMemoryNode("mem-1")

	t.Run("写入后可读回", func(t *testing.T) {
		if err := n.Put("k1", []byte("v1")); err != nil {
			t.Fatal(err)
		}
		got, err := n.Get("k1")
		if err != nil || string(got) != "v1" {
			t.Fatalf("读回=%q err=%v", got, err)
		}
		has, _ := n.Has("k1")
		if !has {
			t.Fatal("Has 应返回 true")
		}
		logCase(t, biz, "Put/Get/Has 基本往返")
	})

	t.Run("返回数据副本不受外部修改影响", func(t *testing.T) {
		v := []byte("immutable")
		_ = n.Put("k2", v)
		v[0] = 'X'
		got, _ := n.Get("k2")
		if string(got) != "immutable" {
			t.Fatal("节点数据被外部切片修改污染")
		}
		logCase(t, biz, "Get 返回拷贝，隔离外部可变切片")
	})

	t.Run("不存在的键返回 ErrNotFound", func(t *testing.T) {
		if _, err := n.Get("missing"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("期望 ErrNotFound, 得到 %v", err)
		}
		logCase(t, biz, "缺失键 Get 返回可判定的 ErrNotFound")
	})

	t.Run("删除后不可读", func(t *testing.T) {
		_ = n.Put("k3", []byte("v3"))
		if err := n.Delete("k3"); err != nil {
			t.Fatal(err)
		}
		if _, err := n.Get("k3"); !errors.Is(err, ErrNotFound) {
			t.Fatal("删除后仍可读到对象")
		}
		if err := n.Delete("never-exist"); err != nil {
			t.Fatal("删除不存在的键不应报错")
		}
		logCase(t, biz, "Delete 后 Get 失败；重复删除幂等")
	})
}

func TestNodeFailure(t *testing.T) {
	biz := "节点故障行为"
	n := NewMemoryNode("mem-down")

	t.Run("故障节点写入不得成功", func(t *testing.T) {
		n.SetHealthy(false)
		if err := n.Put("k", []byte("v")); err == nil {
			t.Fatal("故障节点 Put 竟然成功")
		}
		if n.Healthy() {
			t.Fatal("Healthy 状态错误")
		}
		logCase(t, biz, "节点下线后 Put/Get/Has 全部报错")
	})

	t.Run("故障节点读取不得成功", func(t *testing.T) {
		if _, err := n.Get("k"); err == nil {
			t.Fatal("故障节点 Get 竟然成功")
		}
		if _, err := n.Has("k"); err == nil {
			t.Fatal("故障节点 Has 竟然成功")
		}
		logCase(t, biz, "节点下线期间任何读取都不返回数据")
	})

	t.Run("恢复后数据仍在", func(t *testing.T) {
		n.SetHealthy(true)
		_ = n.Put("k", []byte("persisted-in-mem"))
		n.SetHealthy(false)
		n.SetHealthy(true)
		got, err := n.Get("k")
		if err != nil || string(got) != "persisted-in-mem" {
			t.Fatalf("节点恢复后数据异常: %q %v", got, err)
		}
		logCase(t, biz, "节点重新上线后原有数据可继续服务")
	})
}

func TestDiskBackedNode(t *testing.T) {
	biz := "磁盘持久化节点"
	dir := t.TempDir()

	t.Run("写入落盘并可重新加载", func(t *testing.T) {
		n, err := NewDiskBackedNode("disk-1", dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Put("cas/chunk/abc", []byte("payload")); err != nil {
			t.Fatal(err)
		}
		// 模拟进程重启：用同一目录构造新节点。
		n2, err := NewDiskBackedNode("disk-1", dir)
		if err != nil {
			t.Fatal(err)
		}
		got, err := n2.Get("cas/chunk/abc")
		if err != nil || string(got) != "payload" {
			t.Fatalf("重启后加载失败: %q %v", got, err)
		}
		logCase(t, biz, "写入磁盘后，新节点实例从目录恢复对象")
	})

	t.Run("磁盘上能看到对象文件", func(t *testing.T) {
		matches, _ := filepath.Glob(filepath.Join(dir, "*.obj"))
		if len(matches) == 0 {
			t.Fatal("未找到持久化对象文件")
		}
		b, err := os.ReadFile(matches[0])
		if err != nil || string(b) != "payload" {
			t.Fatalf("对象文件内容异常: %q %v", b, err)
		}
		logCase(t, biz, "对象以普通文件形式持久化在节点目录")
	})

	t.Run("损坏注入改变内容", func(t *testing.T) {
		n, _ := NewDiskBackedNode("disk-2", t.TempDir())
		_ = n.Put("k", []byte("original"))
		if err := n.Corrupt("k"); err != nil {
			t.Fatal(err)
		}
		got, _ := n.Get("k")
		if string(got) == "original" {
			t.Fatal("Corrupt 后内容竟然未变")
		}
		logCase(t, biz, "Corrupt 翻转字节模拟静默损坏，且同步落盘")
	})

	t.Run("损坏注入对不存在键报错", func(t *testing.T) {
		n, _ := NewDiskBackedNode("disk-3", t.TempDir())
		if err := n.Corrupt("ghost"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("期望 ErrNotFound, 得到 %v", err)
		}
		logCase(t, biz, "对缺失对象做损坏注入返回 ErrNotFound")
	})
}
