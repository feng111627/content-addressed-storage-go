package cas

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

// 统一的业务测试日志：先打印业务名称，再逐条打印用例，
// 让 `go test -v` 日志能直观看出覆盖了哪些业务及每条用例的通过情况。
func logCase(t *testing.T, business, caseName string) {
	t.Helper()
	t.Logf("【业务】%-18s 【用例】%s ... ✅ 通过", business, caseName)
}

func TestContentIDStability(t *testing.T) {
	biz := "内容标识稳定性"

	t.Run("相同内容标识一致", func(t *testing.T) {
		data := []byte("hello decentralized world")
		if ContentID(data) != ContentID(append([]byte(nil), data...)) {
			t.Fatal("相同内容产生了不同标识")
		}
		logCase(t, biz, "相同字节内容多次计算得到同一标识")
	})

	t.Run("单字节变化标识必变", func(t *testing.T) {
		data := []byte("hello decentralized world")
		ids := map[string]bool{}
		for i := range data {
			b := append([]byte(nil), data...)
			b[i] ^= 0x01
			id := ContentID(b)
			if id == ContentID(data) {
				t.Fatalf("翻转第 %d 字节后标识未变化", i)
			}
			if ids[id] {
				t.Fatalf("不同内容产生了相同标识")
			}
			ids[id] = true
		}
		logCase(t, biz, "任意一位字节变化都导致标识不同")
	})

	t.Run("标识格式与算法正确", func(t *testing.T) {
		data := []byte("abc")
		sum := sha256.Sum256(data)
		want := IDPrefix + hex.EncodeToString(sum[:])
		if got := ContentID(data); got != want {
			t.Fatalf("ContentID=%s want=%s", got, want)
		}
		if len(want) != IDLen || !strings.HasPrefix(want, IDPrefix) {
			t.Fatalf("标识格式非法: %q", want)
		}
		logCase(t, biz, "标识为 sha256-<64位小写hex> 且摘要正确")
	})

	t.Run("空内容有确定标识", func(t *testing.T) {
		if ContentID(nil) != ContentID([]byte{}) {
			t.Fatal("nil 与空切片的标识不一致")
		}
		if ContentID(nil) != EmptyID {
			t.Fatalf("空内容标识错误: %s", ContentID(nil))
		}
		logCase(t, biz, "空内容(nil/空切片)标识确定且一致")
	})
}

func TestSplitAndJoin(t *testing.T) {
	biz := "分片与重组"

	t.Run("分片序号长度与标识正确", func(t *testing.T) {
		data := bytes.Repeat([]byte{'A'}, 1000)
		chunks, err := Split(data, 128)
		if err != nil {
			t.Fatal(err)
		}
		if len(chunks) != 8 { // 7*128 + 1*104
			t.Fatalf("分片数=%d want=8", len(chunks))
		}
		for i, c := range chunks {
			if c.Seq != i {
				t.Fatalf("第 %d 片序号=%d", i, c.Seq)
			}
			if c.ID != ContentID(c.Data) {
				t.Fatalf("第 %d 片标识与其数据不符", i)
			}
			wantLen := 128
			if i == 7 {
				wantLen = 104
			}
			if len(c.Data) != wantLen {
				t.Fatalf("第 %d 片长度=%d want=%d", i, len(c.Data), wantLen)
			}
		}
		logCase(t, biz, "1000字节按128切分为7整1尾，序号/长度/标识正确")
	})

	t.Run("空内容切分为空列表", func(t *testing.T) {
		chunks, err := Split(nil, 128)
		if err != nil || len(chunks) != 0 {
			t.Fatalf("空内容切分结果异常: chunks=%v err=%v", chunks, err)
		}
		joined, err := Join(chunks)
		if err != nil || len(joined) != 0 {
			t.Fatalf("空分片重组异常: %v err=%v", joined, err)
		}
		logCase(t, biz, "空内容切分为0片，重组为空字节")
	})

	t.Run("切分稳定性: 同一内容多次切分一致", func(t *testing.T) {
		data := []byte("确定性分片结果")
		a, _ := Split(data, 4)
		b, _ := Split(data, 4)
		if !EqualChunks(a, b) {
			t.Fatal("同一内容两次切分结果不一致")
		}
		logCase(t, biz, "同一内容多次切分得到相同的分片与标识序列")
	})

	t.Run("内容变化分片标识不同", func(t *testing.T) {
		a, _ := Split([]byte("xxxx-abc"), 4)
		b, _ := Split([]byte("xxxx-abd"), 4)
		if EqualChunks(a, b) {
			t.Fatal("内容变化后分片标识仍相同")
		}
		logCase(t, biz, "末位变化导致受影响分片标识变化")
	})

	t.Run("非法分片大小报错", func(t *testing.T) {
		if _, err := Split([]byte("x"), 0); err == nil {
			t.Fatal("size=0 应当报错")
		} else {
			logCase(t, biz, "size=0 返回错误: "+err.Error())
		}
		if _, err := Split([]byte("x"), -1); err == nil {
			t.Fatal("负 size 应当报错")
		}
	})

	t.Run("重组还原原文", func(t *testing.T) {
		data := bytes.Repeat([]byte{1, 2, 3, 4, 5}, 500)
		chunks, _ := Split(data, 64)
		got, err := Join(chunks)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, data) {
			t.Fatal("重组内容与原文不一致")
		}
		logCase(t, biz, "切分后 Join 逐字节还原原文")
	})

	t.Run("损坏分片不得参与重组", func(t *testing.T) {
		chunks, _ := Split([]byte("0123456789"), 4)
		chunks[1].Data[0] ^= 0xFF // 篡改但不更新标识
		if _, err := Join(chunks); err == nil {
			t.Fatal("损坏分片竟然重组成功")
		} else {
			logCase(t, biz, "分片被篡改时 Join 拒绝返回内容: "+firstLine(err))
		}
	})

	t.Run("分片缺序号时重组报错", func(t *testing.T) {
		chunks, _ := Split([]byte("0123456789"), 4)
		_, err := Join(append(chunks[:1], chunks[2:]...))
		if err == nil {
			t.Fatal("缺失分片序号应当报错")
		}
		logCase(t, biz, "分片序号不连续时 Join 报错")
	})

	t.Run("流式切分与内存切分等价", func(t *testing.T) {
		data := bytes.Repeat([]byte("stream-"), 300)
		a, _ := Split(data, 33)
		b, err := SplitReader(bytes.NewReader(data), 33)
		if err != nil || !EqualChunks(a, b) {
			t.Fatalf("SplitReader 与 Split 结果不一致 err=%v", err)
		}
		logCase(t, biz, "SplitReader 与 Split 产生相同分片")
	})
}

func firstLine(err error) string {
	s := err.Error()
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return s
}
