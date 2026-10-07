package cas

import (
	"bytes"
	"testing"
)

func TestManifestAddressing(t *testing.T) {
	biz := "分片清单可寻址"
	data := bytes.Repeat([]byte("manifest-"), 200)
	chunks, _ := Split(data, 64)
	m, err := NewManifest(chunks, 64)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("清单记录整体标识与长度", func(t *testing.T) {
		if m.ContentID != ContentID(data) {
			t.Fatal("清单的整体内容标识不正确")
		}
		if m.Length != int64(len(data)) {
			t.Fatalf("清单长度=%d want=%d", m.Length, len(data))
		}
		if len(m.Chunks) != len(chunks) {
			t.Fatal("清单分片引用数量不符")
		}
		logCase(t, biz, "清单包含整体标识、长度与按序分片引用")
	})

	t.Run("清单自身可寻址且稳定", func(t *testing.T) {
		id1, err := ManifestID(m)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := MarshalManifest(m)
		m2, _ := UnmarshalManifest(b)
		id2, _ := ManifestID(m2)
		if id1 != id2 {
			t.Fatal("序列化往返后清单标识变化")
		}
		if len(id1) != IDLen {
			t.Fatalf("清单标识长度非法: %s", id1)
		}
		logCase(t, biz, "清单序列化往返后自身标识稳定: "+id1[:25]+"...")
	})

	t.Run("序列化字节确定", func(t *testing.T) {
		b1, _ := MarshalManifest(m)
		b2, _ := MarshalManifest(m)
		if !bytes.Equal(b1, b2) {
			t.Fatal("同一清单两次序列化字节不同")
		}
		logCase(t, biz, "同一清单的规范化字节表示唯一")
	})

	t.Run("篡改清单被识别", func(t *testing.T) {
		b, _ := MarshalManifest(m)
		bad := append([]byte(nil), b...)
		// 修改长度字段中的某个数字字符
		idx := bytes.Index(bad, []byte("\"length\":"))
		pos := idx + len("\"length\":")
		bad[pos] = (bad[pos]-'0'+1)%10 + '0'
		badM, err := UnmarshalManifest(bad)
		if err == nil {
			err = VerifyManifest(badM)
		}
		if err == nil {
			t.Fatal("被篡改的清单竟然通过校验")
		}
		logCase(t, biz, "长度被篡改的清单无法通过校验: "+firstLine(err))
	})

	t.Run("坏分片无法进入清单", func(t *testing.T) {
		bad := append([]Chunk(nil), chunks...)
		bad[0].Data = append(bad[0].Data, 0x00) // 数据与标识不符
		if _, err := NewManifest(bad, 64); err == nil {
			t.Fatal("含坏分片时不应能构建清单")
		}
		logCase(t, biz, "构建清单时逐片校验，坏分片被拒绝")
	})

	t.Run("分片长度与ChunkSize不一致被拒绝", func(t *testing.T) {
		b, _ := MarshalManifest(m)
		badM, _ := UnmarshalManifest(b)
		badM.Chunks[0].Length = 1
		if err := VerifyManifest(badM); err == nil {
			t.Fatal("非尾分片长度不等于ChunkSize 应当报错")
		}
		logCase(t, biz, "VerifyManifest 拒绝结构异常的清单")
	})

	t.Run("空内容清单语义确定", func(t *testing.T) {
		empty, _ := Split(nil, 64)
		m0, err := NewManifest(empty, 64)
		if err != nil {
			t.Fatal(err)
		}
		if m0.ContentID != EmptyID || m0.Length != 0 || len(m0.Chunks) != 0 {
			t.Fatalf("空内容清单语义异常: %+v", m0)
		}
		if err := VerifyManifest(m0); err != nil {
			t.Fatal(err)
		}
		logCase(t, biz, "空内容清单: 标识=EmptyID, 长度0, 无分片引用")
	})
}
