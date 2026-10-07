// Package contentid 定义内容寻址标识的生成规则。
//
// 寻址规则：对内容的完整字节流计算 SHA-256，取其 64 个小写十六进制
// 字符作为标识。SHA-256 的抗碰撞性保证：
//   - 内容字节完全一致时，标识必然一致；
//   - 内容任意一个字节不同，摘要以压倒性概率不同（雪崩效应）；
//   - 空内容（零字节）也有确定标识，即 SHA-256("")。
package contentid

import (
	"fmt"
	"crypto/sha256"
	"encoding/hex"
)

// ID 是内容的稳定标识（SHA-256 摘要的十六进制表示）。
type ID string

// Algorithm 是当前使用的哈希算法名称。
const Algorithm = "sha256"

// IDLen 是十六进制标识的长度（SHA-256 = 32 字节 = 64 个十六进制字符）。
const IDLen = sha256.Size * 2

// Compute 计算数据的内容标识。内容相同则标识相同，内容任意字节不同则标识不同。
// 注意：nil 与空切片都表示零字节内容，二者标识相同。
func Compute(data []byte) ID {
	sum := sha256.Sum256(data)
	return ID(hex.EncodeToString(sum[:]))
}

// emptyID 在包初始化时计算一次，保证空内容标识全局一致。
var emptyID = Compute(nil)

// Empty 返回空内容（零字节）的确定标识：SHA-256("")。
func Empty() ID {
	return emptyID
}

// Valid 判断标识格式是否合法：64 个小写十六进制字符。
func (id ID) Valid() bool {
	if len(id) != IDLen {
		return false
	}
	for _, c := range id {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}

// String 便于日志输出。
func (id ID) String() string {
	return string(id)
}

// MarshalText / UnmarshalText 让 ID 在 JSON 持久化时就是普通字符串，
// 并在反序列化时校验格式，拒绝非法标识。
func (id ID) MarshalText() ([]byte, error) {
	return []byte(id), nil
}

func (id *ID) UnmarshalText(text []byte) error {
	v := ID(text)
	if !v.Valid() {
		return fmt.Errorf("contentid: invalid id %q", string(text))
	}
	*id = v
	return nil
}
