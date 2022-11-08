package kafka

import "unsafe"

type FilterFunc func(key []byte, value []byte) bool

func match(filters []FilterFunc, msg *Message) (found bool) {
	if len(filters) == 0 || len(msg.Headers) == 0 {
		return
	}
loop:
	for i := 0; i < len(filters); i++ {
		for j := 0; j < len(msg.Headers); j++ {
			header := msg.Headers[j]
			if filters[i](header.Key, header.Value) {
				break loop
			}
		}
	}
	return
}

func ToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func ToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
