package encoders

import (
	"strconv"
)

func EncodeString(v string) []byte {
	a := make([]byte, 1, 12+len(v))
	a[0] = '$'
	num := strconv.AppendInt(make([]byte, 0, 10), int64(len(v)), 10)
	a = append(a, num...)
	a = append(a, ' ')
	a = append(a, v...)
	return a
}

func EncodeInt64(v int64) []byte {
	return strconv.AppendInt(make([]byte, 0, 10), v, 10)
}
