package parsers

import (
	"fmt"
	"strconv"

	. "github.com/vburenin/firempq_connector/fmpq_err"
)

func ParseInt(v string) (int64, error) {
	if v[0] == ':' {
		v = v[1:]
	}

	if v, err := strconv.ParseInt(v, 10, 0); err == nil {
		return v, nil
	}

	return 0, NewFireMpqError(-2, fmt.Sprintf("Wrong int format: %s", v))
}
