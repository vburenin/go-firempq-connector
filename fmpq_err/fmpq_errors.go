package fmpq_err

import "fmt"

type FireMpqError struct {
	Code int64
	Desc string
}

func NewFireMpqError(code int64, desc string) *FireMpqError {
	return &FireMpqError{Code: code, Desc: desc}
}

func (self *FireMpqError) Error() string {
	return fmt.Sprintf("FMPQERR %d:%s", self.Code, self.Desc)
}
