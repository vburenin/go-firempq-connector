package pqclient

import (
	. "github.com/vburenin/firempq_connector/encoders"
)

var popPrmPopWait = []byte("WAIT")
var popPrmLockTimeoutT = []byte("TIMEOUT")
var popPrmLimit = []byte("LIMIT")
var popPrmAsync = []byte("ASYNC")

// popOptions are used to set POP call parameters.
type popOptions struct {
	limit         int64
	waitTimeout   int64
	asyncCallback func(*PriorityQueue, error)
	asyncId       string
}

// NewPopOptions returns an empty instance of POP options.
func NewPopOptions() *popOptions {
	return &popOptions{}
}

// SetLimit sets user defined limit. Upper bound is defined by service config.
func (opts *popOptions) SetLimit(limit int64) *popOptions {
	opts.limit = limit
	return opts
}

// SetWaitTimeout sets wait timeout in milliseconds if no messages are available in the queue.
// Max limit defined by service config.
func (opts *popOptions) SetWaitTimeout(waitTimeout int64) *popOptions {
	opts.waitTimeout = waitTimeout
	return opts
}

func (opts *popOptions) SetAsyncCallback(cb func(*PriorityQueue, error)) *popOptions {
	opts.asyncCallback = cb
	return opts
}

func (opts *popOptions) makeParams() [][]byte {
	if opts == nil {
		return nil
	}
	args := make([][]byte, 0, 2)
	if opts.limit > 0 {
		args = append(args, popPrmLimit)
		args = append(args, EncodeInt64(opts.limit))
	}
	if opts.waitTimeout > 0 {
		args = append(args, popPrmPopWait)
		args = append(args, EncodeInt64(opts.waitTimeout))
	}
	if opts.asyncId != "" {
		args = append(args, popPrmAsync)
		args = append(args, EncodeString(opts.asyncId))
	}
	return args
}

type popLockOptions struct {
	limit         int64
	waitTimeout   int64
	lockTimeout   int64
	asyncCallback func(*PriorityQueue, error)
	asyncId       string
}

func NewPopLockOptions() *popLockOptions {
	return &popLockOptions{lockTimeout: -1}
}

// SetLimit sets user defined limit. Upper bound is defined by service config.
func (opts *popLockOptions) SetLimit(limit int64) *popLockOptions {
	opts.limit = limit
	return opts
}

// SetWaitTimeout sets wait timeout in milliseconds if no messages are available in the queue.
// Max limit defined by service config.
func (opts *popLockOptions) SetWaitTimeout(waitTimeout int64) *popLockOptions {
	opts.waitTimeout = waitTimeout
	return opts
}

func (opts *popLockOptions) SetLockTimeout(lockTimeout int64) *popLockOptions {
	opts.lockTimeout = lockTimeout
	return opts
}

func (opts *popLockOptions) SetAsyncCallback(cb func(*PriorityQueue, error)) *popLockOptions {
	opts.asyncCallback = cb
	return opts
}

func (opts *popLockOptions) makeParams() [][]byte {
	if opts == nil {
		return nil
	}
	args := make([][]byte, 0, 2)
	if opts.limit != 0 {
		args = append(args, popPrmLimit)
		args = append(args, EncodeInt64(opts.limit))
	}
	if opts.waitTimeout > 0 {
		args = append(args, popPrmPopWait)
		args = append(args, EncodeInt64(opts.waitTimeout))
	}
	if opts.lockTimeout >= 0 {
		args = append(args, popPrmLockTimeoutT)
		args = append(args, EncodeInt64(opts.lockTimeout))
	}
	if opts.asyncId != "" {
		args = append(args, popPrmAsync)
		args = append(args, EncodeString(opts.asyncId))
	}
	return args
}
