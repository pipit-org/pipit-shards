package shards

import "errors"

var (
	ErrNilShardManager = errors.New("shards: invalid ShardManager: <nil> pointer")
	ErrNotInit         = errors.New("shards: ShardManager not initialized, need a Init call")
	ErrShardNotFound   = errors.New("shards: not found")
)
