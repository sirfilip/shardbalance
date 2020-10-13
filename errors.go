package shardbalance

import "errors"

var ErrNotFound = errors.New("not found")
var ErrShardExists = errors.New("shard already exists")
