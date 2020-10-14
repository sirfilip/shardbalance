package shardbalance

import (
	"sync"
)

type ShardAddressCollection interface {
	Register(addr string) error
	Deregister(addr string) error
	Itter() []string
	LeastUsed() (string, error)
}

type shardCollNode struct {
	addr string
	prev *shardCollNode
	next *shardCollNode
}

type shardAddressCollection struct {
	sync.RWMutex
	cache map[string]*shardCollNode
	head  *shardCollNode
	tail  *shardCollNode
}

func newAddressCollection() ShardAddressCollection {
	return &shardAddressCollection{cache: make(map[string]*shardCollNode)}
}

func (col *shardAddressCollection) Itter() []string {
	col.RLock()
	defer col.RUnlock()
	shards := make([]string, len(col.cache))
	i := 0
	for _, shard := range col.cache {
		shards[i] = shard.addr
		i++
	}
	return shards
}

func (col *shardAddressCollection) Register(addr string) error {
	col.Lock()
	defer col.Unlock()

	_, found := col.cache[addr]
	if found {
		return ErrShardExists
	}

	node := &shardCollNode{addr: addr}
	col.cache[addr] = node

	if len(col.cache) == 1 {
		col.head = node
		col.tail = node
		return nil
	}

	node.prev = col.tail
	col.tail.next = node
	col.tail = node
	return nil
}

func (col *shardAddressCollection) Deregister(addr string) error {
	col.Lock()
	defer col.Unlock()
	node, found := col.cache[addr]
	if !found {
		return ErrNotFound
	}

	if col.head.addr == addr {
		col.head = node.next
	}

	if col.tail.addr == addr {
		col.tail = node.prev
	}
	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	}
	node.prev = nil
	node.next = nil
	delete(col.cache, addr)
	return nil
}

func (col *shardAddressCollection) LeastUsed() (string, error) {
	col.Lock()
	defer col.Unlock()
	if len(col.cache) == 0 {
		return "", ErrNotFound
	}

	if len(col.cache) == 1 {
		return col.head.addr, nil
	}

	addr := col.tail.addr
	node := col.tail

	node.prev.next = node.next
	col.tail = node.prev

	col.head.prev = node
	node.next = col.head
	node.prev = nil
	col.head = node
	return addr, nil
}
