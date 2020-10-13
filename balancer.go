// shardbalance provides lru map with last access time
// TODO write tests
package shardbalance

type Balancer interface {
	Register(addr string) error
	Deregister(addr string) error
	Addr(shardKey string) (string, bool, error)
}

type LruBasedBalancer struct {
	shardAddresses ShardAddressCollection
	shards         map[string]*shard
	shardCapacity  int
}

func NewBalancer(shardCapacity int) *LruBasedBalancer {
	return &LruBasedBalancer{shardAddresses: NewAddressCollection(), shardCapacity: shardCapacity, shards: make(map[string]*shard)}
}

func (b *LruBasedBalancer) Register(addr string) error {
	if err := b.shardAddresses.Register(addr); err != nil {
		return err
	}
	b.shards[addr] = NewLRU(b.shardCapacity)
	return nil
}

func (b *LruBasedBalancer) Deregister(addr string) error {
	err := b.shardAddresses.Deregister(addr)
	if err != nil {
		return err
	}
	// rebalance existing shardIDs to the existing shards
	shard, found := b.shards[addr]
	if !found {
		return ErrNotFound
	}

	for _, shardKey := range shard.Itter() {
		_, _, err := b.Addr(shardKey)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *LruBasedBalancer) Addr(shardKey string) (string, bool, error) {
	for _, addr := range b.shardAddresses.Itter() {
		if err := b.shards[addr].Get(shardKey); err == nil {
			return addr, false, nil
		}
	}
	shardAddr, err := b.shardAddresses.LeastUsed()
	if err != nil {
		return "", false, err
	}
	b.shards[shardAddr].Set(shardKey)
	return shardAddr, true, nil
}
