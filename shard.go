package shardbalance

type shardNode struct {
	key  string
	prev *shardNode
	next *shardNode
}

type shard struct {
	capacity int
	cache    map[string]*shardNode
	head     *shardNode
	tail     *shardNode
}

func NewLRU(capacity int) *shard {
	return &shard{capacity: capacity, cache: make(map[string]*shardNode)}
}

func (s *shard) Get(key string) error {
	node, found := s.cache[key]
	if !found {
		return ErrNotFound
	}
	s.promote(node)
	return nil
}

func (s *shard) Set(key string) {
	if s.capacity < 1 {
		return
	}

	node, found := s.cache[key]
	if found {
		s.promote(node)
	} else {
		s.add(&shardNode{key: key})
	}
}

func (s *shard) Itter() []string {
	addrs := make([]string, len(s.cache))
	i := 0
	for addr := range s.cache {
		addrs[i] = addr
		i++
	}
	return addrs
}

func (s *shard) promote(node *shardNode) {
	s.remove(node)
	s.add(node)
}

func (s *shard) remove(node *shardNode) {
	delete(s.cache, node.key)
	if s.head.key == node.key {
		s.head = node.next
	}
	if s.tail.key == node.key {
		s.tail = node.prev
	}
	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	}
	node.prev = nil
	node.next = nil
}

func (s *shard) add(node *shardNode) {
	if len(s.cache) == s.capacity {
		s.remove(s.tail)
	}
	if len(s.cache) == 0 {
		s.head = node
		s.tail = node
	} else {
		node.next = s.head
		s.head.prev = node
		s.head = node
	}
	s.cache[node.key] = node
}
