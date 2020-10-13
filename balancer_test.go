package shardbalance

import (
	"errors"
	"testing"
)

func assertErrorExpectation(t *testing.T, expected, got error) {
	t.Helper()
	if !errors.Is(expected, got) {
		t.Errorf("error expectation failed. expected %v but got %v", expected, got)
	}
}

func TestBalancer_Register(t *testing.T) {
	for title, test := range map[string]struct {
		history []string
		addr    string
		err     error
	}{
		"unique addresses we like": {
			history: []string{"one", "two", "three"},
			addr:    "four",
			err:     nil,
		},
		"duplicate addresses we dont": {
			history: []string{"one", "two", "three"},
			addr:    "one",
			err:     ErrShardExists,
		},
	} {
		t.Run(title, func(t *testing.T) {
			balancer := NewBalancer(42)
			for _, addr := range test.history {
				err := balancer.Register(addr)
				if err != nil {
					t.Error(err)
				}
			}
			err := balancer.Register(test.addr)
			assertErrorExpectation(t, test.err, err)
		})
	}
}

func TestBalancer_Deregister(t *testing.T) {
	for title, test := range map[string]struct {
		history   []string
		addr      string
		shardKeys []string
		err       error
	}{
		"non existing addresses will flunk": {
			history:   []string{"one", "two", "three"},
			addr:      "four",
			shardKeys: []string{},
			err:       ErrNotFound,
		},
		"known addresses we can remove": {
			history:   []string{"one", "two", "three"},
			addr:      "one",
			shardKeys: []string{"one", "two", "three", "four"},
			err:       nil,
		},
	} {
		t.Run(title, func(t *testing.T) {
			balancer := NewBalancer(42)
			for _, addr := range test.history {
				err := balancer.Register(addr)
				if err != nil {
					t.Error(err)
				}
			}
			for _, shardKey := range test.shardKeys {
				_, _, err := balancer.Addr(shardKey)
				if err != nil {
					t.Error(err)
				}
			}
			err := balancer.Deregister(test.addr)
			assertErrorExpectation(t, test.err, err)
			for _, shardKey := range test.shardKeys {
				_, added, err := balancer.Addr(shardKey)
				if err != nil {
					t.Error(err)
				}
				if added {
					t.Error("Expected shard to exist")
				}
			}
		})
	}
}
