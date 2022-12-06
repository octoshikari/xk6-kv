package kv

import (
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	"go.k6.io/k6/js/modules"
)

func init() {
	modules.Register("k6/x/kv", new(RootModule))
}

type RootModule struct{}

type ModuleInstance struct {
	vu modules.VU
}

// Ensure the interfaces are implemented correctly.
var (
	_ modules.Module   = &RootModule{}
	_ modules.Instance = &ModuleInstance{}
)

type Client struct {
	db *badger.DB
}

var check = false
var client *Client

// NewModuleInstance implements the modules.Module interface to return
// a new instance for each VU.
func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &ModuleInstance{
		vu: vu,
	}
}

// Exports implements the modules.Instance interface and returns the exports
// of the JS module.
func (kv *ModuleInstance) Exports() modules.Exports {
	return modules.Exports{Default: kv}
}

// NewClient returns a new KV Client.
func (kv *ModuleInstance) NewClient(name string, memory bool) *Client {
	if check != true {
		if name == "" {
			name = "/tmp/badger"
		}
		var db *badger.DB
		if memory {
			db, _ = badger.Open(badger.DefaultOptions("").WithLoggingLevel(badger.ERROR).WithInMemory(true))
		} else {
			db, _ = badger.Open(badger.DefaultOptions(name).WithLoggingLevel(badger.ERROR))
		}
		client = &Client{db: db}
		check = true
		return client
	} else {
		return client
	}
}

// Set the given key with the given value.
func (c *Client) Set(key string, value string) error {
	err := c.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), []byte(value))
		return err
	})
	return err
}

// SetWithTTLInSecond Set the given key with the given value with TTL in second
func (c *Client) SetWithTTLInSecond(key string, value string, ttl int) error {
	err := c.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry([]byte(key), []byte(value)).WithTTL(time.Duration(ttl) * time.Second)
		err := txn.SetEntry(e)
		return err
	})
	return err
}

// Get returns the value for the given key.
func (c *Client) Get(key string) (string, error) {
	var valCopy []byte
	_ = c.db.View(func(txn *badger.Txn) error {
		item, _ := txn.Get([]byte(key))
		if item != nil {
			valCopy, _ = item.ValueCopy(nil)
		}
		return nil
	})
	if len(valCopy) > 0 {
		return string(valCopy), nil
	}
	return "", fmt.Errorf("error in get value with key %s", key)
}

// ViewPrefix return all the key value pairs where the key starts with some prefix.
func (c *Client) ViewPrefix(prefix string) map[string]string {
	m := make(map[string]string)
	err := c.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(prefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				m[string(k)] = string(v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return m
}

// Delete the given key
func (c *Client) Delete(key string) error {
	err := c.db.Update(func(txn *badger.Txn) error {
		item, _ := txn.Get([]byte(key))
		if item != nil {
			err := txn.Delete([]byte(key))
			return err
		}
		return nil
	})
	return err
}
