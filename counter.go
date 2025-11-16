package sqlitekv

import (
	"context"
	"fmt"
	"sync"
)

func (c *Counter) GetId() int64 {
	return c.Id
}
func (c *Counter) SetId(id int64) {
	c.Id = id
}

func (c *Counter) GetVal() (val []byte, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (c *Counter) SetVal(val []byte) error {
	return fmt.Errorf("not implemented")
}

func (c *Counter) Column(i int, name string) (ok bool, val any, err error) {
	return false, nil, nil
}

func (c *Counter) SetColumn(i int, name string, ok bool, val any) error {
	return nil
}

type Counter struct {
	Id           int64
	Start        int64 `json:"start"`
	ReserveCount int64 `json:"reserve_count"`
	Current      int64 `json:"current"`
	Limit        int64 `json:"limit"`

	col  *Collection
	opts CounterOptions

	mu sync.Mutex
}

type CounterOptions struct {
	StartCount   int64
	ReserveCount int64
}

func NewCounter(col *Collection, opts CounterOptions) (c *Counter, err error) {
	if opts.ReserveCount <= 0 {
		opts.ReserveCount = 100
	}

	if opts.StartCount <= 0 {
		opts.StartCount = 1
	}

	c = &Counter{
		Start:        opts.StartCount,
		ReserveCount: opts.ReserveCount,
		col:          col,
		opts:         opts,
	}
	err = c.init()
	return
}

func (c *Counter) Next(ctx context.Context) (counter int64, err error) {
	c.mu.Lock()
	if c.Current < c.Limit {
		counter = c.Current
		c.Current++
		c.mu.Unlock()
		return counter, nil
	}

	counter = c.Current
	c.Limit = c.Current + c.opts.ReserveCount
	err = c.put(ctx)
	if err != nil {
		c.mu.Unlock()
		return
	}
	c.Current++
	c.mu.Unlock()

	return counter, nil
}

func (c *Counter) put(ctx context.Context) (err error) {
	//buf, err := json.Marshal(c.state)
	//if err != nil {
	//	return
	//}
	//_, err = c.col.Put(ctx, &c.state)
	return
}

func (c *Counter) init() (err error) {
	ctx := context.Background()
	ok, err := c.col.Get(ctx, c)
	if err != nil {
		return
	}

	if ok {
		c.Current = c.Limit
	} else {
		c.Current = c.opts.StartCount
	}

	c.Limit = c.Current + c.opts.ReserveCount
	err = c.put(ctx)

	return err
}
