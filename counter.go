package sqlitekv

import (
	"context"
	"encoding/json"
	"sync"
)

type counterState struct {
	Current int64 `json:"current"`
	Limit   int64 `json:"limit"`
}

type Counter struct {
	state counterState

	key  string
	col  *Collection
	opts CounterOptions

	mu sync.Mutex
}

type CounterOptions struct {
	StartCount   int64
	ReserveCount int64
}

func NewCounter(col *Collection, key string, opts CounterOptions) (c *Counter, err error) {
	if opts.ReserveCount <= 0 {
		opts.ReserveCount = 100
	}

	if opts.StartCount <= 0 {
		opts.StartCount = 1
	}

	c = &Counter{
		key:  key,
		col:  col,
		opts: opts,
	}
	err = c.init()
	return
}

func (c *Counter) Next(ctx context.Context) (counter int64, err error) {
	c.mu.Lock()
	if c.state.Current < c.state.Limit {
		counter = c.state.Current
		c.state.Current++
		c.mu.Unlock()
		return counter, nil
	}

	counter = c.state.Current
	c.state.Limit = c.state.Current + c.opts.ReserveCount
	err = c.put(ctx)
	if err != nil {
		c.mu.Unlock()
		return
	}
	c.state.Current++
	c.mu.Unlock()

	return counter, nil
}

func (c *Counter) put(ctx context.Context) (err error) {
	buf, err := json.Marshal(c.state)
	if err != nil {
		return
	}
	_, err = c.col.Put(ctx, c.key, buf)
	return
}

func (c *Counter) init() (err error) {
	ctx := context.Background()
	var state counterState
	ok, err := c.col.Get(ctx, c.key, func(rid int64, _ string, buf []byte, gencols []any) error {
		err = json.Unmarshal(buf, &state)
		return err
	})
	if err != nil {
		return
	}

	c.state = state

	if ok {
		c.state.Current = c.state.Limit
	} else {
		c.state.Current = c.opts.StartCount
	}

	c.state.Limit = c.state.Current + c.opts.ReserveCount
	err = c.put(ctx)

	return err
}
