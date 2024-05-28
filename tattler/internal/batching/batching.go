/*
Package batching provides batching operations for reader data that removes any older data for the same item
that is sent during the batch time.

The batch is not size based, as we don't actually have a way to determine the batch size because
we haven't encoded into bytes. To control sizing, we can adjust the amount of time we wait or size
encoded data when we send it.

The Batcher will emit a Batches map of data types to a Batch map. The Batch is a map of UIDs to data. We
overwrite any new data that comes in with the same UID. This allows us to get rid of older data before
we emit the batch.

Usage is pretty simple:

	batcher, err := batching.New(5 * time.Second)
	if err != nil {
		// Do something
	}

	// Handle the output.
	go func() {
		for _, batches := range batcher.Out() {
			for data := range batches.Iter() {
				// Do something with data
			}
			// Then recycle the batch, if your sure you're done with it.
			batcher.Recycle(batches)
		}
	}()

	// Send input to the batcher.
	for entry := range c.Stream() { // where c is a some reader returning data.Entry
		batcher.In() <- entry
	}
*/
package batching

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"sync"
	"time"

	"github.com/element-of-surprise/auditARG/tattler/internal/readers/data"

	"k8s.io/apimachinery/pkg/types"
)

// Batches is a map of entry types to batches.
type Batches map[data.EntryType]Batch

// Iter returns a channel that iterates over the data. Closing ctx will stop the iteration.
func (b Batches) Iter(ctx context.Context) <-chan data.Entry {
	ch := make(chan data.Entry, 1)
	go func() {
		defer close(ch)
		for _, batch := range b {
			for _, d := range batch {
				select {
				case <-ctx.Done():
					return
				case ch <- d:
				}
			}
		}
	}()
	return ch
}

// Batch is a map of UIDs to data.
type Batch map[types.UID]data.Entry

// Batcher is used to ingest data and emit batches.
type Batcher struct {
	timespan    time.Duration
	current     Batches
	batchesPool sync.Pool
	batchPool   sync.Pool

	in  <-chan data.Entry
	out chan Batches

	emitter func()

	log *slog.Logger
}

// Option is a opional argument for New().
type Option func(*Batcher) error

// WithLogger sets the logger.
func WithLogger(log *slog.Logger) Option {
	return func(b *Batcher) error {
		b.log = log
		return nil
	}
}

// New creates a new Batcher.
func New(in <-chan data.Entry, out chan Batches, timespan time.Duration, options ...Option) (*Batcher, error) {
	if in == nil || out == nil {
		return nil, errors.New("can't call Batcher.New() with a nil in or out channel")
	}

	b := &Batcher{
		timespan: timespan,
		current:  Batches{},
		in:       in,
		out:      out,
		log:      slog.Default(),
	}
	b.setupPools()
	b.emitter = b.emit

	for _, o := range options {
		if err := o(b); err != nil {
			return nil, err
		}
	}

	go b.run()

	return b, nil
}

// setupPools sets up the sync.Pools. Provided here instead of in New()
// to allow for testing.
func (b *Batcher) setupPools() {
	b.batchesPool = sync.Pool{
		New: func() any {
			return Batches{}
		},
	}
	b.batchPool = sync.Pool{
		New: func() any {
			return Batch{}
		},
	}
}

// Recycle recycles batches when you are done with them.
func (b *Batcher) Recycle(batches Batches) {
	for _, batch := range batches {
		maps.DeleteFunc[Batch](batch, func(types.UID, data.Entry) bool {
			return true
		})
		b.batchPool.Put(batch)
	}
	maps.DeleteFunc[Batches](batches, func(data.EntryType, Batch) bool {
		return true
	})
	b.batchesPool.Put(batches)
}

// run runs the Batcher loop.
func (b *Batcher) run() {
	defer close(b.out)

	ticker := time.NewTicker(b.timespan)
	defer ticker.Stop()

	for {
		exit, err := b.handleInput(ticker.C)
		if err != nil {
			b.log.Error(err.Error())
		}
		if exit {
			return
		}
	}
}

// handleInput handles the input data and batching when the ticker fires.
func (b *Batcher) handleInput(tick <-chan time.Time) (exit bool, err error) {
	select {
	case data, ok := <-b.in:
		if !ok {
			return true, nil
		}
		if err := b.handleData(data); err != nil {
			return false, err
		}
	case <-tick:
		if len(b.current) == 0 {
			return false, nil
		}
		b.emitter()
	}
	return false, nil
}

// emit emits the current batches and preps for the new batches. This is assigned
// to b.emitter by New() at runtime.
func (b *Batcher) emit() {
	batches := b.current
	n := b.batchesPool.Get().(Batches)
	b.current = n
	b.out <- batches
}

// handleData handles putting the data into the current batch.
func (b *Batcher) handleData(entry data.Entry) error {
	batch, ok := b.current[entry.Type]
	if !ok {
		batch = b.batchPool.Get().(Batch)
	}

	if entry.UID() == "" {
		return errors.New("no UID for entry")
	}

	// Note: We are overwriting any data that comes in with the same UID.
	// We may want to in the future try to do something other than simple
	// ordering to determine which data to keep for extra safety.
	// That might be using .Generation or something else.
	batch[entry.UID()] = entry
	b.current[entry.Type] = batch
	return nil
}
