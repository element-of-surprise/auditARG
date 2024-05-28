package tattler

import (
	"context"
	"fmt"
	"time"

	"github.com/element-of-surprise/auditARG/tattler/internal/batching"
	"github.com/element-of-surprise/auditARG/tattler/internal/readers/data"
	"github.com/element-of-surprise/auditARG/tattler/internal/readers/safety"
	"github.com/element-of-surprise/auditARG/tattler/internal/routing"
)

// Reader defines the interface that must be implemented by all readers.
// We do not support a Reader that is not within this package.
type Reader interface {
	// SetOut sets the output channel that the reader must output on. Must return an error and be a no-op
	// if Run() has been called.
	SetOut(context.Context, chan data.Entry) error
	// Run starts the Reader processing. You may only call this once if Run() does not return an error.
	Run(context.Context) error
}

// Runner runs readers and sends the output through a series data modifications and batching until
// it is sent to data processors.
type Runner struct {
	input   chan data.Entry
	secrets *safety.Secrets
	batcher *batching.Batcher
	router  *routing.Batches
	readers []Reader

	started bool
}

// New constructs a new Runner.
func New(input chan data.Entry, batchTimespan time.Duration) (*Runner, error) {
	batchingIn := make(chan data.Entry, 1)
	routerIn := make(chan batching.Batches, 1)

	secrets, err := safety.New(input, batchingIn)
	if err != nil {
		return nil, err
	}

	batcher, err := batching.New(batchingIn, routerIn, batchTimespan)
	if err != nil {
		return nil, err
	}

	router, err := routing.New(routerIn)
	if err != nil {
		return nil, err
	}

	r := &Runner{
		secrets: secrets,
		batcher: batcher,
		router:  router,
		input:   input,
	}
	return r, nil
}

// AddReader adds a reader's output channel as input to be processed. A Reader does not need to have
// SetOut() or Run() called, as these are handled by AddReader() and Start().
func (r *Runner) AddReader(ctx context.Context, reader Reader) error {
	if r.started {
		return fmt.Errorf("cannot add a reader after Runner has started")
	}
	if err := reader.SetOut(ctx, r.input); err != nil {
		return fmt.Errorf("Reader(%T).SetOut(): %w", r, err)
	}
	r.readers = append(r.readers, reader)
	return nil
}

// AddProcessor registers a processors input to receive Batches data.
func (r *Runner) AddProcessor(ctx context.Context, name string, in chan batching.Batches) error {
	if r.started {
		return fmt.Errorf("cannot add a processor after Runner has started")
	}
	return r.router.Register(ctx, name, in)
}

// Start starts the Runner.
func (r *Runner) Start(ctx context.Context) error {
	for _, reader := range r.readers {
		if err := reader.Run(ctx); err != nil {
			return fmt.Errorf("reader(%T): %w", reader, err)
		}
	}

	if err := r.router.Start(ctx); err != nil {
		return err
	}
	return nil
}
