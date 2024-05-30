/*
Package preprocessing provides preprocessing operations for reader data that alters the data before
it is sent to be processed. This allows for data to be altered safely without concurrency issues.
Processors are read only and are not allowed to alter data.
*/
package preprocess

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/element-of-surprise/auditARG/tattler/internal/readers/data"
)

// PreProcessor is function that processes data before it is sent to a processor. It must be thread-safe.
// This is where you would alter data before it is sent for processing. Any change here affects
// all processors.
type PreProcessor func(context.Context, data.Entry) error

// Runner runs a series of PreProcessors.
type Runner struct {
	in, out chan data.Entry
	procs   []PreProcessor

	log *slog.Logger
}

// Option is an option for New().
type Option func(*Runner) error

// WithLogger sets the logger. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(r *Runner) error {
		if l == nil {
			return fmt.Errorf("logger cannot be nil")
		}
		r.log = l
		return nil
	}
}

// New creates a new Runner. A runner can be stopped by closing the input channel.
func New(ctx context.Context, in, out chan data.Entry, procs []PreProcessor, options ...Option) (*Runner, error) {
	r := &Runner{
		in:    in,
		out:   out,
		procs: procs,
		log:   slog.Default(),
	}

	for _, o := range options {
		if err := o(r); err != nil {
			return nil, err
		}
	}

	go r.run(ctx)

	return r, nil
}

// run starts the Runner.
func (r *Runner) run(ctx context.Context) error {
	defer close(r.out)
	for entry := range r.in {
		var err error
		for _, p := range r.procs {
			if err = p(ctx, entry); err != nil {
				r.log.Error(err.Error())
				break
			}
		}
		if err != nil {
			continue
		}
		r.out <- entry
	}
	return nil
}
