/*
Package safety provides a set of safety checks for exposing Kubernetes resources to the outside world.

Usage:

	s := safety.New(in, out, safety.WithLogger(l))

	// Read entries that have been scrubbed of sensitive information.
	go func() {
		// read entries from out
		for e := range out {
			// do something with e
		}
	}()

	// Send entries to be scrubbed.
	for _, e := range entries {
		in <- e
	}
*/
package safety

import (
	"fmt"
	"log/slog"
	"regexp"

	"github.com/element-of-surprise/auditARG/internal/readers/data"

	corev1 "k8s.io/api/core/v1"
)

type Secrets struct {
	in  <-chan data.Entry
	out chan data.Entry

	log *slog.Logger
}

// Option is a functional option for the Secrets.
type Option func(*Secrets) error

// WithLogger sets the logger for the Secrets. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(s *Secrets) error {
		s.log = l
		return nil
	}
}

func New(in <-chan data.Entry, out chan data.Entry, options ...Option) (*Secrets, error) {
	if in == nil || out == nil {
		panic("can't call Secrets.New() with a nil in or out channel")
	}

	s := &Secrets{
		in:  in,
		out: out,
		log: slog.Default(),
	}

	for _, o := range options {
		if err := o(s); err != nil {
			return nil, err
		}
	}

	go s.run()
	return s, nil
}

func (s *Secrets) run() {
	defer close(s.out)

	for e := range s.in {
		s.entryRouter(e)
	}
}

func (s *Secrets) entryRouter(e data.Entry) {
	switch e.Type {
	case data.ETInformer:
		err := s.informerScrubber(e)
		if err != nil {
			s.log.Error(fmt.Sprintf("error scrubbing informer: %v", err))
			return
		}
	}
	s.out <- e
}

func (s *Secrets) informerScrubber(e data.Entry) error {
	i, err := e.Informer()
	if err != nil {
		return err
	}

	switch i.Type {
	case data.OTPod:
		p, ok := i.Object().(*corev1.Pod)
		if !ok {
			return fmt.Errorf("safety.Secrets.informerRouter: error casting object to pod: %v", err)
		}
		s.scrubPod(p)
	}
	return nil
}

// scrubPod scrubs sensitive information from a pod.
func (s *Secrets) scrubPod(p *corev1.Pod) {
	spec := p.Spec
	for i, cont := range spec.Containers {
		spec.Containers[i] = s.scrubContainer(cont)
	}
	p.Spec = spec
}

var secretRE = regexp.MustCompile(`(?i)(token|pass|jwt|hash|secret|bearer|cred|secure|signing|cert|code|key)`)

// scrubContainer scrubs sensitive information from a container.
func (s *Secrets) scrubContainer(c corev1.Container) corev1.Container {
	for i, ev := range c.Env {
		if secretRE.MatchString(ev.Name) {
			ev.Value = "REDACTED"
			c.Env[i] = ev
		}
	}
	return c
}
