package batching

import (
	"testing"
	"time"

	"github.com/element-of-surprise/auditARG/tattler/internal/readers/data"

	"github.com/kylelemons/godebug/pretty"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestHandleInput(t *testing.T) {
	t.Parallel()

	closedCh := make(chan data.Entry)
	close(closedCh)

	tests := []struct {
		name     string
		in       func() chan data.Entry
		tick     <-chan time.Time
		current  Batches
		wantEmit bool
		wantExit bool
		wantErr  bool
	}{
		{
			name:     "Input channel is closed",
			in:       func() chan data.Entry { return closedCh },
			wantExit: true,
		},
		{
			name: "HandleData error",
			in: func() chan data.Entry {
				ch := make(chan data.Entry, 1)
				ch <- data.Entry{}
				return ch
			},
			wantErr: true,
		},
		{
			name: "Successful input",
			in: func() chan data.Entry {
				ch := make(chan data.Entry, 1)
				ch <- mustEntry(
					mustInformer(
						data.Change[*corev1.Pod]{
							ChangeType: data.CTAdd,
							ObjectType: data.OTPod,
							New: &corev1.Pod{
								ObjectMeta: v1.ObjectMeta{
									UID: types.UID("test"),
								},
							},
						},
					),
				)
				return ch
			},
		},
		{
			name: "Successful tick but nothing to send",
			in:   func() chan data.Entry { return make(chan data.Entry) },
			tick: time.After(1 * time.Microsecond),
		},
		{
			name: "Successful tick and data to send",
			in:   func() chan data.Entry { return make(chan data.Entry) },
			tick: time.After(1 * time.Microsecond),
			current: Batches{
				data.ETInformer: Batch{},
			},
			wantEmit: true,
		},
	}

	for _, test := range tests {
		if test.current == nil {
			test.current = make(Batches)
		}
		b := &Batcher{
			in:      test.in(),
			current: test.current,
		}
		b.setupPools()
		var emitted bool
		emitter := func() {
			emitted = true
		}
		b.emitter = emitter

		gotExit, gotErr := b.handleInput(test.tick)
		switch {
		case gotErr != nil && !test.wantErr:
			t.Errorf("TestHandleInput(%s): got err == %v, want err == nil", test.name, gotErr)
			continue
		case gotErr == nil && test.wantErr:
			t.Errorf("TestHandleInput(%s): got err == nil, want err != nil", test.name)
			continue
		case gotErr != nil:
			continue
		}

		if gotExit != test.wantExit {
			t.Errorf("TestHandleInput(%s): (exit value): got %v, want %v", test.name, gotExit, test.wantExit)
		}

		if emitted != test.wantEmit {
			t.Errorf("TestHandleInput(%s): (emitted value): got %v, want %v", test.name, emitted, test.wantEmit)
		}
	}
}

func TestEmit(t *testing.T) {
	t.Parallel()

	batches := Batches{
		data.ETInformer: Batch{
			"test": mustEntry(
				mustInformer(
					data.Change[*corev1.Pod]{
						ChangeType: data.CTAdd,
						ObjectType: data.OTPod,
						New:        &corev1.Pod{},
					},
				),
			),
		},
	}

	b := &Batcher{
		out:     make(chan Batches, 1),
		current: batches,
	}
	b.setupPools()

	b.emit()

	select {
	case got := <-b.out:
		if diff := pretty.Compare(batches, got); diff != "" {
			t.Errorf("TestEmit(emitted data): -want/+got:\n%s", diff)
		}
		return
	default:
		t.Error("TestEmit: expected data on out channel")
	}

	if diff := pretty.Compare(b.current, Batches{}); diff != "" {
		t.Errorf("TestEmit(after emit): .current: -want/+got:\n%s", diff)
	}
}

func TestHandleData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    data.Entry
		wantErr bool
	}{
		{
			name:    "Invalid data type",
			data:    data.Entry{},
			wantErr: true,
		},
		{
			name: "Valid data",
			data: mustEntry(
				mustInformer(
					data.Change[*corev1.Pod]{
						ChangeType: data.CTAdd,
						ObjectType: data.OTPod,
						New:        &corev1.Pod{ObjectMeta: v1.ObjectMeta{UID: types.UID("test")}},
					},
				),
			),
		},
	}

	for _, test := range tests {
		b := &Batcher{
			current: make(Batches),
		}
		b.setupPools()

		err := b.handleData(test.data)
		switch {
		case err != nil && !test.wantErr:
			t.Errorf("TestHandleData(%s): got error %v, want %v", test.name, err, test.wantErr)
			continue
		case err == nil && test.wantErr:
			t.Errorf("TestHandleData(%s): got %v, want error", test.name, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.data, b.current[test.data.Type][test.data.UID()]); diff != "" {
			t.Errorf("TestHandleData(%s): -want/+got:\n%s", test.name, diff)
		}
	}
}

func mustInformer[T data.K8Object](o data.Change[T]) data.Informer {
	i, err := data.NewInformer(o)
	if err != nil {
		panic(err)
	}
	return i
}

func mustEntry(i data.Informer) data.Entry {
	e, err := data.NewEntry(i)
	if err != nil {
		panic(err)
	}
	return e
}
