package persistentvolumes

import (
	"context"
	"testing"
	"time"

	"github.com/element-of-surprise/auditARG/tattler/internal/readers/data"

	"github.com/kylelemons/godebug/pretty"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

func TestClose(t *testing.T) {
	t.Parallel()

	stop := make(chan struct{})

	c := &Reader{
		ch:   make(chan data.Entry, 1),
		stop: stop,
		informer: timedInformers{
			ch:    stop,
			delay: 1 * time.Second,
		},
	}

	now := time.Now()
	c.Close(context.Background())

	sum := time.Duration(0)
	ti := c.informer.(timedInformers)
	sum += ti.delay

	since := time.Since(now)
	if time.Since(now) < sum {
		t.Errorf("TestClose: got time.Since(now) == %s, want time.Since(now) >= %s", since, sum)
	}
}

func TestAddOrDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		obj     any
		ct      data.ChangeType
		want    data.PersistentVolume
		wantErr bool
	}{
		{
			name:    "Error: obj is nil",
			ct:      data.CTAdd,
			wantErr: true,
		},
		{
			name:    "Error: unsupported type",
			obj:     &corev1.NodeAddress{},
			ct:      data.CTAdd,
			wantErr: true,
		},
		{
			name: "PersistentVolume add",
			obj:  &corev1.PersistentVolume{},
			ct:   data.CTAdd,
			want: data.MustNewPersistentVolume(
				data.Change[*corev1.PersistentVolume]{
					ChangeType: data.CTAdd,
					ObjectType: data.OTPersistentVolume,
					New:        &corev1.PersistentVolume{},
				},
			),
		},
		{
			name: "PersistentVolume delete",
			obj:  &corev1.PersistentVolume{},
			ct:   data.CTDelete,
			want: data.MustNewPersistentVolume(
				data.Change[*corev1.PersistentVolume]{
					ChangeType: data.CTDelete,
					ObjectType: data.OTPersistentVolume,
					Old:        &corev1.PersistentVolume{},
				},
			),
		},
	}

	for _, test := range tests {
		c := &Reader{ch: make(chan data.Entry, 1)}

		err := c.addOrDelete(test.obj, test.ct)
		switch {
		case test.wantErr && err == nil:
			t.Errorf("TestAddOrDelete(%s): got err == nil, want err != nil", test.name)
			continue
		case !test.wantErr && err != nil:
			t.Errorf("TestAddOrDelete(%s): got err == %v, want err == nil", test.name, err)
			continue
		case err != nil:
			continue
		}
		e := <-c.ch
		got, err := e.PersistentVolume()
		if err != nil {
			t.Errorf("TestAddOrDelete(%s): got err == %v, want err == nil", test.name, err)
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestAddOrDelete(%s): -want/+got\n%s", test.name, diff)
			continue
		}
	}
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		oldObj, newObj any
		want           data.PersistentVolume
		wantErr        bool
	}{

		{
			name:    "Error: oldObj is nil",
			newObj:  &corev1.PersistentVolume{},
			wantErr: true,
		},
		{
			name:    "Error: newObj is nil",
			oldObj:  &corev1.PersistentVolume{},
			wantErr: true,
		},
		{
			name:    "Error: oldObj and newObj are not the same type",
			oldObj:  &corev1.PersistentVolume{},
			newObj:  &corev1.Pod{},
			wantErr: true,
		},
		{
			name:    "Error: unsupported type",
			oldObj:  &corev1.NodeAddress{},
			newObj:  &corev1.NodeAddress{},
			wantErr: true,
		},
		{
			name:   "PersistentVolume update",
			oldObj: &corev1.PersistentVolume{},
			newObj: &corev1.PersistentVolume{},
			want: data.MustNewPersistentVolume(data.Change[*corev1.PersistentVolume]{
				ChangeType: data.CTUpdate,
				ObjectType: data.OTPersistentVolume,
				New:        &corev1.PersistentVolume{},
				Old:        &corev1.PersistentVolume{},
			}),
		},
	}

	for _, test := range tests {
		c := &Reader{ch: make(chan data.Entry, 1)}

		err := c.update(test.oldObj, test.newObj)
		switch {
		case test.wantErr && err == nil:
			t.Errorf("TestUpdate(%s): got err == nil, want err != nil", test.name)
			continue
		case !test.wantErr && err != nil:
			t.Errorf("TestUpdate(%s): got err == %v, want err == nil", test.name, err)
			continue
		case err != nil:
			continue
		}

		e := <-c.ch
		got, err := e.PersistentVolume()
		if err != nil {
			t.Errorf("TestUpdate(%s): got err == %v, want err == nil", test.name, err)
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestUpdate(%s): -want/+got\n%s", test.name, diff)
			continue
		}
	}
}

type timedInformers struct {
	cache.SharedIndexInformer

	ch    <-chan struct{}
	delay time.Duration
}

func (t timedInformers) IsStopped() bool {
	for {
		select {
		case <-t.ch:
		default:
			return false
		}
		break
	}
	time.Sleep(t.delay)
	return true
}
