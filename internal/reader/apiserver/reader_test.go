package reader

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

func TestClose(t *testing.T) {
	t.Parallel()

	stop := make(chan struct{})

	c := &Changes{
		ch:   make(chan Data, 1),
		stop: stop,
		indexes: []cache.SharedIndexInformer{
			timedInformers{
				ch:    stop,
				delay: 1 * time.Second,
			},
			timedInformers{
				ch:    stop,
				delay: 200 * time.Millisecond,
			},
			timedInformers{
				ch:    stop,
				delay: 300 * time.Millisecond,
			},
		},
	}

	now := time.Now()
	c.Close(context.Background())

	sum := time.Duration(0)
	for _, i := range c.indexes {
		ti := i.(timedInformers)
		sum += ti.delay
	}

	since := time.Since(now)
	if time.Since(now) < sum {
		t.Errorf("TestClose: got time.Since(now) == %s, want time.Since(now) >= %s", since, sum)
	}
}

func TestTypeInform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		call    string
		factory informers.SharedInformerFactory
		wantErr bool
	}{
		{
			name: "Error: Namespace EventHandler returns error",
			call: "Namespace",
			factory: NewFakeInformer(
				fakeInformerArgs{
					namespace: &fakeSharedIndexInformer{
						sendErr: errors.New("error"),
					},
				},
			),
			wantErr: true,
		},
		{
			name: "Namespace Success",
			call: "Namespace",
			factory: NewFakeInformer(
				fakeInformerArgs{
					namespace: &fakeSharedIndexInformer{},
				},
			),
		},
		{
			name: "Error: Node EventHandler returns error",
			call: "Node",
			factory: NewFakeInformer(
				fakeInformerArgs{
					nodes: &fakeSharedIndexInformer{
						sendErr: errors.New("error"),
					},
				},
			),
			wantErr: true,
		},
		{
			name: "Node Success",
			call: "Node",
			factory: NewFakeInformer(
				fakeInformerArgs{
					nodes: &fakeSharedIndexInformer{},
				},
			),
		},
		{
			name: "Error: Pod EventHandler returns error",
			call: "Pod",
			factory: NewFakeInformer(
				fakeInformerArgs{
					pods: &fakeSharedIndexInformer{
						sendErr: errors.New("error"),
					},
				},
			),
			wantErr: true,
		},
		{
			name: "Pod Success",
			call: "Pod",
			factory: NewFakeInformer(
				fakeInformerArgs{
					pods: &fakeSharedIndexInformer{},
				},
			),
		},
	}

	for _, test := range tests {
		c := &Changes{informer: test.factory}
		c.handlers = cache.ResourceEventHandlerFuncs{
			AddFunc:    c.addHandler,
			UpdateFunc: c.updateHandler,
			DeleteFunc: c.deleteHandler,
		}

		var hasSynced cache.InformerSynced
		var err error
		switch test.call {
		case "Namespace":
			hasSynced, err = c.namespaceInform()
		case "Node":
			hasSynced, err = c.nodeInform()
		case "Pod":
			hasSynced, err = c.podInform()
		default:
			panic("unknown call")
		}
		switch {
		case test.wantErr && err == nil:
			t.Errorf("TestNamespaceInform(%s): got err == nil, want err != nil", test.name)
			continue
		case !test.wantErr && err != nil:
			t.Errorf("TestNamespaceInform(%s): got err == %v, want err == nil", test.name, err)
			continue
		case err != nil:
			continue
		}

		if !hasSynced() {
			t.Errorf("TestNamespaceInform(%s): got hasSynced == false, want hasSynced == true", test.name)
		}
		if len(c.indexes) == 0 {
			t.Errorf("TestNamespaceInform(%s): got len(indexes) == 0, want len(indexes) > 0", test.name)
		}
	}
}

func TestAddOrDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		obj     any
		ct      ChangeType
		want    Data
		wantErr bool
	}{
		{
			name:    "Error: obj is nil",
			ct:      CTAdd,
			wantErr: true,
		},
		{
			name:    "Error: unsupported type",
			obj:     &corev1.NodeAddress{},
			ct:      CTAdd,
			wantErr: true,
		},
		{
			name: "Node add",
			obj:  &corev1.Node{},
			ct:   CTAdd,
			want: Data{
				Type: DTNode,
				Node: Change[corev1.Node]{Type: CTAdd, New: corev1.Node{}},
			},
		},
		{
			name: "Pod add",
			obj:  &corev1.Pod{},
			ct:   CTAdd,
			want: Data{
				Type: DTPod,
				Pod:  Change[corev1.Pod]{Type: CTAdd, New: corev1.Pod{}},
			},
		},
		{
			name: "Namespace add",
			obj:  &corev1.Namespace{},
			ct:   CTAdd,
			want: Data{
				Type:      DTNamespace,
				Namespace: Change[corev1.Namespace]{Type: CTAdd, New: corev1.Namespace{}},
			},
		},
		{
			name: "Node delete",
			obj:  &corev1.Node{},
			ct:   CTDelete,
			want: Data{
				Type: DTNode,
				Node: Change[corev1.Node]{Type: CTDelete, Old: corev1.Node{}},
			},
		},
		{
			name: "Pod delete",
			obj:  &corev1.Pod{},
			ct:   CTDelete,
			want: Data{
				Type: DTPod,
				Pod:  Change[corev1.Pod]{Type: CTDelete, Old: corev1.Pod{}},
			},
		},
		{
			name: "Namespace delete",
			obj:  &corev1.Namespace{},
			ct:   CTDelete,
			want: Data{
				Type:      DTNamespace,
				Namespace: Change[corev1.Namespace]{Type: CTDelete, Old: corev1.Namespace{}},
			},
		},
	}

	for _, test := range tests {
		c := &Changes{ch: make(chan Data, 1)}

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

		if diff := pretty.Compare(test.want, <-c.ch); diff != "" {
			t.Errorf("TestAddOrDelete(%s): -want/+got\n%s", test.name, diff)
		}
	}
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		oldObj, newObj any
		want           Data
		wantErr        bool
	}{
		{
			name:    "Error: oldObj is nil",
			newObj:  &corev1.Node{},
			wantErr: true,
		},
		{
			name:    "Error: newObj is nil",
			oldObj:  &corev1.Node{},
			wantErr: true,
		},
		{
			name:    "Error: oldObj and newObj are not the same type",
			oldObj:  &corev1.Node{},
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
			name:   "Node update",
			oldObj: &corev1.Node{},
			newObj: &corev1.Node{},
			want: Data{
				Type: DTNode,
				Node: Change[corev1.Node]{Type: CTUpdate, New: corev1.Node{}, Old: corev1.Node{}},
			},
		},
		{
			name:   "Pod update",
			oldObj: &corev1.Pod{},
			newObj: &corev1.Pod{},
			want: Data{
				Type: DTPod,
				Pod:  Change[corev1.Pod]{Type: CTUpdate, New: corev1.Pod{}, Old: corev1.Pod{}},
			},
		},
		{
			name:   "Namespace update",
			oldObj: &corev1.Namespace{},
			newObj: &corev1.Namespace{},
			want: Data{
				Type:      DTNamespace,
				Namespace: Change[corev1.Namespace]{Type: CTUpdate, New: corev1.Namespace{}, Old: corev1.Namespace{}},
			},
		},
	}

	for _, test := range tests {
		c := &Changes{ch: make(chan Data, 1)}

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

		if diff := pretty.Compare(test.want, <-c.ch); diff != "" {
			t.Errorf("TestUpdate(%s): -want/+got\n%s", test.name, diff)
		}
	}
}
