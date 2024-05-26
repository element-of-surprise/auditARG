package informers

import (
	"time"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/informers/core"
	v1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"
)

type fakeInformer struct {
	informers.SharedInformerFactory

	core *fakeCore
}

type fakeInformerArgs struct {
	nodes     *fakeSharedIndexInformer
	pods      *fakeSharedIndexInformer
	namespace *fakeSharedIndexInformer
}

func NewFakeInformer(args fakeInformerArgs) *fakeInformer {
	return &fakeInformer{
		core: &fakeCore{
			v1: &fakeV1{
				nodes: &fakeNodeInformer{
					shared: args.nodes,
				},
				pods: &fakePodInformer{
					sharedIndexInformer: args.pods,
				},
				namespace: &fakeNamespaceInformer{
					sharedIndexInformer: args.namespace,
				},
			},
		},
	}
}

func (f *fakeInformer) Core() core.Interface {
	return f.core
}

type fakeCore struct {
	core.Interface
	v1 *fakeV1
}

func (f *fakeCore) V1() v1.Interface {
	return f.v1
}

type fakeV1 struct {
	v1.Interface

	nodes     *fakeNodeInformer
	pods      *fakePodInformer
	namespace *fakeNamespaceInformer
}

func (f *fakeV1) Nodes() v1.NodeInformer {
	return f.nodes
}

func (f *fakeV1) Pods() v1.PodInformer {
	return f.pods
}

func (f *fakeV1) Namespaces() v1.NamespaceInformer {
	return f.namespace
}

type fakeNodeInformer struct {
	v1.NodeInformer
	shared *fakeSharedIndexInformer
}

func (f *fakeNodeInformer) Informer() cache.SharedIndexInformer {
	return f.shared
}

type fakePodInformer struct {
	v1.PodInformer

	sharedIndexInformer *fakeSharedIndexInformer
}

func (f *fakePodInformer) Informer() cache.SharedIndexInformer {
	return f.sharedIndexInformer
}

type fakeNamespaceInformer struct {
	v1.NamespaceInformer

	sharedIndexInformer *fakeSharedIndexInformer
}

func (f *fakeNamespaceInformer) Informer() cache.SharedIndexInformer {
	return f.sharedIndexInformer
}

type fakeSharedIndexInformer struct {
	cache.SharedIndexInformer

	handlers []cache.ResourceEventHandler
	sendErr  error
}

func (f *fakeSharedIndexInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	f.handlers = append(f.handlers, handler)
	if f.sendErr != nil {
		return nil, f.sendErr
	}
	return &fakeResourceEventHandlerRegistration{}, nil
}

type fakeResourceEventHandlerRegistration struct {
	cache.ResourceEventHandlerRegistration
}

func (f *fakeResourceEventHandlerRegistration) HasSynced() bool {
	return true
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
