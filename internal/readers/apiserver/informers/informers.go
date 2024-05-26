/*
Package reader provides the reader for the apiserver.

Prerequisites to using the reader:

	import "k8s.io/client-go/informers"

	var kubeconfig string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")
	} else {
		kubeconfig = ""
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	informer := informers.NewSharedInformerFactory(clientset, time.Minute*10)

Usage:

	// This will create a new reader that will stream node and pod data.
	// RTNode and RTPod are the types of data to retrieve as bitwise flags.
	c, err := New(informer, retrieveTypes RetrieveType, RTNode | RTPod)
	if err != nil {
		// Do something
	}

	for entry := range c.Stream() {
		data, _ := entry.Informer() // Won't get an error because we know the type.

		switch data.Type {
		case DTNode:
			switch data.Node.Type {
			case CTAdd:
				// Do something
			case CTUpdate:
				// Do something
			case CTDelete:
				// Do something
			default:
				// Do something
			}
		case DTPod:
			switch data.Pod.Type {
			case CTAdd:
				// Do something
			case CTUpdate:
				// Do something
			case CTDelete:
				// Do something
			default:
				// Do something
			}
		}
	}
*/
package informers

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/element-of-surprise/auditARG/internal/readers/data"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

// Reader reports changes made to data objects on the APIServer via the informers API.
type Reader struct {
	informer informers.SharedInformerFactory
	indexes  []cache.SharedIndexInformer
	handlers cache.ResourceEventHandlerFuncs

	ch   chan data.Entry
	stop chan struct{}

	log *slog.Logger
}

// Option is an option for New(). Unused for now.
type Option func(*Reader) error

// WithLogger sets the logger for the Changes object.
func WithLogger(log *slog.Logger) Option {
	return func(c *Reader) error {
		c.log = log
		return nil
	}
}

// RetrieveType is the type of data to retrieve. Uses as a bitwise flag.
// So, like: RTNode | RTPod, or RTNode, or RTPod.
type RetrieveType uint8

const (
	// RTNode retrieves node data.
	RTNode RetrieveType = 0x0
	// RTPod retrieves pod data.
	RTPod RetrieveType = 0x1
	// RTNamespace retrieves namespace data.
	RTNamespace RetrieveType = 0x2
)

// New creates a new Changes object. retrieveTypes is a bitwise flag to determine what data to retrieve.
func New(informer informers.SharedInformerFactory, retrieveTypes RetrieveType, opts ...Option) (*Reader, error) {
	if informer == nil {
		return nil, fmt.Errorf("informer is nil")
	}

	c := &Reader{
		informer: informer,
		stop:     make(chan struct{}),
	}
	c.handlers = cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addHandler,
		UpdateFunc: c.updateHandler,
		DeleteFunc: c.deleteHandler,
	}

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	if c.ch == nil {
		c.ch = make(chan data.Entry, 5000)
	}
	if c.log == nil {
		c.log = slog.Default()
	}

	if retrieveTypes&RTNode != RTNode && retrieveTypes&RTPod != RTPod && retrieveTypes&RTNamespace != RTNamespace {
		return nil, fmt.Errorf("no data types to retrieve")
	}

	syncers := make([]cache.InformerSynced, 0, 2)
	if retrieveTypes&RTNode == RTNode {
		s, err := c.nodeInform()
		if err != nil {
			return nil, err
		}
		syncers = append(syncers, s)
	}

	if retrieveTypes&RTPod == RTPod {
		s, err := c.podInform()
		if err != nil {
			return nil, err
		}
		syncers = append(syncers, s)
	}

	if retrieveTypes&RTNamespace == RTNamespace {
		s, err := c.namespaceInform()
		if err != nil {
			return nil, err
		}
		syncers = append(syncers, s)
	}

	informer.Start(c.stop)

	if !cache.WaitForCacheSync(c.stop, syncers...) {
		return nil, fmt.Errorf("failed to sync cache")
	}

	return c, nil
}

var closeDelay = 100 * time.Millisecond

// Close closes the Changes object. This will block until all indexes are stopped.
// If the context is canceled, it will return the context error.
func (c *Reader) Close(ctx context.Context) error {
	close(c.stop)
	defer close(c.ch)

start:
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for _, index := range c.indexes {
		if !index.IsStopped() {
			time.Sleep(closeDelay)
			goto start
		}
	}
	return nil
}

// Stream returns the data stream.
func (c *Reader) Stream() <-chan data.Entry {
	return c.ch
}

// nodeInform sets up the node informer.
func (c *Reader) nodeInform() (cache.InformerSynced, error) {
	nodeInformer := c.informer.Core().V1().Nodes().Informer()
	reg, err := nodeInformer.AddEventHandler(c.handlers)
	if err != nil {
		return nil, err
	}
	c.indexes = append(c.indexes, nodeInformer)
	return reg.HasSynced, nil
}

// podInform sets up the pod informer.
func (c *Reader) podInform() (cache.InformerSynced, error) {
	podInformer := c.informer.Core().V1().Pods().Informer()
	reg, err := podInformer.AddEventHandler(c.handlers)
	if err != nil {
		return nil, err
	}
	c.indexes = append(c.indexes, podInformer)
	return reg.HasSynced, nil
}

// namespaceInform sets up the namespace informer.
func (c *Reader) namespaceInform() (cache.InformerSynced, error) {
	namespaceInformer := c.informer.Core().V1().Namespaces().Informer()
	reg, err := namespaceInformer.AddEventHandler(c.handlers)
	if err != nil {
		return nil, err
	}
	c.indexes = append(c.indexes, namespaceInformer)
	return reg.HasSynced, nil
}

// addHandler is the event handler for adding data. This is a shim around addOrDelete.
func (c *Reader) addHandler(obj any) {
	err := c.addOrDelete(obj, data.CTAdd)
	if err != nil {
		c.log.Error(err.Error())
	}
}

func (c *Reader) updateHandler(oldObj any, newObj any) {
	err := c.update(oldObj, newObj)
	if err != nil {
		c.log.Error(err.Error())
	}
}

// deleteHandler is the event handler for deleting data. This is a shim around addOrDelete.
func (c *Reader) deleteHandler(obj any) {
	err := c.addOrDelete(obj, data.CTDelete)
	if err != nil {
		c.log.Error(err.Error())
	}
}

// addOrDelete handles event types add and delete.
func (c *Reader) addOrDelete(obj any, ct data.ChangeType) error {
	if obj == nil {
		return fmt.Errorf("Changes.addOrDelete(): obj cannot be nil")
	}

	var d data.Informer
	switch v := obj.(type) {
	case *corev1.Node:
		node := data.Change[*corev1.Node]{ChangeType: ct, ObjectType: data.OTNode}
		switch ct {
		case data.CTAdd:
			node.New = v
		case data.CTDelete:
			node.Old = v
		default:
			return fmt.Errorf("unsupported change type in Changes.addOrDelete(): %d", ct)
		}
		var err error
		d, err = data.NewInformer(node)
		if err != nil {
			return err
		}
	case *corev1.Pod:
		d.Type = data.OTPod
		pod := data.Change[*corev1.Pod]{ChangeType: ct, ObjectType: data.OTPod}
		switch ct {
		case data.CTAdd:
			pod.New = v
		case data.CTDelete:
			pod.Old = v
		default:
			return fmt.Errorf("unsupported change type in Changes.addOrDelete(): %d", ct)
		}
		var err error
		d, err = data.NewInformer(pod)
		if err != nil {
			return err
		}
	case *corev1.Namespace:
		ns := data.Change[*corev1.Namespace]{ChangeType: ct, ObjectType: data.OTNamespace}
		switch ct {
		case data.CTAdd:
			ns.New = v
		case data.CTDelete:
			ns.Old = v
		}
		var err error
		d, err = data.NewInformer(ns)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown object type: %T", obj)
	}

	e, err := data.NewEntry(d)
	if err != nil {
		return err
	}

	c.ch <- e
	return nil
}

// update is the event handler for updating data.
func (c *Reader) update(oldObj any, newObj any) error {
	if oldObj == nil || newObj == nil {
		return fmt.Errorf("Changes.update(): oldObj and newObj cannot be nil")
	}

	// Note: This is a safety check, but it does engage reflection.
	// This is a trade-off for safety. However, this would be a completely broken
	// APIServer and client if this were to happen. So we might want to remove it.
	if reflect.TypeOf(oldObj) != reflect.TypeOf(newObj) {
		return fmt.Errorf("Changes.update(): oldObj(%T) and newObj(%T) are not the same type", oldObj, newObj)
	}

	var d data.Informer
	switch v := newObj.(type) {
	case *corev1.Node:
		node := data.Change[*corev1.Node]{
			ChangeType: data.CTUpdate,
			ObjectType: data.OTNode,
			New:        v,
			Old:        oldObj.(*corev1.Node),
		}

		var err error
		d, err = data.NewInformer(node)
		if err != nil {
			return err
		}
	case *corev1.Pod:
		pod := data.Change[*corev1.Pod]{
			ChangeType: data.CTUpdate,
			ObjectType: data.OTPod,
			New:        v,
			Old:        oldObj.(*corev1.Pod),
		}
		var err error
		d, err = data.NewInformer(pod)
		if err != nil {
			return err
		}
	case *corev1.Namespace:
		ns := data.Change[*corev1.Namespace]{
			ChangeType: data.CTUpdate,
			ObjectType: data.OTNamespace,
			New:        v,
			Old:        oldObj.(*corev1.Namespace),
		}

		var err error
		d, err = data.NewInformer(ns)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown object type: %T", newObj)
	}

	e, err := data.NewEntry(d)
	if err != nil {
		return err
	}

	c.ch <- e
	return nil
}
