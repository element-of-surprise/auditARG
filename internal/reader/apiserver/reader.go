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

	for data := range c.Stream() {
		switch data.Type {
		case DTNode:
			switch data.Node.Type {
			case CTAdd:
				// Do something
			case CTUpdate:
				// Do something
			case CTDelete:
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
			}
		}
	}
*/
package reader

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

//go:generate stringer -type=DataType

// DataType is the type of the data.
type DataType uint8

const (
	// DTUnknown indicates a bug in the code.
	DTUnknown DataType = 0
	// DTNode indicates the data is a node.
	DTNode DataType = 1
	// DTPod indicates the data is a pod.
	DTPod DataType = 2
	// DTNamespace indicates the data is a namespace.
	DTNamespace DataType = 3
)

// Data is the data that is passed through the pipeline for a request.
// Note: This data type is field aligned for better performance.
type Data struct {
	// Node is the node data. This is only valid if Type is Node.
	Node Change[corev1.Node]
	// Pod is the pod data. This is only valid if Type is Pod.
	Pod Change[corev1.Pod]
	// Namespace is the namespace data. This is only valid if Type is Namespace.
	Namespace Change[corev1.Namespace]

	// Type is the type of the data.
	Type DataType
}

//go:generate stringer -type=ChangeType

// ChangeType is the type of change.
type ChangeType uint8

const (
	// CTUnknown indicates a bug in the code.
	CTUnknown ChangeType = 0
	// CTAdd indicates the data was added.
	CTAdd ChangeType = 1
	// CTUpdate indicates the data was updated.
	CTUpdate ChangeType = 2
	// CTDelete indicates the data was deleted.
	CTDelete ChangeType = 3
)

// Change is a change made to a data set.
// Note: This data type is field aligned for better performance.
type Change[T any] struct {
	// Old is the old data. This is only valid if Type is Update or Delete.
	Old T
	// New is the new data. This is only valid if Type is Add or Update.
	New T
	// Type is the type of the change.
	Type ChangeType
}

// Changes are changes made to data sets on the APIServer.
type Changes struct {
	informer informers.SharedInformerFactory
	indexes  []cache.SharedIndexInformer
	handlers cache.ResourceEventHandlerFuncs

	ch   chan Data
	stop chan struct{}

	log *slog.Logger
}

// Option is an option for New(). Unused for now.
type Option func(*Changes) error

// WithLogger sets the logger for the Changes object.
func WithLogger(log *slog.Logger) Option {
	return func(c *Changes) error {
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
func New(informer informers.SharedInformerFactory, retrieveTypes RetrieveType, opts ...Option) (*Changes, error) {
	if informer == nil {
		return nil, fmt.Errorf("informer is nil")
	}

	c := &Changes{
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
		c.ch = make(chan Data, 5000)
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
func (c *Changes) Close(ctx context.Context) error {
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
func (c *Changes) Stream() <-chan Data {
	return c.ch
}

// nodeInform sets up the node informer.
func (c *Changes) nodeInform() (cache.InformerSynced, error) {
	nodeInformer := c.informer.Core().V1().Nodes().Informer()
	reg, err := nodeInformer.AddEventHandler(c.handlers)
	if err != nil {
		return nil, err
	}
	c.indexes = append(c.indexes, nodeInformer)
	return reg.HasSynced, nil
}

// podInform sets up the pod informer.
func (c *Changes) podInform() (cache.InformerSynced, error) {
	podInformer := c.informer.Core().V1().Pods().Informer()
	reg, err := podInformer.AddEventHandler(c.handlers)
	if err != nil {
		return nil, err
	}
	c.indexes = append(c.indexes, podInformer)
	return reg.HasSynced, nil
}

// namespaceInform sets up the namespace informer.
func (c *Changes) namespaceInform() (cache.InformerSynced, error) {
	namespaceInformer := c.informer.Core().V1().Namespaces().Informer()
	reg, err := namespaceInformer.AddEventHandler(c.handlers)
	if err != nil {
		return nil, err
	}
	c.indexes = append(c.indexes, namespaceInformer)
	return reg.HasSynced, nil
}

// addHandler is the event handler for adding data. This is a shim around addOrDelete.
func (c *Changes) addHandler(obj any) {
	err := c.addOrDelete(obj, CTAdd)
	if err != nil {
		c.log.Error(err.Error())
	}
}

func (c *Changes) updateHandler(oldObj any, newObj any) {
	err := c.update(oldObj, newObj)
	if err != nil {
		c.log.Error(err.Error())
	}
}

// deleteHandler is the event handler for deleting data. This is a shim around addOrDelete.
func (c *Changes) deleteHandler(obj any) {
	err := c.addOrDelete(obj, CTDelete)
	if err != nil {
		c.log.Error(err.Error())
	}
}

// addOrDelete handles event types add and delete.
func (c *Changes) addOrDelete(obj any, ct ChangeType) error {
	if obj == nil {
		return fmt.Errorf("Changes.addOrDelete(): obj cannot be nil")
	}

	var d Data
	switch v := obj.(type) {
	case *corev1.Node:
		d.Type = DTNode
		d.Node = Change[corev1.Node]{Type: ct}
		switch ct {
		case CTAdd:
			d.Node.New = *v
		case CTDelete:
			d.Node.Old = *v
		default:
			return fmt.Errorf("unsupported change type in Changes.addOrDelete(): %d", ct)
		}
	case *corev1.Pod:
		d.Type = DTPod
		d.Pod = Change[corev1.Pod]{Type: ct}
		switch ct {
		case CTAdd:
			d.Pod.New = *v
		case CTDelete:
			d.Pod.Old = *v
		default:
			return fmt.Errorf("unsupported change type in Changes.addOrDelete(): %d", ct)
		}
	case *corev1.Namespace:
		d.Type = DTNamespace
		d.Namespace = Change[corev1.Namespace]{Type: ct}
		switch ct {
		case CTAdd:
			d.Namespace.New = *v
		case CTDelete:
			d.Namespace.Old = *v
		}
	default:
		return fmt.Errorf("unknown object type: %T", obj)
	}

	c.ch <- d
	return nil
}

// update is the event handler for updating data.
func (c *Changes) update(oldObj any, newObj any) error {
	if oldObj == nil || newObj == nil {
		return fmt.Errorf("Changes.update(): oldObj and newObj cannot be nil")
	}

	// Note: This is a safety check, but it does engage reflection.
	// This is a trade-off for safety. However, this would be a completely broken
	// APIServer and client if this were to happen. So we might want to remove it.
	if reflect.TypeOf(oldObj) != reflect.TypeOf(newObj) {
		return fmt.Errorf("Changes.update(): oldObj(%T) and newObj(%T) are not the same type", oldObj, newObj)
	}

	var d Data
	switch v := newObj.(type) {
	case *corev1.Node:
		d.Type = DTNode
		d.Node = Change[corev1.Node]{Type: CTUpdate}
		d.Node.New = *v
		d.Node.Old = *oldObj.(*corev1.Node)
	case *corev1.Pod:
		d.Type = DTPod
		d.Pod = Change[corev1.Pod]{Type: CTUpdate}
		d.Pod.New = *v
		d.Pod.Old = *oldObj.(*corev1.Pod)
	case *corev1.Namespace:
		d.Type = DTNamespace
		d.Namespace = Change[corev1.Namespace]{Type: CTUpdate}
		d.Namespace.New = *v
		d.Namespace.Old = *oldObj.(*corev1.Namespace)
	default:
		return fmt.Errorf("unknown object type: %T", newObj)
	}

	c.ch <- d
	return nil
}
