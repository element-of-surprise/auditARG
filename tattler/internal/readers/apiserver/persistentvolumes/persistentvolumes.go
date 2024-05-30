package persistentvolumes

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"reflect"
	"time"

	"github.com/element-of-surprise/auditARG/tattler/internal/readers/data"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

type Reader struct {
	informer cache.SharedIndexInformer
	ch       chan data.Entry

	started bool
	stop    chan struct{}

	log *slog.Logger
}

// Option is a function that can be passed to New to configure the Reader.
type Option func(*Reader) error

// WithLogger sets the logger for the Reader.
func WithLogger(log *slog.Logger) Option {
	return func(r *Reader) error {
		r.log = log
		return nil
	}
}

// New creates a new Reader that reads PersistentVolumes from the Kubernetes API server.
func New(ctx context.Context, clientset *kubernetes.Clientset, resync time.Duration, options ...Option) (*Reader, error) {
	r := &Reader{
		stop: make(chan struct{}),
		log:  slog.Default(),
	}

	pvs, err := clientset.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		klog.Fatalf("Error listing PersistentVolumes: %v", err)
	}
	klog.Infof("Successfully listed PersistentVolumes: %d items found", len(pvs.Items))

	for _, option := range options {
		if err := option(r); err != nil {
			return nil, err
		}
	}

	r.informer = cache.NewSharedIndexInformer(
		cache.NewListWatchFromClient(
			clientset.CoreV1().RESTClient(),
			"persistentvolumes",
			metav1.NamespaceAll,
			fields.Everything(),
		),
		&v1.PersistentVolume{},
		resync,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)

	r.informer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    r.addHandler,
			UpdateFunc: r.updateHandler,
			DeleteFunc: r.deleteHandler,
		},
	)

	return r, nil
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

	for {
		if !c.informer.IsStopped() {
			time.Sleep(closeDelay)
			goto start
		}
		break
	}
	return nil
}

// SetOut sets the output channel that the reader must output on. Must return an error and be a no-op
// if Run() has been called.
func (r *Reader) SetOut(ctx context.Context, out chan data.Entry) error {
	if r.started {
		return fmt.Errorf("cannot call SetOut once the Reader has had Start() called")
	}

	r.ch = out
	return nil
}

// Run starts the Reader processing. You may only call this once if Run() does not return an error.
func (r *Reader) Run(ctx context.Context) error {
	if r.started {
		return fmt.Errorf("cannot call Run once the Reader has already started")
	}
	if r.ch == nil {
		return fmt.Errorf("cannot call Run if SetOut has not been called")
	}
	r.started = true

	go r.informer.Run(r.stop)

	log.Println("called")
	if !cache.WaitForCacheSync(r.stop, r.informer.HasSynced) {
		r.started = false
		r.stop = make(chan struct{})
		return fmt.Errorf("failed to sync cache")
	}

	log.Println("Started")

	return nil
}

// addHandler is the event handler for adding data. This is a shim around addOrDelete.
func (c *Reader) addHandler(obj any) {
	log.Println("addHandler")
	err := c.addOrDelete(obj, data.CTAdd)
	if err != nil {
		c.log.Error(err.Error())
	}
}

func (c *Reader) updateHandler(oldObj any, newObj any) {
	log.Println("updateHandler")
	err := c.update(oldObj, newObj)
	if err != nil {
		c.log.Error(err.Error())
	}
}

// deleteHandler is the event handler for deleting data. This is a shim around addOrDelete.
func (c *Reader) deleteHandler(obj any) {
	log.Println("deleteHandler")
	err := c.addOrDelete(obj, data.CTDelete)
	if err != nil {
		c.log.Error(err.Error())
	}
}

// addOrDelete handles event types add and delete.
func (c *Reader) addOrDelete(obj any, ct data.ChangeType) error {
	if obj == nil {
		return fmt.Errorf("persistentvolumes.Reader.addOrDelete(): obj cannot be nil")
	}

	var d data.PersistentVolume
	switch v := obj.(type) {
	case *v1.PersistentVolume:
		log.Println("its a pv")
		pvc := data.Change[*v1.PersistentVolume]{ChangeType: ct, ObjectType: data.OTPersistentVolume}
		switch ct {
		case data.CTAdd:
			pvc.New = v
		case data.CTDelete:
			pvc.Old = v
		default:
			return fmt.Errorf("unsupported change type in persistentvolumes.Reader.addOrDelete(): %d", ct)
		}
		var err error
		d, err = data.NewPersistentVolume(pvc)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("persistent volumnes: unknown object type: %T", obj)
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
		return fmt.Errorf("persistentvolumes.Reader.update(): oldObj and newObj cannot be nil")
	}

	// Note: This is a safety check, but it does engage reflection.
	// This is a trade-off for safety. However, this would be a completely broken
	// APIServer and client if this were to happen. So we might want to remove it.
	if reflect.TypeOf(oldObj) != reflect.TypeOf(newObj) {
		return fmt.Errorf("persistentvolumes.Reader.update(): oldObj(%T) and newObj(%T) are not the same type", oldObj, newObj)
	}

	var d data.PersistentVolume
	switch v := newObj.(type) {
	case *v1.PersistentVolume:
		log.Println("happened")
		pvc := data.Change[*v1.PersistentVolume]{
			ChangeType: data.CTUpdate,
			ObjectType: data.OTPersistentVolume,
			New:        v,
			Old:        oldObj.(*v1.PersistentVolume),
		}

		var err error
		d, err = data.NewPersistentVolume(pvc)
		if err != nil {
			panic("wtf")
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
